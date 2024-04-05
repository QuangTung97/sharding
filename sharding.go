package sharding

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/concurrency"
	"github.com/QuangTung97/zk/curator"
)

type Sharding struct {
	parentPath string
	nodeID     string
	numShards  ShardID
	nodeAddr   string

	logger zk.Logger

	cur *curator.Curator

	obs *observerCore

	state *sessionState

	lockBegin func(sess *curator.Session)

	clientID curator.FakeClientID
}

type assignState struct {
	version int32
	shards  []ShardID
}

type sessionState struct {
	lockCreated    bool
	nodesCreated   bool
	assignsCreated bool
	leaderStarted  bool

	nodes            []string
	currentAssignMap map[string]assignState

	getAssignNodesCompleted  bool
	listActiveNodesCompleted bool
}

func NewNodeID() string {
	var data [16]byte
	_, err := rand.Reader.Read(data[:])
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}

func New(
	parentPath string, nodeID string,
	numShards ShardID, nodeAddr string,
	options ...Option,
) *Sharding {
	s := &Sharding{
		parentPath: parentPath,
		nodeID:     nodeID,
		numShards:  numShards,
		nodeAddr:   nodeAddr,

		logger: &defaultLoggerImpl{},
	}

	for _, fn := range options {
		fn(s)
	}

	lock := concurrency.NewLock(s.getLockPath(), nodeID)

	startLeader := func(sess *curator.Session, next func(sess *curator.Session)) {
		lock.Start(sess, next)
		if s.obs != nil {
			s.obs.onStart(sess)
		}
	}

	s.cur = curator.NewChain(
		s.createContainerNodes,
		startLeader,
		s.onLeaderCallback,
	)

	return s
}

func (s *Sharding) getLockPath() string {
	return s.parentPath + lockZNodeName
}

func (s *Sharding) getNodesPath() string {
	return s.parentPath + nodeZNodeName
}

func (s *Sharding) getAssignsPath() string {
	return s.parentPath + assignZNodeName
}

func (s *Sharding) createContainerNodes(sess *curator.Session, next func(sess *curator.Session)) {
	s.state = &sessionState{
		currentAssignMap: map[string]assignState{},
	}
	s.lockBegin = next
	s.createInitNodes(sess)
}

func (s *Sharding) createCompleted(sess *curator.Session) {
	if s.state.lockCreated && s.state.nodesCreated && s.state.assignsCreated {
		if s.state.leaderStarted {
			panic("Leader already started")
		}
		s.state.leaderStarted = true
		s.lockBegin(sess)
	}
}

func (s *Sharding) createInitNodes(sess *curator.Session) {
	sessMustCreatePersistence(sess, s.getLockPath(), func(resp zk.CreateResponse) {
		s.state.lockCreated = true
		s.createCompleted(sess)
	})

	sessMustCreatePersistence(sess, s.getNodesPath(), func(resp zk.CreateResponse) {
		s.createEphemeralNode(sess)
	})

	sessMustCreatePersistence(sess, s.getAssignsPath(), func(resp zk.CreateResponse) {
		s.state.assignsCreated = true
		s.createCompleted(sess)
	})
}

func (s *Sharding) createEphemeralNode(sess *curator.Session) {
	pathVal := s.getNodesPath() + "/" + s.nodeID
	data := nodeData{Address: s.nodeAddr}.marshalJSON()

	sessMustCreateWithData(sess, pathVal, zk.FlagEphemeral, data, func(resp zk.CreateResponse) {
		s.state.nodesCreated = true
		s.createCompleted(sess)
	})
}

func (s *Sharding) onLeaderCallback(sess *curator.Session, _ func(sess *curator.Session)) {
	s.logger.Infof("Leader Started")
	s.listAssignNodes(sess)
	s.listActiveNodes(sess)
}

func (s *Sharding) getAssignNodeData(sess *curator.Session, nodeID string, getRemains *int) {
	sess.Run(func(client curator.Client) {
		client.Get(s.getAssignsPath()+"/"+nodeID, func(resp zk.GetResponse, err error) {
			*getRemains--

			if err != nil {
				if errors.Is(err, zk.ErrConnectionClosed) {
					if *getRemains <= 0 {
						sess.AddRetry(s.listAssignNodes)
					}
					return
				}
				if errors.Is(err, zk.ErrNoNode) {
					s.listAssignNodes(sess)
					return
				}
				panic(err)
			}

			var assign assignData
			if err := json.Unmarshal(resp.Data, &assign); err != nil {
				panic(err)
			}
			s.state.currentAssignMap[nodeID] = assignState{
				version: resp.Stat.Version,
				shards:  assign.Shards,
			}

			if *getRemains <= 0 {
				s.state.getAssignNodesCompleted = true
				s.startHandleNodeChanges(sess)
			}
		})
	})
}

func (s *Sharding) startHandleNodeChanges(sess *curator.Session) {
	if s.state.listActiveNodesCompleted && s.state.getAssignNodesCompleted {
		sess.Run(func(client curator.Client) {
			s.handleNodesChanged(sess)
		})
	}
}

func (s *Sharding) listAssignNodes(sess *curator.Session) {
	sessMustChildren(sess, s.getAssignsPath(), func(resp zk.ChildrenResponse) {
		getRemains := len(resp.Children)
		for _, nodeID := range resp.Children {
			s.getAssignNodeData(sess, nodeID, &getRemains)
		}
		if getRemains <= 0 {
			s.state.getAssignNodesCompleted = true
			s.startHandleNodeChanges(sess)
		}
	})
}

func (s *Sharding) listActiveNodes(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(s.getNodesPath(), func(resp zk.ChildrenResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrConnectionClosed) {
					sess.AddRetry(s.listActiveNodes)
					return
				}
				panic(err)
			}
			s.state.nodes = resp.Children
			slices.Sort(s.state.nodes)
			s.state.listActiveNodesCompleted = true
			s.startHandleNodeChanges(sess)
		}, func(ev zk.Event) {
			if ev.Type == zk.EventNodeChildrenChanged {
				s.listActiveNodes(sess)
			}
		})
	})
}

func (s *Sharding) handleNodesChanged(sess *curator.Session) {
	n := ShardID(len(s.state.nodes))
	minShardsPerNode := s.numShards / n
	maxShardsPerNode := (s.numShards + n - 1) / n
	numMax := s.numShards - minShardsPerNode*n

	nodes := slices.Clone(s.state.nodes)
	slices.SortStableFunc(nodes, func(a, b string) int {
		assignA := s.state.currentAssignMap[a].shards
		assignB := s.state.currentAssignMap[b].shards
		return len(assignB) - len(assignA)
	})
	nodeSet := map[string]struct{}{}
	for _, nodeID := range nodes {
		nodeSet[nodeID] = struct{}{}
	}

	freeShards := map[ShardID]struct{}{}
	for id := 0; id < int(s.numShards); id++ {
		freeShards[ShardID(id)] = struct{}{}
	}

	var deleted []string
	for nodeID, assigningState := range s.state.currentAssignMap {
		_, ok := nodeSet[nodeID]
		if !ok {
			deleted = append(deleted, nodeID)
			continue
		}
		for _, id := range assigningState.shards {
			delete(freeShards, id)
		}
	}

	actionCount := 0

	for i := 0; i < int(numMax); i++ {
		s.updateIfChanged(sess, nodes[i], int(maxShardsPerNode), freeShards, &actionCount)
	}

	for i := int(numMax); i < int(n); i++ {
		s.updateIfChanged(sess, nodes[i], int(minShardsPerNode), freeShards, &actionCount)
	}

	slices.Sort(deleted)
	for _, nodeID := range deleted {
		s.deleteAssignNode(sess, nodeID, &actionCount)
	}
}

func freeShardsToList(freeShards map[ShardID]struct{}) []ShardID {
	list := make([]ShardID, 0, len(freeShards))
	for id := range freeShards {
		list = append(list, id)
	}
	slices.Sort(list)
	return list
}

func (s *Sharding) updateIfChanged(
	sess *curator.Session,
	nodeID string, expectLen int, freeShards map[ShardID]struct{},
	actionCount *int,
) {
	current := slices.Clone(s.state.currentAssignMap[nodeID].shards)
	if len(current) > expectLen {
		for _, id := range current[expectLen:] {
			freeShards[id] = struct{}{}
		}
		current = current[:expectLen]
		s.upsertAssigns(sess, nodeID, current, actionCount)
		return
	}
	if len(current) < expectLen {
		list := freeShardsToList(freeShards)
		missing := expectLen - len(current)
		current = append(current, list[:missing]...)
		for _, id := range list[:missing] {
			delete(freeShards, id)
		}
		s.upsertAssigns(sess, nodeID, current, actionCount)
		return
	}
}

func marshalAssignNodeData(shards []ShardID) []byte {
	data, err := json.Marshal(assignData{
		Shards: shards,
	})
	if err != nil {
		panic(err)
	}
	return data
}

func (s *Sharding) getNodeAssignPath(nodeID string) string {
	return s.getAssignsPath() + "/" + nodeID
}

func (s *Sharding) upsertAssigns(
	sess *curator.Session, nodeID string, shards []ShardID,
	actionCount *int,
) {
	prev, ok := s.state.currentAssignMap[nodeID]
	if ok {
		s.updateAssignNode(sess, nodeID, shards, prev, actionCount)
	} else {
		s.createAssignNode(sess, nodeID, shards, actionCount)
	}
}

func (s *Sharding) retryListAssignsIfErr(sess *curator.Session, err error, actionCount *int) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, zk.ErrConnectionClosed) {
		if *actionCount <= 0 {
			sess.AddRetry(s.listAssignNodes)
		}
		return true
	}
	if isOneOfErrors(err,
		zk.ErrBadVersion, zk.ErrNodeExists, zk.ErrNoNode,
	) {
		s.listAssignNodes(sess)
		return true
	}
	panic(err)
}

func isOneOfErrors(err error, errorList ...error) bool {
	for _, cmpErr := range errorList {
		if errors.Is(err, cmpErr) {
			return true
		}
	}
	return false
}

func (s *Sharding) updateAssignNode(
	sess *curator.Session, nodeID string,
	shards []ShardID, prev assignState,
	actionCount *int,
) {
	sess.Run(func(client curator.Client) {
		pathVal := s.getNodeAssignPath(nodeID)
		data := marshalAssignNodeData(shards)

		*actionCount++
		client.Set(pathVal, data, prev.version, func(resp zk.SetResponse, err error) {
			*actionCount--
			if s.retryListAssignsIfErr(sess, err, actionCount) {
				return
			}
			s.state.currentAssignMap[nodeID] = assignState{
				version: resp.Stat.Version,
				shards:  shards,
			}
		})
	})
}

func (s *Sharding) createAssignNode(
	sess *curator.Session, nodeID string,
	shards []ShardID, actionCount *int,
) {
	sess.Run(func(client curator.Client) {
		pathVal := s.getNodeAssignPath(nodeID)
		data := marshalAssignNodeData(shards)
		*actionCount++
		client.Create(pathVal, data, 0, func(resp zk.CreateResponse, err error) {
			*actionCount--
			if s.retryListAssignsIfErr(sess, err, actionCount) {
				return
			}
			s.state.currentAssignMap[nodeID] = assignState{
				shards: shards,
			}
		})
	})
}

func (s *Sharding) deleteAssignNode(sess *curator.Session, nodeID string, actionCount *int) {
	sess.Run(func(client curator.Client) {
		version := s.state.currentAssignMap[nodeID].version

		*actionCount++
		client.Delete(s.getAssignsPath()+"/"+nodeID, version, func(resp zk.DeleteResponse, err error) {
			*actionCount--
			if s.retryListAssignsIfErr(sess, err, actionCount) {
				return
			}
			delete(s.state.currentAssignMap, nodeID)
		})
	})
}

func (s *Sharding) GetCurator() *curator.Curator {
	return s.cur
}
