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

	cur *curator.Curator

	state *sessionState

	lockBegin func(sess *curator.Session)
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

	getAssignsRemain         int
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

func New(parentPath string, nodeID string, numShards ShardID) *Sharding {
	s := &Sharding{
		parentPath: parentPath,
		nodeID:     nodeID,
		numShards:  numShards,
	}

	lock := concurrency.NewLock(s.getLockPath(), nodeID)

	s.cur = curator.NewChain(
		s.createContainerNodes,
		lock.Start,
		s.onLeaderCallback,
	)

	return s
}

func (s *Sharding) getLockPath() string {
	return s.parentPath + "/lock"
}

func (s *Sharding) getNodesPath() string {
	return s.parentPath + "/nodes"
}

func (s *Sharding) getAssignsPath() string {
	return s.parentPath + "/assigns"
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
	sessMustCreate(sess, pathVal, zk.FlagEphemeral, func(resp zk.CreateResponse) {
		s.state.nodesCreated = true
		s.createCompleted(sess)
	})
}

func (s *Sharding) onLeaderCallback(sess *curator.Session, _ func(sess *curator.Session)) {
	s.listAssignNodes(sess)
	s.listActiveNodes(sess)
}

func (s *Sharding) getAssignNodeData(sess *curator.Session, nodeID string) {
	sess.Run(func(client curator.Client) {
		client.Get(s.getAssignsPath()+"/"+nodeID, func(resp zk.GetResponse, err error) {
			// TODO Check Connection Error
			if err != nil {
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
			s.state.getAssignsRemain--
			s.startHandleNodeChanges(sess)
		})
	})
}

func (s *Sharding) startHandleNodeChanges(sess *curator.Session) {
	if s.state.getAssignsRemain == 0 && s.state.listActiveNodesCompleted {
		s.handleNodesChanged(sess)
	}
}

func (s *Sharding) listAssignNodes(sess *curator.Session) {
	sessMustChildren(sess, s.getAssignsPath(), func(resp zk.ChildrenResponse) {
		s.state.getAssignsRemain = len(resp.Children)
		for _, nodeID := range resp.Children {
			s.getAssignNodeData(sess, nodeID)
		}
		s.startHandleNodeChanges(sess)
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

	for i := 0; i < int(numMax); i++ {
		s.updateIfChanged(sess, nodes[i], int(maxShardsPerNode), freeShards)
	}

	for i := int(numMax); i < int(n); i++ {
		s.updateIfChanged(sess, nodes[i], int(minShardsPerNode), freeShards)
	}

	slices.Sort(deleted)
	for _, nodeID := range deleted {
		s.deleteAssignNode(sess, nodeID)
	}
}

func (s *Sharding) deleteAssignNode(sess *curator.Session, nodeID string) {
	sess.Run(func(client curator.Client) {
		version := s.state.currentAssignMap[nodeID].version
		client.Delete(s.getAssignsPath()+"/"+nodeID, version, func(resp zk.DeleteResponse, err error) {
			// TODO Check Connection Error
			if err != nil {
				panic(err)
			}
			delete(s.state.currentAssignMap, nodeID)
		})
	})
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
) {
	current := slices.Clone(s.state.currentAssignMap[nodeID].shards)
	if len(current) > expectLen {
		return
	}
	if len(current) < expectLen {
		list := freeShardsToList(freeShards)
		missing := expectLen - len(current)
		current = append(current, list[:missing]...)
		for _, id := range list[:missing] {
			delete(freeShards, id)
		}
		s.upsertAssigns(sess, nodeID, current)
		return
	}
}

func (s *Sharding) upsertAssigns(sess *curator.Session, nodeID string, shards []ShardID) {
	pathVal := s.getAssignsPath() + "/" + nodeID
	data, err := json.Marshal(assignData{
		Shards: shards,
	})
	if err != nil {
		panic(err)
	}

	prev, ok := s.state.currentAssignMap[nodeID]
	if ok {
		sess.Run(func(client curator.Client) {
			client.Set(pathVal, data, prev.version, func(resp zk.SetResponse, err error) {
				// TODO Check Connection Error
				if err != nil {
					panic(err)
				}
				s.state.currentAssignMap[nodeID] = assignState{
					version: resp.Stat.Version,
					shards:  shards,
				}
			})
		})
	} else {
		sess.Run(func(client curator.Client) {
			client.Create(pathVal, data, 0, func(resp zk.CreateResponse, err error) {
				// TODO Check Connection Error
				if err != nil {
					panic(err)
				}
				s.state.currentAssignMap[nodeID] = assignState{
					shards: shards,
				}
			})
		})
	}
}

func (s *Sharding) GetCurator() *curator.Curator {
	return s.cur
}
