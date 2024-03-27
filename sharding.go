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

type sessionState struct {
	lockCreated    bool
	nodesCreated   bool
	assignsCreated bool
	leaderStarted  bool

	nodes            []string
	currentAssigns   []string
	currentAssignMap map[string][]ShardID
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
	s.state = &sessionState{}
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
	sess.Run(func(client curator.Client) {
		client.Create(s.getLockPath(), nil, 0, func(resp zk.CreateResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrNodeExists) {
					s.state.lockCreated = true
					s.createCompleted(sess)
					return
				}
				panic(err)
			}
			s.state.lockCreated = true
			s.createCompleted(sess)
		})
		client.Create(s.getNodesPath(), nil, 0, func(resp zk.CreateResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrNodeExists) {
					s.createEphemeralNode(sess)
					return
				}
				panic(err)
			}
			s.createEphemeralNode(sess)
		})
		client.Create(s.getAssignsPath(), nil, 0, func(resp zk.CreateResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrNodeExists) {
					s.assignsCreated(sess)
					return
				}
				panic(err)
			}
			s.assignsCreated(sess)
		})
	})
}

func (s *Sharding) assignsCreated(sess *curator.Session) {
	s.state.assignsCreated = true
	s.createCompleted(sess)
}

func (s *Sharding) createEphemeralNode(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Create(s.getNodesPath()+"/"+s.nodeID,
			[]byte("address01"),
			zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				if err != nil {
					if errors.Is(err, zk.ErrNodeExists) {
						s.nodesCreated(sess)
						return
					}
					panic(err)
				}
				s.nodesCreated(sess)
			},
		)
	})
}

func (s *Sharding) nodesCreated(sess *curator.Session) {
	s.state.nodesCreated = true
	s.createCompleted(sess)
}

func (s *Sharding) onLeaderCallback(sess *curator.Session, _ func(sess *curator.Session)) {
	s.leaderStarted(sess)
}

func (s *Sharding) leaderStarted(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(s.getNodesPath(), func(resp zk.ChildrenResponse, err error) {
			if err != nil {
				panic(err)
			}

			s.state.nodes = resp.Children
			slices.Sort(s.state.nodes)

			s.handleNodesChanged(sess)
		}, func(ev zk.Event) {
		})
	})
}

func (s *Sharding) handleNodesChanged(sess *curator.Session) {
	n := ShardID(len(s.state.nodes))
	minShardsPerNode := s.numShards / n
	maxShardsPerNode := (s.numShards + n - 1) / n
	numMax := s.numShards - minShardsPerNode*n

	nodes := make([]string, n)
	copy(nodes, s.state.nodes)
	slices.SortStableFunc(nodes, func(a, b string) int {
		assignA := s.state.currentAssignMap[a]
		assignB := s.state.currentAssignMap[b]
		return len(assignB) - len(assignA)
	})

	freeShards := map[ShardID]struct{}{}
	for id := 0; id < int(s.numShards); id++ {
		freeShards[ShardID(id)] = struct{}{}
	}

	for _, list := range s.state.currentAssignMap {
		for _, id := range list {
			delete(freeShards, id)
		}
	}

	for i := 0; i < int(numMax); i++ {
		s.updateIfChanged(sess, nodes[i], int(maxShardsPerNode), freeShards)
	}

	for i := int(numMax); i < int(n); i++ {
		s.updateIfChanged(sess, nodes[i], int(minShardsPerNode), freeShards)
	}
}

type ShardID uint32

type assignData struct {
	Shards []ShardID `json:"shards"`
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
	current := s.state.currentAssignMap[nodeID]
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
	sess.Run(func(client curator.Client) {
		client.Create(pathVal, data, 0, func(resp zk.CreateResponse, err error) {
			if err != nil {
				panic(err)
			}
		})
	})
}

func (s *Sharding) GetCurator() *curator.Curator {
	return s.cur
}
