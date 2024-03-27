package sharding

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/concurrency"
	"github.com/QuangTung97/zk/curator"
)

type Sharding struct {
	parentPath string
	nodeID     string

	cur *curator.Curator

	state *sessionState

	lockBegin func(sess *curator.Session)
}

type sessionState struct {
	lockNodeCreated bool
	nodesCreated    bool
	leaderStarted   bool
}

func NewNodeID() string {
	var data [16]byte
	_, err := rand.Reader.Read(data[:])
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}

func New(parentPath string, nodeID string) *Sharding {
	s := &Sharding{
		parentPath: parentPath,
		nodeID:     nodeID,
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

func (s *Sharding) createContainerNodes(sess *curator.Session, next func(sess *curator.Session)) {
	s.state = &sessionState{}
	s.lockBegin = next
	s.createInitNodes(sess)
}

func (s *Sharding) createCompleted(sess *curator.Session) {
	if s.state.lockNodeCreated && s.state.nodesCreated {
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
			s.state.lockNodeCreated = true
			s.createCompleted(sess)
		})
		client.Create(s.getNodesPath(), nil, 0, func(resp zk.CreateResponse, err error) {
			if err != nil {
				panic(err)
			}
			s.createEphemeralNode(sess)
		})
	})
}

func (s *Sharding) createEphemeralNode(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Create(s.getNodesPath()+"/"+s.nodeID,
			[]byte("address01"),
			zk.FlagEphemeral,
			func(resp zk.CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
				s.state.nodesCreated = true
				s.createCompleted(sess)
			},
		)
	})
}

func (s *Sharding) onLeaderCallback(sess *curator.Session, _ func(sess *curator.Session)) {
}

func (s *Sharding) GetCurator() *curator.Curator {
	return s.cur
}
