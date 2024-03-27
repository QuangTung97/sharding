package sharding

import (
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/concurrency"
	"github.com/QuangTung97/zk/curator"
)

type Sharding struct {
	parentPath string

	cur *curator.Curator

	lockNodeCreated bool

	lockBegin func(sess *curator.Session)
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

func (s *Sharding) createContainerNodes(sess *curator.Session, next func(sess *curator.Session)) {
	s.lockBegin = next
	s.createInitNodes(sess)
}

func (s *Sharding) createCompleted(sess *curator.Session) {
	s.lockBegin(sess)
}

func (s *Sharding) handleCreateLockPath(sess *curator.Session) func(resp zk.GetResponse, err error) {
	return func(resp zk.GetResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrNoNode) {
				s.createLockNode(sess)
				return
			}
			panic(err)
		}
		s.lockNodeCreated = true
		s.createCompleted(sess)
	}
}

func (s *Sharding) createInitNodes(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Get(s.getLockPath(), s.handleCreateLockPath(sess))
	})
}

func (s *Sharding) createLockNode(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.Create(s.getLockPath(), nil, 0, func(resp zk.CreateResponse, err error) {
			s.lockNodeCreated = true
			s.createCompleted(sess)
		})
	})
}

func (s *Sharding) onLeaderCallback(sess *curator.Session, _ func(sess *curator.Session)) {
}

func (s *Sharding) GetCurator() *curator.Curator {
	return s.cur
}
