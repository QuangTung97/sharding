package sharding

import (
	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

type Node struct {
	ID      string
	Address string
	Shards  []ShardID
	MZxid   int64 // updated zxid
}

type ChangeEvent struct {
	Old []Node
	New []Node
}

type ObserverFunc func(event ChangeEvent)

type observerCore struct {
	parent       string
	observerFunc ObserverFunc
}

func newObserverCore(parent string, observerFunc ObserverFunc) *observerCore {
	return &observerCore{
		parent:       parent,
		observerFunc: observerFunc,
	}
}

func (c *observerCore) onStart(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(c.parent+nodeZNodeName,
			func(resp zk.ChildrenResponse, err error) {
				if err != nil {
					panic(err)
				}
			},
			func(ev zk.Event) {
			},
		)
	})
}
