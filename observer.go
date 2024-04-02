package sharding

import (
	"encoding/json"

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

type observerNodeData struct {
	data   nodeData
	shards []ShardID
	mzxid  int64
}

type observerCore struct {
	parent       string
	observerFunc ObserverFunc

	remainNodes int

	nodes map[string]*observerNodeData
}

func newObserverCore(parent string, observerFunc ObserverFunc) *observerCore {
	return &observerCore{
		parent:       parent,
		observerFunc: observerFunc,

		nodes: map[string]*observerNodeData{},
	}
}

func (c *observerCore) onStart(sess *curator.Session) {
	c.listNodes(sess)
	c.listAssigns(sess)
}

func (c *observerCore) listNodes(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(c.parent+nodeZNodeName,
			func(resp zk.ChildrenResponse, err error) {
				if err != nil {
					panic(err)
				}
				c.handleNodesChildren(sess, resp)
			},
			func(ev zk.Event) {
			},
		)
	})
}

func (c *observerCore) listAssigns(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(c.parent+assignZNodeName,
			func(resp zk.ChildrenResponse, err error) {
				if err != nil {
					panic(err)
				}
				c.handleAssignsChildren(sess, resp)
			},
			func(ev zk.Event) {
				c.listAssigns(sess)
			},
		)
	})

}

func (c *observerCore) handleNodesChildren(sess *curator.Session, resp zk.ChildrenResponse) {
	for _, node := range resp.Children {
		sess.Run(func(client curator.Client) {
			c.remainNodes++
			client.Get(c.parent+nodeZNodeName+"/"+node, func(resp zk.GetResponse, err error) {
				c.remainNodes--
				if err != nil {
					panic(err)
				}
				c.handleNodeData(sess, node, resp)
			})
		})
	}
}

func (c *observerCore) handleAssignsChildren(sess *curator.Session, resp zk.ChildrenResponse) {
	for _, child := range resp.Children {
		c.getAssignNode(sess, child)
	}
}

func (c *observerCore) notifyObserver() {
	var newList []Node
	for nodeID, info := range c.nodes {
		if len(info.data.Address) == 0 {
			continue
		}
		if info.mzxid <= 0 {
			continue
		}
		newList = append(newList, Node{
			ID:      nodeID,
			Address: info.data.Address,
			Shards:  info.shards,
			MZxid:   info.mzxid,
		})
	}
	c.observerFunc(ChangeEvent{
		New: newList,
	})
}

func (c *observerCore) getAssignNode(sess *curator.Session, nodeID string) {
	sess.Run(func(client curator.Client) {
		client.GetW(c.parent+assignZNodeName+"/"+nodeID, func(resp zk.GetResponse, err error) {
			if err != nil {
				panic(err)
			}
			c.handleGetAssignData(nodeID, resp)
		}, func(ev zk.Event) {
		})
	})
}

func (c *observerCore) handleGetAssignData(nodeID string, resp zk.GetResponse) {
	n := c.getNode(nodeID)
	n.mzxid = resp.Stat.Mzxid
	var assignVal assignData
	if err := json.Unmarshal(resp.Data, &assignVal); err != nil {
		panic(err)
	}
	n.shards = assignVal.Shards
	c.notifyObserver()
}

func (c *observerCore) getNode(nodeID string) *observerNodeData {
	n, ok := c.nodes[nodeID]
	if !ok {
		n = &observerNodeData{}
		c.nodes[nodeID] = n
	}
	return n
}

func (c *observerCore) handleNodeData(sess *curator.Session, nodeID string, resp zk.GetResponse) {
	var data nodeData
	err := json.Unmarshal(resp.Data, &data)
	if err != nil {
		panic(err)
	}
	n := c.getNode(nodeID)
	n.data = data
}
