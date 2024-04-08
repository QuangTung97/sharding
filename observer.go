package sharding

import (
	"cmp"
	"encoding/json"
	"errors"
	"slices"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

// Node information when observing changes
type Node struct {
	ID      string
	Address string
	Shards  []ShardID
	MZxid   int64 // updated zxid
}

// ChangeEvent happens every time zookeeper state changed
type ChangeEvent struct {
	Old []Node
	New []Node
}

// ObserverFunc is the callback function of observer
type ObserverFunc func(event ChangeEvent)

type observerNodeData struct {
	data   nodeData
	shards []ShardID
	mzxid  int64
}

type observerCore struct {
	parent       string
	numShards    ShardID
	observerFunc ObserverFunc

	// state data
	oldNotify []Node
	nodes     map[string]*observerNodeData
}

func newObserverCore(parent string, numShards ShardID, observerFunc ObserverFunc) *observerCore {
	return &observerCore{
		parent:       parent,
		numShards:    numShards,
		observerFunc: observerFunc,
	}
}

func (c *observerCore) initState() {
	c.nodes = map[string]*observerNodeData{}
}

func (c *observerCore) onStart(sess *curator.Session) {
	c.initState()
	c.listNodes(sess)
	c.listAssigns(sess)
}

func (c *observerCore) listNodes(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(c.parent+nodeZNodeName,
			func(resp zk.ChildrenResponse, err error) {
				if err != nil {
					if errors.Is(err, zk.ErrConnectionClosed) {
						sess.AddRetry(c.listNodes)
						return
					}
					panic(err)
				}
				c.handleNodesChildren(sess, resp)
			},
			func(ev zk.Event) {
				if ev.Type == zk.EventNodeChildrenChanged {
					c.listNodes(sess)
				}
			},
		)
	})
}

func (c *observerCore) listAssigns(sess *curator.Session) {
	sess.Run(func(client curator.Client) {
		client.ChildrenW(c.parent+assignZNodeName,
			func(resp zk.ChildrenResponse, err error) {
				if err != nil {
					if errors.Is(err, zk.ErrConnectionClosed) {
						sess.AddRetry(c.listAssigns)
						return
					}
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

func (c *observerCore) getNodeIDsNotInList(l []string) []string {
	inputSet := map[string]struct{}{}
	for _, e := range l {
		inputSet[e] = struct{}{}
	}

	var result []string
	for k := range c.nodes {
		_, ok := inputSet[k]
		if !ok {
			result = append(result, k)
		}
	}

	return result
}

func (c *observerCore) handleNodesChildren(sess *curator.Session, resp zk.ChildrenResponse) {
	for _, tmpNode := range resp.Children {
		node := tmpNode
		n := c.getNode(node)
		if len(n.data.Address) > 0 {
			continue
		}
		c.getNodeData(sess, node)
	}

	c.cleanUpUnusedNodes(resp.Children, func(n *observerNodeData) {
		n.data = nodeData{}
	})
}

func (c *observerCore) getNodeData(sess *curator.Session, node string) {
	sess.Run(func(client curator.Client) {
		client.Get(c.parent+nodeZNodeName+"/"+node, func(resp zk.GetResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrConnectionClosed) {
					sess.AddRetry(func(sess *curator.Session) {
						c.getNodeData(sess, node)
					})
					return
				}
				if errors.Is(err, zk.ErrNoNode) {
					return
				}
				panic(err)
			}
			c.handleNodeData(node, resp)
		})
	})
}

func (c *observerCore) cleanUpUnusedNodes(newChildren []string, handler func(n *observerNodeData)) {
	checkNodes := c.getNodeIDsNotInList(newChildren)
	var deletedNodes []string

	for _, nodeID := range checkNodes {
		info := c.getNode(nodeID)
		handler(info)
		if len(info.data.Address) == 0 && info.mzxid == 0 {
			deletedNodes = append(deletedNodes, nodeID)
		}
	}

	for _, node := range deletedNodes {
		delete(c.nodes, node)
	}
}

func (c *observerCore) handleAssignsChildren(sess *curator.Session, resp zk.ChildrenResponse) {
	for _, tmpNode := range resp.Children {
		child := tmpNode
		n := c.getNode(child)
		if n.mzxid > 0 {
			continue
		}
		c.getAssignNode(sess, child)
	}

	c.cleanUpUnusedNodes(resp.Children, func(n *observerNodeData) {
		n.mzxid = 0
	})
}

type shardAssign struct {
	nodeID string
	zxid   int64
}

func getKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (c *observerCore) notifyObserver() {
	shardAlloc := map[ShardID]shardAssign{}

	nodeIDList := getKeys(c.nodes)

	for _, nodeID := range nodeIDList {
		info := c.nodes[nodeID]

		if len(info.data.Address) == 0 {
			continue
		}
		if info.mzxid <= 0 {
			continue
		}

		for _, shardID := range info.shards {
			prev := shardAlloc[shardID]
			if prev.zxid > info.mzxid {
				continue
			}
			shardAlloc[shardID] = shardAssign{
				nodeID: nodeID,
				zxid:   info.mzxid,
			}
		}
	}

	var newList []Node
	for nodeID, info := range c.nodes {
		var newShards []ShardID
		for _, shardID := range info.shards {
			n := shardAlloc[shardID]
			if n.nodeID == nodeID {
				newShards = append(newShards, shardID)
			}
		}

		if len(newShards) == 0 {
			continue
		}

		newList = append(newList, Node{
			ID:      nodeID,
			Address: info.data.Address,
			Shards:  newShards,
			MZxid:   info.mzxid,
		})
	}

	if len(shardAlloc) < int(c.numShards) {
		return
	}

	slices.SortFunc(newList, func(a, b Node) int {
		return cmp.Compare(a.ID, b.ID)
	})

	oldList := c.oldNotify
	if slices.EqualFunc(oldList, newList, nodeEqual) {
		return
	}

	c.oldNotify = slices.Clone(newList)
	c.observerFunc(ChangeEvent{
		Old: oldList,
		New: newList,
	})
}

func nodeEqual(a, b Node) bool {
	if a.ID != b.ID {
		return false
	}
	if a.Address != b.Address {
		return false
	}
	if a.MZxid != b.MZxid {
		return false
	}
	return slices.Equal(a.Shards, b.Shards)
}

//revive:disable-next-line:cognitive-complexity
func (c *observerCore) getAssignNode(sess *curator.Session, nodeID string) {
	sess.Run(func(client curator.Client) {
		client.GetW(c.parent+assignZNodeName+"/"+nodeID, func(resp zk.GetResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrConnectionClosed) {
					sess.AddRetry(func(sess *curator.Session) {
						c.getAssignNode(sess, nodeID)
					})
					return
				}
				if errors.Is(err, zk.ErrNoNode) {
					return
				}
				panic(err)
			}
			c.handleGetAssignData(nodeID, resp)
		}, func(ev zk.Event) {
			if ev.Type == zk.EventNodeDataChanged {
				c.getAssignNode(sess, nodeID)
			} else if ev.Type == zk.EventNodeDeleted {
				n := c.getNode(nodeID)
				n.mzxid = 0
			}
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

func (c *observerCore) handleNodeData(nodeID string, resp zk.GetResponse) {
	var data nodeData
	err := json.Unmarshal(resp.Data, &data)
	if err != nil {
		panic(err)
	}
	n := c.getNode(nodeID)
	n.data = data
	c.notifyObserver()
}
