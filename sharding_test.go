package sharding

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
	"github.com/stretchr/testify/assert"
)

func TestNewNodeID(t *testing.T) {
	nodeID := NewNodeID()
	fmt.Println(nodeID)
	assert.Equal(t, 32, len(nodeID))
}

const client1 curator.FakeClientID = "client1"
const client2 curator.FakeClientID = "client2"
const client3 curator.FakeClientID = "client3"
const client4 curator.FakeClientID = "client4"

const initClient curator.FakeClientID = "init"

const parentPath = "/sharding"
const numShards = 8

func initStore() *curator.FakeZookeeper {
	store := curator.NewFakeZookeeper()

	c := curator.NewFakeClientFactory(store, initClient)
	c.Start(curator.New(func(sess *curator.Session) {
		sess.Run(func(client curator.Client) {
			client.Create(parentPath, nil, 0, func(resp zk.CreateResponse, err error) {
				if err != nil {
					panic(err)
				}
			})
		})
	}))

	store.Begin(initClient)
	store.CreateApply(initClient)
	if len(store.PendingCalls(initClient)) > 0 {
		panic("must be empty")
	}

	return store
}

func startSharding(
	store *curator.FakeZookeeper,
	client curator.FakeClientID,
	nodeID string,
	options ...Option,
) *Sharding {
	factory := curator.NewFakeClientFactory(store, client)
	s := New(parentPath, nodeID, numShards, fmt.Sprintf("%s-addr:4001", nodeID), options...)
	s.clientID = client
	factory.Start(s.GetCurator())
	return s
}

func TestSharding_Begin(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")

	store.Begin(client1)
	store.CreateApply(client1) // create lock
	store.CreateApply(client1) // create nodes
	store.CreateApply(client1) // create assigns
	store.CreateApply(client1) // create nodes/node01

	// Lock Start
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)

	store.PrintData()
	store.PrintPendingCalls()

	lockNode := store.Root.Children[0]
	assert.Equal(t, "assigns", lockNode.Children[2].Name)
	children := lockNode.Children[2].Children
	assert.Equal(t, 1, len(children))
	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3,4,5,6,7]}`, string(children[0].Data))

	assert.Equal(t, "nodes", lockNode.Children[1].Name)
	nodes := lockNode.Children[1]
	assert.Equal(t, "node01", nodes.Children[0].Name)
	assert.Equal(t, `{"address":"node01-addr:4001"}`, string(nodes.Children[0].Data))
}

func initContainerNodes(store *curator.FakeZookeeper, client curator.FakeClientID) {
	store.CreateApply(client) // create lock
	store.CreateApply(client) // create nodes
	store.CreateApply(client) // create assigns
	store.CreateApply(client) // create nodes/node01
}

func lockGranted(store *curator.FakeZookeeper, client curator.FakeClientID) {
	store.ChildrenApply(client)
	store.CreateApply(client)
	store.ChildrenApply(client)
}

func lockBlocked(store *curator.FakeZookeeper, client curator.FakeClientID) {
	store.ChildrenApply(client)
	store.CreateApply(client)
	store.ChildrenApply(client)
	store.GetApply(client)
}

func TestSharding_Two_Nodes(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")

	store.Begin(client1)
	store.Begin(client2)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)

	lockGranted(store, client1)
	lockBlocked(store, client2)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)

	store.PrintData()
	store.PrintPendingCalls()

	lockNode := store.Root.Children[0]
	assert.Equal(t, "assigns", lockNode.Children[2].Name)
	children := lockNode.Children[2].Children

	assert.Equal(t, 2, len(children))

	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3]}`, string(children[0].Data))

	assert.Equal(t, "node02", children[1].Name)
	assert.Equal(t, `{"shards":[4,5,6,7]}`, string(children[1].Data))
}

func TestSharding_Three_Nodes(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03")

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)
	lockBlocked(store, client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	lockNode := store.Root.Children[0]
	assert.Equal(t, "assigns", lockNode.Children[2].Name)
	children := lockNode.Children[2].Children

	assert.Equal(t, 3, len(children))

	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2]}`, string(children[0].Data))

	assert.Equal(t, "node02", children[1].Name)
	assert.Equal(t, `{"shards":[3,4,5]}`, string(children[1].Data))

	assert.Equal(t, "node03", children[2].Name)
	assert.Equal(t, `{"shards":[6,7]}`, string(children[2].Data))
}

func TestSharding_Three_Nodes_One_Node_Deleted(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03")

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)
	lockBlocked(store, client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	// Node 3 Session Expired
	store.SessionExpired(client3)
	store.ChildrenApply(client1)

	store.SetApply(client1)
	store.SetApply(client1)
	store.DeleteApply(client1)

	store.PrintData()
	store.PrintPendingCalls()

	lockNode := store.Root.Children[0]
	assert.Equal(t, "assigns", lockNode.Children[2].Name)
	children := lockNode.Children[2].Children

	assert.Equal(t, 2, len(children))

	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3]}`, string(children[0].Data))

	assert.Equal(t, "node02", children[1].Name)
	assert.Equal(t, `{"shards":[4,5,6,7]}`, string(children[1].Data))
}

func TestSharding_Three_Nodes__Lease_Expired(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03")

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)
	lockBlocked(store, client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	// Leader Expired
	store.SessionExpired(client1)
	assert.Equal(t, []string{}, store.PendingCalls(client1))

	// Lock Granted for Client 2
	store.ChildrenApply(client2)

	store.ChildrenApply(client2)
	store.ChildrenApply(client2)

	store.GetApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)

	store.PrintData()
	store.PrintPendingCalls()

	store.SetApply(client2)
	store.SetApply(client2)
	store.DeleteApply(client2)

	assert.Equal(t, []string{}, store.PendingCalls(client1))

	lockNode := store.Root.Children[0]
	assert.Equal(t, "assigns", lockNode.Children[2].Name)
	children := lockNode.Children[2].Children

	assert.Equal(t, 2, len(children))

	assert.Equal(t, "node02", children[0].Name)
	assert.Equal(t, `{"shards":[3,4,5,0]}`, string(children[0].Data))

	assert.Equal(t, "node03", children[1].Name)
	assert.Equal(t, `{"shards":[6,7,1,2]}`, string(children[1].Data))
}

func TestSharding_Create_Container_Nodes__Connection_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)

	store.ConnError(client1)
	store.Retry(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	children := store.Root.Children[0].Children
	assert.Equal(t, 3, len(children))
	assert.Equal(t, "locks", children[0].Name)
	assert.Equal(t, "nodes", children[1].Name)
	assert.Equal(t, "assigns", children[2].Name)
}

func TestSharding_Create_Ephemeral_Node_Connection_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	store.ConnError(client1)
	store.Retry(client1)

	store.CreateApply(client1)

	children := store.Root.Children[0].Children[1].Children
	assert.Equal(t, 1, len(children))
	assert.Equal(t, "node01", children[0].Name)
}

func TestSharding_List_Assign_Nodes_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	lockGranted(store, client1)

	store.ConnError(client1)
	store.Retry(client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)

	children := store.Root.Children[0].Children[2].Children
	assert.Equal(t, 1, len(children))
	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3,4,5,6,7]}`, string(children[0].Data))
}

func TestSharding_Get_Assign_Node_Data_Conn_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)
	initContainerNodes(store, client1)

	lockGranted(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	store.SessionExpired(client1)

	// Start Client2
	startSharding(store, client2, "node02")
	store.Begin(client2)
	initContainerNodes(store, client2)

	lockGranted(store, client2)

	store.ChildrenApply(client2)
	store.ChildrenApply(client2)

	store.ConnError(client2)
	store.Retry(client2)
	store.ChildrenApply(client2)
	store.GetApply(client2)

	store.CreateApply(client2)
	store.DeleteApply(client2)

	children := store.Root.Children[0].Children[2].Children
	assert.Equal(t, 1, len(children))
	assert.Equal(t, "node02", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3,4,5,6,7]}`, string(children[0].Data))
}

func TestSharding_Get_Assign_Data_Of_Two_Nodes__Conn_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03")

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)
	lockBlocked(store, client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	// session expired
	store.SessionExpired(client1)

	// Client 2 Granted
	store.ChildrenApply(client2)

	// Get children of nodes and assigns
	store.ChildrenApply(client2)
	store.ChildrenApply(client2)

	store.ConnError(client2)
	store.Retry(client2)

	store.ChildrenApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)

	store.SetApply(client2)
	store.SetApply(client2)
	store.DeleteApply(client2)

	assert.Equal(t, []string{}, store.PendingCalls(client2))

	children := store.Root.Children[0].Children[2].Children
	assert.Equal(t, 2, len(children))

	assert.Equal(t, "node02", children[0].Name)
	assert.Equal(t, `{"shards":[3,4,5,0]}`, string(children[0].Data))

	assert.Equal(t, "node03", children[1].Name)
	assert.Equal(t, `{"shards":[6,7,1,2]}`, string(children[1].Data))
}

func TestSharding_Create_And_Delete_With_Conn_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)
	initContainerNodes(store, client1)

	lockGranted(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	store.SessionExpired(client1)

	// Start Client2
	startSharding(store, client2, "node02")
	store.Begin(client2)
	initContainerNodes(store, client2)

	lockGranted(store, client2)

	store.ChildrenApply(client2)
	store.ChildrenApply(client2)
	store.GetApply(client2)

	store.ConnError(client2)
	store.Retry(client2)
	store.ChildrenApply(client2)
	store.GetApply(client2)

	store.CreateApply(client2)
	store.DeleteApply(client2)

	children := store.Root.Children[0].Children[2].Children
	assert.Equal(t, 1, len(children))
	assert.Equal(t, "node02", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3,4,5,6,7]}`, string(children[0].Data))
}

func TestSharding__Set_Conn_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03")

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)
	lockBlocked(store, client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	// session expired
	store.SessionExpired(client1)

	// Client 2 Granted
	store.ChildrenApply(client2)

	// Get children of nodes and assigns
	store.ChildrenApply(client2)
	store.ChildrenApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)

	store.ConnError(client2)
	store.Retry(client2)

	store.ChildrenApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)

	store.SetApply(client2)
	store.SetApply(client2)
	store.DeleteApply(client2)

	assert.Equal(t, []string{}, store.PendingCalls(client2))

	children := store.Root.Children[0].Children[2].Children
	assert.Equal(t, 2, len(children))

	assert.Equal(t, "node02", children[0].Name)
	assert.Equal(t, `{"shards":[3,4,5,0]}`, string(children[0].Data))

	assert.Equal(t, "node03", children[1].Name)
	assert.Equal(t, `{"shards":[6,7,1,2]}`, string(children[1].Data))
}

func TestSharding__Client1_Created__Then_Client2_Added(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)

	initContainerNodes(store, client1)
	lockGranted(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	// Client 2 Started
	startSharding(store, client2, "node02")
	store.Begin(client2)

	initContainerNodes(store, client2)
	lockBlocked(store, client2)

	store.ChildrenApply(client1)
	store.SetApply(client1)
	store.CreateApply(client1)

	children := store.Root.Children[0].Children[2].Children
	assert.Equal(t, 2, len(children))

	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3]}`, string(children[0].Data))

	assert.Equal(t, "node02", children[1].Name)
	assert.Equal(t, `{"shards":[4,5,6,7]}`, string(children[1].Data))
}

func TestSharding_WithObserver_SingleNode__Then_New_Node_Added(t *testing.T) {
	store := initStore()

	var events []ChangeEvent

	sharding1 := startSharding(store, client1, "node01", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	store.Begin(client1)

	initContainerNodes(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	store.GetApply(client1)

	// lock completed
	store.ChildrenApply(client1)

	// create allocations
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	assert.Equal(t, 0, len(events))
	// list assigns again
	store.ChildrenApply(client1)

	assert.Equal(t, 0, len(events))
	store.GetApply(client1)
	assert.Equal(t, 1, len(events))

	assert.Equal(t, []ChangeEvent{
		{
			New: []Node{
				{
					ID:      "node01",
					Address: "node01-addr:4001",
					Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
					MZxid:   107,
				},
			},
		},
	}, events)

	// =========================
	// Second Node Started
	// =========================
	startSharding(store, client2, "node02")
	store.Begin(client2)

	initContainerNodes(store, client2)

	store.ChildrenApply(client1) // list children after event notified of observer
	store.ChildrenApply(client1) // list children for sharding

	store.GetApply(client1) // get nodes/node02

	store.SetApply(client1)
	store.CreateApply(client1)

	store.GetApply(client1)
	store.ChildrenApply(client1) // list children for assigns of observer

	assert.Equal(t, 1, len(events))
	store.GetApply(client1) // get assigns/node02
	assert.Equal(t, 2, len(events))
	assert.Equal(t, ChangeEvent{
		Old: []Node{
			{
				ID:      "node01",
				Address: "node01-addr:4001",
				Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
				MZxid:   107,
			},
		},
		New: []Node{
			{
				ID:      "node01",
				Address: "node01-addr:4001",
				Shards:  []ShardID{0, 1, 2, 3},
				MZxid:   109,
			},
			{
				ID:      "node02",
				Address: "node02-addr:4001",
				Shards:  []ShardID{4, 5, 6, 7},
				MZxid:   110,
			},
		},
	}, events[1])

	// =========================
	// Second Node Session Expired
	// =========================
	store.SessionExpired(client2)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	assert.Equal(t, 2, len(events))

	store.SetApply(client1)
	store.DeleteApply(client1)

	assert.Equal(t, 2, len(events))
	store.GetApply(client1) // get node01 for observer
	assert.Equal(t, 3, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
			MZxid:   112,
		},
	}, events[2].New)

	store.ChildrenApply(client1) // children assigns for observer

	assert.Equal(t, 0, len(store.PendingCalls(client1)))
	assert.Equal(t, 3, len(events))
	assert.Equal(t, []string{"node01"}, getKeys(sharding1.obs.nodes))
}

func TestSharding_WithObserver_OneNode_Then_2_Nodes_Added(t *testing.T) {
	store := initStore()

	var events []ChangeEvent

	sharding1 := startSharding(store, client1, "node01", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	store.Begin(client1)

	initContainerNodes(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.GetApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	store.ChildrenApply(client1)

	assert.Equal(t, 0, len(events))
	store.GetApply(client1)
	assert.Equal(t, 1, len(events))

	// =========================
	// 2 Other Nodes Started
	// =========================
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03")
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	// watch notify for sharding and observer of client1
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.GetApply(client1)
	store.GetApply(client1)

	store.SetApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	store.GetApply(client1)
	store.ChildrenApply(client1)
	store.GetApply(client1) // get assigns/node02 for observer

	assert.Equal(t, 1, len(events))
	store.GetApply(client1) // get assigns/node03 for observer
	assert.Equal(t, 2, len(events))
	assert.Equal(t, ChangeEvent{
		Old: []Node{
			{
				ID:      "node01",
				Address: "node01-addr:4001",
				Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
				MZxid:   107,
			},
		},
		New: []Node{
			{
				ID:      "node01",
				Address: "node01-addr:4001",
				Shards:  []ShardID{0, 1, 2},
				MZxid:   110,
			},
			{
				ID:      "node02",
				Address: "node02-addr:4001",
				Shards:  []ShardID{3, 4, 5},
				MZxid:   111,
			},
			{
				ID:      "node03",
				Address: "node03-addr:4001",
				Shards:  []ShardID{6, 7},
				MZxid:   112,
			},
		},
	}, events[1])

	assert.Equal(t, 0, len(store.PendingCalls(client1)))

	// =========================
	// 2 Other Nodes Expired
	// =========================
	store.SessionExpired(client2)
	store.SessionExpired(client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.SetApply(client1)
	store.DeleteApply(client1)
	store.DeleteApply(client1)

	assert.Equal(t, 2, len(events))
	store.GetApply(client1)
	assert.Equal(t, 3, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
			MZxid:   115,
		},
	}, events[2].New)

	store.ChildrenApply(client1)
	assert.Equal(t, 3, len(events))

	assert.Equal(t, 0, len(store.PendingCalls(client1)))
	assert.Equal(t, []string{"node01"}, getKeys(sharding1.obs.nodes))
}

func TestSharding_Observer_On_Non_Leader_Node(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")
	store.Begin(client1)

	initContainerNodes(store, client1)
	lockGranted(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	assert.Equal(t, 0, len(store.PendingCalls(client1)))

	// ========================
	// Second Node Started
	// ========================
	var events []ChangeEvent
	sharding2 := startSharding(store, client2, "node02", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	store.Begin(client2)
	initContainerNodes(store, client2)

	store.ChildrenApply(client2)
	store.ChildrenApply(client2)
	store.ChildrenApply(client2)
	store.CreateApply(client2)

	store.GetApply(client2)
	store.GetApply(client2)
	assert.Equal(t, 0, len(events))
	store.GetApply(client2) // get assigns node01
	assert.Equal(t, 1, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
			MZxid:   107,
		},
	}, events[0].New)

	store.ChildrenApply(client2)
	store.GetApply(client2) // client 2 watch for lock of client 1

	// Leader do rebalance
	store.ChildrenApply(client1)
	store.SetApply(client1)
	store.CreateApply(client1)

	store.GetApply(client2)
	store.ChildrenApply(client2)

	assert.Equal(t, 1, len(events))
	store.GetApply(client2)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3},
			MZxid:   110,
		},
		{
			ID:      "node02",
			Address: "node02-addr:4001",
			Shards:  []ShardID{4, 5, 6, 7},
			MZxid:   111,
		},
	}, events[1].New)

	assert.Equal(t, 0, len(store.PendingCalls(client1)))
	assert.Equal(t, 0, len(store.PendingCalls(client2)))

	// ========================
	// Leader Node Expired
	// ========================
	store.SessionExpired(client1)

	store.ChildrenApply(client2) // new leader
	store.ChildrenApply(client2)
	store.ChildrenApply(client2)
	store.ChildrenApply(client2)

	store.GetApply(client2)
	store.GetApply(client2)

	store.SetApply(client2)
	store.DeleteApply(client2)

	assert.Equal(t, 2, len(events))
	store.GetApply(client2)
	assert.Equal(t, 3, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node02",
			Address: "node02-addr:4001",
			Shards:  []ShardID{4, 5, 6, 7, 0, 1, 2, 3},
			MZxid:   113,
		},
	}, events[2].New)

	store.ChildrenApply(client2)
	assert.Equal(t, 3, len(events))

	assert.Equal(t, 0, len(store.PendingCalls(client2)))
	assert.Equal(t, []string{"node02"}, getKeys(sharding2.obs.nodes))
}

func TestSharding_Observer_List_Conn_Error(t *testing.T) {
	store := initStore()

	var events []ChangeEvent
	startSharding(store, client1, "node01", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	store.Begin(client1)

	initContainerNodes(store, client1)

	store.ConnError(client1)
	store.Retry(client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.GetApply(client1)
	store.ChildrenApply(client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)

	store.ChildrenApply(client1)

	assert.Equal(t, 0, len(events))
	store.GetApply(client1)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
			MZxid:   107,
		},
	}, events[0].New)
}

func TestSharding_Observer_Get_Node_Data_Error(t *testing.T) {
	store := initStore()

	var events []ChangeEvent
	startSharding(store, client1, "node01", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	store.Begin(client1)

	initContainerNodes(store, client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.ConnError(client1)
	store.Retry(client1)

	store.ChildrenApply(client1)
	store.GetApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	// Get Assigns Node01 Error
	store.ConnError(client1)
	store.Retry(client1)

	assert.Equal(t, 0, len(events))
	store.GetApply(client1)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
			MZxid:   107,
		},
	}, events[0].New)
}

func TestSharding_Observer_3_Nodes__One_Watch_Partial_Results(t *testing.T) {
	store := initStore()

	var events []ChangeEvent

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	sharding3 := startSharding(store, client3, "node03", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)

	store.ChildrenApply(client3)
	store.ChildrenApply(client3)
	store.ChildrenApply(client3)
	store.CreateApply(client3)

	store.GetApply(client3)
	store.GetApply(client3)
	store.GetApply(client3)
	store.ChildrenApply(client3)
	store.GetApply(client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	store.ChildrenApply(client3)
	store.GetApply(client3)
	store.GetApply(client3)

	assert.Equal(t, 0, len(events))
	store.GetApply(client3)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2},
			MZxid:   111,
		},
		{
			ID:      "node02",
			Address: "node02-addr:4001",
			Shards:  []ShardID{3, 4, 5},
			MZxid:   112,
		},
		{
			ID:      "node03",
			Address: "node03-addr:4001",
			Shards:  []ShardID{6, 7},
			MZxid:   113,
		},
	}, events[0].New)

	// ====================
	// Node 2 Expired
	// ====================
	store.SessionExpired(client2)

	store.ChildrenApply(client3)
	store.ChildrenApply(client3)
	store.GetApply(client3)

	store.ChildrenApply(client1)

	store.SetApply(client1)
	store.GetApply(client3)

	store.SetApply(client1)

	assert.Equal(t, 1, len(events))
	store.GetApply(client3)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3},
			MZxid:   115,
		},
		{
			ID:      "node03",
			Address: "node03-addr:4001",
			Shards:  []ShardID{6, 7, 4, 5},
			MZxid:   116,
		},
	}, events[1].New)

	store.DeleteApply(client1)
	store.ChildrenApply(client3)
	assert.Equal(t, 2, len(events))

	assert.Equal(t, []string{"node01", "node03"}, getKeys(sharding3.obs.nodes))
}

func TestSharding_Observer_2_Nodes_One_Node_Expired_When_Getting_Data(t *testing.T) {
	store := initStore()

	var events []ChangeEvent

	startSharding(store, client1, "node01", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))
	startSharding(store, client2, "node02")

	store.Begin(client1)
	store.Begin(client2)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)
	store.GetApply(client1)

	// Node 2 Expired
	store.SessionExpired(client2)

	store.GetApply(client1)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.ChildrenApply(client1)

	store.CreateApply(client1)

	store.ChildrenApply(client1) // assigns children
	assert.Equal(t, 0, len(events))
	store.GetApply(client1)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3, 4, 5, 6, 7},
			MZxid:   109,
		},
	}, events[0].New)
}

func TestSharding_Observer_3_Nodes_One_Node_Expired_When_Getting_Data_With_Watch(t *testing.T) {
	store := initStore()

	var events []ChangeEvent

	startSharding(store, client1, "node01")
	startSharding(store, client2, "node02")
	startSharding(store, client3, "node03", WithShardingObserver(func(event ChangeEvent) {
		events = append(events, event)
	}))

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(client3)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)
	initContainerNodes(store, client3)

	lockGranted(store, client1)
	lockBlocked(store, client2)

	store.ChildrenApply(client3)
	store.ChildrenApply(client3)
	store.ChildrenApply(client3)
	store.CreateApply(client3)
	store.GetApply(client3)
	store.GetApply(client3)
	store.GetApply(client3)
	store.ChildrenApply(client3)
	store.GetApply(client3)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	store.ChildrenApply(client3)
	store.GetApply(client3)

	// Node 2 Expired
	store.SessionExpired(client2)

	store.ChildrenApply(client1)
	store.SetApply(client1)
	store.SetApply(client1)
	store.DeleteApply(client1)

	// Client 3 Run Again
	store.GetApply(client3)
	store.GetApply(client3)

	store.ChildrenApply(client3)
	store.ChildrenApply(client3)

	assert.Equal(t, 0, len(events))
	store.GetApply(client3) // get assigns node01
	assert.Equal(t, 1, len(events))
	assert.Equal(t, []Node{
		{
			ID:      "node01",
			Address: "node01-addr:4001",
			Shards:  []ShardID{0, 1, 2, 3},
			MZxid:   115,
		},
		{
			ID:      "node03",
			Address: "node03-addr:4001",
			Shards:  []ShardID{6, 7, 4, 5},
			MZxid:   116,
		},
	}, events[0].New)

	store.ChildrenApply(client3)
	store.GetApply(client3)

	assert.Equal(t, 1, len(events))
	assert.Equal(t, 0, len(store.PendingCalls(client3)))
}

func TestSharding_Without_Observers__Using_Tester(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(store, []curator.FakeClientID{client1, client2, client3}, 123)

	tester.Begin()

	steps := tester.RunSessionExpiredAndConnectionError(
		20,
		20,
		1000,
	)
	assert.Equal(t, 1000, steps)

	steps = tester.RunSessionExpiredAndConnectionError(
		0,
		0,
		1000_000,
	)
	assert.Equal(t, 36, steps)

	store.PrintData()
	store.PrintPendingCalls()
}

func TestSharding_Without_Observers__Using_Tester__With_Smaller_Probability(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		123,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 10, 10_000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__Many_Times(t *testing.T) {
	for k := 0; k < 5000; k++ {
		store := initStore()

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 10, 1000)
		runTesterWithoutErrors(tester)

		assert.Equal(t, 0, len(store.PendingCalls(client1)))
		assert.Equal(t, 0, len(store.PendingCalls(client2)))
		assert.Equal(t, 0, len(store.PendingCalls(client3)))

		checkFinalShards(t, store)
	}
}

func TestSharding_Without_Observers__Using_Tester__Many_Times__Lower_Prob(t *testing.T) {
	for k := 0; k < 1000; k++ {
		store := initStore()

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 5, 1000)
		runTesterWithoutErrors(tester)

		assert.Equal(t, 0, len(store.PendingCalls(client1)))
		assert.Equal(t, 0, len(store.PendingCalls(client2)))
		assert.Equal(t, 0, len(store.PendingCalls(client3)))

		checkFinalShards(t, store)
	}
}

func TestSharding_Without_Observers__Using_Tester__Many_Times__With_4_Nodes__Lower_Prob(t *testing.T) {
	for k := 0; k < 1000; k++ {
		store := initStore()

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03", WithLogger(&noopLogger{}))
		startSharding(store, client4, "node04", WithLogger(&noopLogger{}))

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3, client4},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 3, 1000)
		runTesterWithoutErrors(tester)

		assert.Equal(t, 0, len(store.PendingCalls(client1)))
		assert.Equal(t, 0, len(store.PendingCalls(client2)))
		assert.Equal(t, 0, len(store.PendingCalls(client3)))
		assert.Equal(t, 0, len(store.PendingCalls(client4)))

		checkFinalShards(t, store)
	}
}

func TestSharding_Without_Observers__Using_Tester__Many_Times__Lower_Prob__With_Ops_Error(t *testing.T) {
	for k := 0; k < 2000; k++ {
		store := initStore()

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 5, 1000, curator.WithRunOperationErrorPercentage(8))
		runTesterWithoutErrors(tester)

		assert.Equal(t, 0, len(store.PendingCalls(client1)))
		assert.Equal(t, 0, len(store.PendingCalls(client2)))
		assert.Equal(t, 0, len(store.PendingCalls(client3)))

		checkFinalShards(t, store)
	}
}

func TestSharding_Without_Observers__Using_Tester__With_Error(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712306020784456407,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 10, 1000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__With_Error_2(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712311500875229288,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 1000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__With_Error_3(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712312244036663379,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 10, 1000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__With_Error_4(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712314456540876303,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 10, 1000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__With_Error_5(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712314932609040999,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 10, 1000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__Many_Times__Lower_Prob__With_Ops_Error__Case1(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712630125872893950,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 1000, curator.WithRunOperationErrorPercentage(8))
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	assert.Equal(t, 0, len(store.PendingCalls(client1)))
	assert.Equal(t, 0, len(store.PendingCalls(client2)))
	assert.Equal(t, 0, len(store.PendingCalls(client3)))

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__Many_Times__Lower_Prob__With_Ops_Error__Case2(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712631093229480643,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 1000, curator.WithRunOperationErrorPercentage(8))
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	assert.Equal(t, 0, len(store.PendingCalls(client1)))
	assert.Equal(t, 0, len(store.PendingCalls(client2)))
	assert.Equal(t, 0, len(store.PendingCalls(client3)))

	checkFinalShards(t, store)
}

func TestSharding_Without_Observers__Using_Tester__Many_Times__Lower_Prob__With_Ops_Error__Case3(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03", WithLogger(&noopLogger{}))

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712631353051612016,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 1000, curator.WithRunOperationErrorPercentage(8))
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	assert.Equal(t, 0, len(store.PendingCalls(client1)))
	assert.Equal(t, 0, len(store.PendingCalls(client2)))
	assert.Equal(t, 0, len(store.PendingCalls(client3)))

	checkFinalShards(t, store)
}

func checkFinalShards(t *testing.T, store *curator.FakeZookeeper) {
	assign := store.Root.Children[0].Children[2]
	assert.Equal(t, "assigns", assign.Name)

	var nodes []string
	var shards []ShardID
	for _, child := range assign.Children {
		nodes = append(nodes, child.Name)

		var d assignData
		err := json.Unmarshal(child.Data, &d)
		if err != nil {
			panic(err)
		}
		shards = append(shards, d.Shards...)
	}

	assert.Less(t, len(nodes), 5)

	slices.Sort(shards)
	assert.Equal(t, []ShardID{0, 1, 2, 3, 4, 5, 6, 7}, shards)
}

func runTesterWithExactSteps(
	tester *curator.FakeZookeeperTester,
	prob float64, exactSteps int,
	options ...curator.RunOption,
) {
	remainSteps := exactSteps
	for remainSteps > 0 {
		steps := tester.RunSessionExpiredAndConnectionError(
			prob,
			prob,
			remainSteps,
			options...,
		)
		remainSteps -= steps
	}
}

func runTesterWithoutErrors(tester *curator.FakeZookeeperTester) {
	steps := tester.RunSessionExpiredAndConnectionError(
		0, 0,
		100_000,
	)
	if steps == 100_000 {
		fmt.Println("============================================")
		fmt.Println("[ERROR] SHOULD NOT RUN INDEFINITELY")
		fmt.Println("============================================")
		panic("should not run indefinitely")
	}
}

type noopLogger struct {
}

func (*noopLogger) Infof(format string, args ...any) {
}

func (*noopLogger) Warnf(format string, args ...any) {
}

func (*noopLogger) Errorf(format string, args ...any) {
}
