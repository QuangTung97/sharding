package sharding

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

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
	assert.Equal(t, `{"shards":[0,1,2,6]}`, string(children[0].Data))

	assert.Equal(t, "node02", children[1].Name)
	assert.Equal(t, `{"shards":[3,4,5,7]}`, string(children[1].Data))
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

	store.ChildrenApply(client1)
	assert.Equal(t, []string{}, store.PendingCalls(client1))

	// Lock Granted for Client 2
	store.ChildrenApply(client2)

	store.ChildrenApply(client2)
	store.ChildrenApply(client2)

	store.GetApply(client2)
	store.GetApply(client2)
	store.GetApply(client2)

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
	store.ChildrenApply(client1)

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
	store.ChildrenApply(client1)

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
	store.ChildrenApply(client1)

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
	store.ChildrenApply(client1)

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
	store.GetApply(client1)

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

	store.GetApply(client1)

	store.SetApply(client1)
	store.CreateApply(client1)

	store.GetApply(client1)
	assert.Equal(t, 1, len(events))
	store.ChildrenApply(client1) // list children for assigns of observer

	assert.Equal(t, 1, len(events))
	store.GetApply(client1) // get assigns/node02
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

func getKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
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
