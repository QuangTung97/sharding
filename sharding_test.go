package sharding

import (
	"testing"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
	"github.com/stretchr/testify/assert"
)

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
) {
	factory := curator.NewFakeClientFactory(store, client)
	s := New(parentPath, nodeID, numShards)
	factory.Start(s.GetCurator())
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
	store.CreateApply(client1)

	store.PrintData()
	store.PrintPendingCalls()

	lockNode := store.Root.Children[0]
	assert.Equal(t, "assigns", lockNode.Children[2].Name)
	children := lockNode.Children[2].Children
	assert.Equal(t, 1, len(children))
	assert.Equal(t, "node01", children[0].Name)
	assert.Equal(t, `{"shards":[0,1,2,3,4,5,6,7]}`, string(children[0].Data))
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
	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	store.PrintData()
	store.PrintPendingCalls()

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
