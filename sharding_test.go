package sharding

import (
	"testing"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
	"github.com/stretchr/testify/assert"
)

const client1 curator.FakeClientID = "client1"
const client2 curator.FakeClientID = "client2"
const initClient curator.FakeClientID = "init"

const parentPath = "/sharding"

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
	s := New(parentPath, nodeID)
	factory.Start(s.GetCurator())
}

func TestSharding_Begin(t *testing.T) {
	store := initStore()

	startSharding(store, client1, "node01")

	store.Begin(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	// Lock Start
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.ChildrenApply(client1)

	assert.Equal(t, []string{
		"children",
	}, store.PendingCalls(client1))
}
