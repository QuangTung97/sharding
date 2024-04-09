package sharding

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/QuangTung97/zk/curator"
	"github.com/stretchr/testify/assert"
)

func TestSharding_With_Observer(t *testing.T) {
	store := initStore()

	var lastEvent ChangeEvent

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03",
		WithLogger(&noopLogger{}),
		WithShardingObserver(func(event ChangeEvent) {
			lastEvent = event
		}),
	)

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
	checkObserverShards(t, store, lastEvent)
}

func TestSharding_With_Observer_With_Lower_Probability(t *testing.T) {
	store := initStore()

	var lastEvent ChangeEvent

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03",
		WithLogger(&noopLogger{}),
		WithShardingObserver(func(event ChangeEvent) {
			lastEvent = event
		}),
	)

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		123,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 10_000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
	checkObserverShards(t, store, lastEvent)
}

func TestSharding_With_Observer_With_Lower_Probability__Multi_Times(t *testing.T) {
	for k := 0; k < 1000; k++ {
		store := initStore()

		var lastEvent ChangeEvent

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03",
			WithLogger(&noopLogger{}),
			WithShardingObserver(func(event ChangeEvent) {
				lastEvent = event
			}),
		)

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 5, 10_000)
		runTesterWithoutErrors(tester)

		checkFinalShards(t, store)
		checkObserverShards(t, store, lastEvent)
	}
}

func TestSharding_With_Observer_With_Lower_Probability__Error_1(t *testing.T) {
	store := initStore()

	var lastEvent ChangeEvent

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03",
		WithLogger(&noopLogger{}),
		WithShardingObserver(func(event ChangeEvent) {
			lastEvent = event
		}),
	)

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712542744725656523,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 10_000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
	checkObserverShards(t, store, lastEvent)
}

func TestSharding_With_Observer_With_Lower_Probability__Error_2(t *testing.T) {
	store := initStore()

	var lastEvent ChangeEvent

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
	startSharding(store, client3, "node03",
		WithLogger(&noopLogger{}),
		WithShardingObserver(func(event ChangeEvent) {
			lastEvent = event
		}),
	)

	tester := curator.NewFakeZookeeperTester(
		store, []curator.FakeClientID{client1, client2, client3},
		1712547069689410219,
	)

	tester.Begin()
	runTesterWithExactSteps(tester, 5, 10_000)
	runTesterWithoutErrors(tester)

	store.PrintData()
	store.PrintPendingCalls()

	checkFinalShards(t, store)
	checkObserverShards(t, store, lastEvent)
}

func TestSharding_With_Observer_With_Very_Low_Probability__Multi_Times(t *testing.T) {
	for k := 0; k < 500; k++ {
		store := initStore()

		var lastEvent ChangeEvent

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03",
			WithLogger(&noopLogger{}),
			WithShardingObserver(func(event ChangeEvent) {
				lastEvent = event
			}),
		)

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 3, 10_000)
		runTesterWithoutErrors(tester)

		checkFinalShards(t, store)
		checkObserverShards(t, store, lastEvent)
	}
}

func TestSharding_With_Observer_With_High_Probability__4_Nodes__Multi_Times(t *testing.T) {
	for k := 0; k < 1000; k++ {
		store := initStore()

		var lastEvent ChangeEvent

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03",
			WithLogger(&noopLogger{}),
			WithShardingObserver(func(event ChangeEvent) {
				lastEvent = event
			}),
		)
		startSharding(store, client4, "node04", WithLogger(&noopLogger{}))

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3, client4},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 16, 2000)
		runTesterWithoutErrors(tester)

		checkFinalShards(t, store)
		checkObserverShards(t, store, lastEvent)
	}
}

func TestSharding_With_Observer_With_Low_Low_Probability__Multi_Times__With_Ops_Error(t *testing.T) {
	for k := 0; k < 500; k++ {
		store := initStore()

		var lastEvent ChangeEvent

		startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
		startSharding(store, client2, "node02", WithLogger(&noopLogger{}))
		startSharding(store, client3, "node03",
			WithLogger(&noopLogger{}),
			WithShardingObserver(func(event ChangeEvent) {
				lastEvent = event
			}),
		)

		seed := time.Now().UnixNano()
		fmt.Println("SEED:", seed)

		tester := curator.NewFakeZookeeperTester(
			store, []curator.FakeClientID{client1, client2, client3},
			seed,
		)

		tester.Begin()
		runTesterWithExactSteps(tester, 10, 10_000, curator.WithRunOperationErrorPercentage(10))
		runTesterWithoutErrors(tester)

		checkFinalShards(t, store)
		checkObserverShards(t, store, lastEvent)
	}
}

func checkObserverShards(t *testing.T, store *curator.FakeZookeeper, lastEvent ChangeEvent) {
	var shards []ShardID
	eventAlloc := map[string][]ShardID{}
	for _, node := range lastEvent.New {
		shards = append(shards, node.Shards...)
		eventAlloc[node.ID] = node.Shards
	}
	slices.Sort(shards)
	assert.Equal(t, []ShardID{0, 1, 2, 3, 4, 5, 6, 7}, shards)

	storeAlloc := map[string][]ShardID{}
	assign := store.Root.Children[0].Children[2]
	assert.Equal(t, "assigns", assign.Name)

	for _, child := range assign.Children {
		var d assignData
		err := json.Unmarshal(child.Data, &d)
		if err != nil {
			panic(err)
		}
		storeAlloc[child.Name] = d.Shards
	}

	assert.Equal(t, storeAlloc, eventAlloc)
}

func TestStandaloneObserver_2_Nodes(t *testing.T) {
	store := initStore()
	var events []ChangeEvent

	startSharding(store, client1, "node01", WithLogger(&noopLogger{}))
	startSharding(store, client2, "node02", WithLogger(&noopLogger{}))

	factory := curator.NewFakeClientFactory(store, observer1)
	observer := NewObserver(parentPath, numShards, func(event ChangeEvent) {
		events = append(events, event)
	})
	factory.Start(observer.GetCurator())

	store.Begin(client1)
	store.Begin(client2)
	store.Begin(observer1)

	initContainerNodes(store, client1)
	initContainerNodes(store, client2)

	store.CreateApply(observer1) // create lock
	store.CreateApply(observer1) // create nodes
	store.CreateApply(observer1) // create assigns

	lockGranted(store, client1)
	lockBlocked(store, client2)

	store.ChildrenApply(client1)
	store.ChildrenApply(client1)
	store.CreateApply(client1)
	store.CreateApply(client1)

	store.ChildrenApply(observer1)
	store.ChildrenApply(observer1)

	store.GetApply(observer1)
	store.GetApply(observer1)
	store.GetApply(observer1)

	assert.Equal(t, 0, len(events))
	store.GetApply(observer1)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, ChangeEvent{
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
	}, events[0])

	store.PrintData()
	store.PrintPendingCalls()
}
