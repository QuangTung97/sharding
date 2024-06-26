package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/QuangTung97/zk/curator"

	"github.com/QuangTung97/sharding"
)

// WatchChange callback function
func WatchChange(event sharding.ChangeEvent) {
	fmt.Println("------------------------------------------")
	for _, node := range event.New {
		fmt.Printf("%+v\n", node)
	}
}

func observer() {
	obs := sharding.NewObserver("/sm", 8, WatchChange)

	factory := curator.NewClientFactory(
		[]string{"localhost"}, "user01", "password01",
	)
	defer factory.Close()

	factory.Start(obs.GetCurator())

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
}

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "observer" {
		observer()
		return
	}

	nodeID := sharding.NewNodeID()
	fmt.Println("NODE_ID:", nodeID)

	addr := fmt.Sprintf("addr-%s:4001", nodeID)

	s := sharding.New(
		"/sm", nodeID, 8, addr,
		sharding.WithShardingObserver(WatchChange),
	)

	factory := curator.NewClientFactory(
		[]string{"localhost"}, "user01", "password01",
	)
	defer factory.Close()

	factory.Start(s.GetCurator())

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
}
