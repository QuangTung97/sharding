package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/QuangTung97/zk/curator"

	"github.com/QuangTung97/sharding"
)

func main() {
	nodeID := sharding.NewNodeID()
	fmt.Println("NODE_ID:", nodeID)
	s := sharding.New("/sm", nodeID, 16, "localhost:4001")

	factory := curator.NewClientFactory(
		[]string{"localhost"}, "user01", "password01",
	)
	defer factory.Close()

	factory.Start(s.GetCurator())

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	for i := 0; i < 6000; i++ {
		time.Sleep(1 * time.Second)
		select {
		case <-ch:
			return
		default:
		}
	}
}
