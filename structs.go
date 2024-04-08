package sharding

import (
	"encoding/json"
)

const (
	lockZNodeName   = "/locks"
	nodeZNodeName   = "/nodes"
	assignZNodeName = "/assigns"
)

// ShardID for shard if from zero
type ShardID uint32

type nodeData struct {
	Address string `json:"address"`
}

func (d nodeData) marshalJSON() []byte {
	data, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return data
}

type assignData struct {
	Shards []ShardID `json:"shards"`
}
