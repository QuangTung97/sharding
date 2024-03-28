package sharding

type ShardID uint32

type assignData struct {
	Shards []ShardID `json:"shards"`
}
