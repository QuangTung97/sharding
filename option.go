package sharding

import (
	"github.com/QuangTung97/zk"
)

type Option func(s *Sharding)

func WithShardingObserver(fn ObserverFunc) Option {
	return func(s *Sharding) {
		s.obs = newObserverCore(s.parentPath, s.numShards, fn)
	}
}

func WithLogger(l zk.Logger) Option {
	return func(s *Sharding) {
		s.logger = l
	}
}
