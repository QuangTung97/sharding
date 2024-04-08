package sharding

import (
	"github.com/QuangTung97/zk"
)

// Option for sharding options
type Option func(s *Sharding)

// WithShardingObserver set observer function callback
func WithShardingObserver(fn ObserverFunc) Option {
	return func(s *Sharding) {
		s.obs = newObserverCore(s.parentPath, s.numShards, fn)
	}
}

// WithLogger changes logger
func WithLogger(l zk.Logger) Option {
	return func(s *Sharding) {
		s.logger = l
	}
}
