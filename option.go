package sharding

type Option func(s *Sharding)

func WithShardingObserver(fn ObserverFunc) Option {
	return func(s *Sharding) {
		s.obs = newObserverCore(s.parentPath, fn)
	}
}
