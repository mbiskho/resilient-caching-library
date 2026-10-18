package cache

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCacheGet(b *testing.B) {
	cache := New(Options{
		TTL:     time.Minute,
		Loader: func(ctx context.Context, key string) (any, error) { return key, nil },
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cache.Get(context.Background(), "benchmark-key")
		if err != nil {
			b.Fatal(err)
		}
	}
}
