package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheStoresAndReadsValue(t *testing.T) {
	loaderCalls := int32(0)
	cache := New(Options{
		TTL: time.Minute,
		Loader: func(ctx context.Context, key string) (any, error) {
			atomic.AddInt32(&loaderCalls, 1)
			return "value-for-" + key, nil
		},
	})

	value, err := cache.Get(context.Background(), "alpha")
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}
	if value != "value-for-alpha" {
		t.Fatalf("Get returned %v, want %q", value, "value-for-alpha")
	}
	if stats := cache.Stats(); stats.Loads != 1 {
		t.Fatalf("expected 1 load, got %d", stats.Loads)
	}
}

func TestCacheSingleFlightCollapsesConcurrentLoads(t *testing.T) {
	loaderCalls := int32(0)
	cache := New(Options{
		TTL: time.Minute,
		Loader: func(ctx context.Context, key string) (any, error) {
			atomic.AddInt32(&loaderCalls, 1)
			time.Sleep(50 * time.Millisecond)
			return "single-flight", nil
		},
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := cache.Get(context.Background(), "shared")
			if err != nil {
				t.Errorf("Get returned error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&loaderCalls); got != 1 {
		t.Fatalf("expected a single backend call, got %d", got)
	}
}

func TestNegativeCacheUsesConfiguredTTL(t *testing.T) {
	loaderCalls := int32(0)
	sentinel := errors.New("backend unavailable")
	cache := New(Options{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		Loader: func(ctx context.Context, key string) (any, error) {
			atomic.AddInt32(&loaderCalls, 1)
			return nil, sentinel
		},
	})

	if _, err := cache.Get(context.Background(), "missing"); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if _, err := cache.Get(context.Background(), "missing"); !errors.Is(err, sentinel) {
		t.Fatalf("second Get should return negative cache result, got %v", err)
	}
	if got := atomic.LoadInt32(&loaderCalls); got != 1 {
		t.Fatalf("expected one load from the backend, got %d", got)
	}
}
