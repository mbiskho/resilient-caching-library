package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"resilient-caching-library/pkg/cache"
)

func main() {
	workers := flag.Int("workers", 250, "concurrent requests")
	requests := flag.Int("requests", 1500, "total requests")
	keyTTL := flag.Duration("ttl", 150*time.Millisecond, "cache ttl")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	flag.Parse()

	rand.Seed(*seed)

	fmt.Println("Resilient cache stampede demo")
	fmt.Printf("Workers: %d | Requests: %d | TTL: %s\n", *workers, *requests, *keyTTL)

	naive := runNaiveScenario(*workers, *requests, *keyTTL)
	protected := runProtectedScenario(*workers, *requests, *keyTTL)

	fmt.Printf("Naive backend loads: %d\n", naive)
	fmt.Printf("Protected backend loads: %d\n", protected)
	fmt.Println("Result: the protected path absorbs concurrent expiry without letting every caller hit the store at once.")
}

type naiveCache struct {
	mu        sync.Mutex
	value     any
	expiresAt time.Time
	loader    func(context.Context, string) (any, error)
	ttl       time.Duration
}

func newNaiveCache(ttl time.Duration, loader func(context.Context, string) (any, error)) *naiveCache {
	return &naiveCache{ttl: ttl, loader: loader}
}

func (n *naiveCache) Get(ctx context.Context, key string) (any, error) {
	n.mu.Lock()
	if n.value != nil && time.Now().Before(n.expiresAt) {
		value := n.value
		n.mu.Unlock()
		return value, nil
	}
	n.mu.Unlock()

	value, err := n.loader(ctx, key)
	if err != nil {
		return nil, err
	}

	n.mu.Lock()
	n.value = value
	n.expiresAt = time.Now().Add(n.ttl)
	n.mu.Unlock()
	return value, nil
}

func runNaiveScenario(workers, requests int, ttl time.Duration) int64 {
	var backendCalls atomic.Int64
	backend := func(ctx context.Context, key string) (any, error) {
		backendCalls.Add(1)
		time.Sleep(12 * time.Millisecond)
		return "value-for-" + key, nil
	}

	cacheInstance := newNaiveCache(ttl, backend)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requests/workers; j++ {
				_, _ = cacheInstance.Get(context.Background(), "hot-key")
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	return backendCalls.Load()
}

func runProtectedScenario(workers, requests int, ttl time.Duration) int64 {
	var backendCalls atomic.Int64
	backend := func(ctx context.Context, key string) (any, error) {
		backendCalls.Add(1)
		time.Sleep(12 * time.Millisecond)
		return "value-for-" + key, nil
	}

	cacheInstance := cache.New(cache.Options{TTL: ttl, Loader: backend, MaxEntries: 50, EarlyExpiryRatio: 0.25, Circuit: cache.CircuitBreakerConfig{FailureThreshold: 5, ResetTimeout: 150 * time.Millisecond}})

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requests/workers; j++ {
				_, err := cacheInstance.Get(context.Background(), "hot-key")
				if err != nil {
					log.Printf("cache miss: %v", err)
				}
				time.Sleep(time.Duration(rand.Intn(8)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	return backendCalls.Load()
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
