package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

var ErrCircuitOpen = errors.New("cache circuit breaker is open")

// LoaderFunc loads a value for a cache key.
type LoaderFunc func(context.Context, string) (any, error)

// Policy describes how entries are kept in memory.
type Policy string

const (
	PolicyLRU  Policy = "lru"
	PolicyLFU  Policy = "lfu"
	PolicyFIFO Policy = "fifo"
)

// Options control the cache behaviour.
type Options struct {
	TTL              time.Duration
	NegativeTTL      time.Duration
	EarlyExpiryRatio float64
	MaxEntries       int
	Policy           Policy
	Loader           LoaderFunc
	Circuit          CircuitBreakerConfig
}

// CircuitBreakerConfig configures a minimal circuit breaker.
type CircuitBreakerConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	MaxHalfOpen      int
}

// Stats summarizes runtime cache performance.
type Stats struct {
	Hits                int64
	Misses              int64
	Loads               int64
	NegativeCaches      int64
	EarlyRefreshes      int64
	CircuitOpenEvents   int64
	CircuitClosedEvents int64
	Evictions           int64
}

// Cache is a resilient in-memory cache with single-flight protection,
// negative caching and probabilistic early refresh.
type Cache struct {
	mu                sync.RWMutex
	entries           map[string]*entry
	policy            *evictionPolicy
	maxEntries        int
	ttl               time.Duration
	negativeTTL       time.Duration
	earlyExpiryRatio  float64
	loader            LoaderFunc
	breaker           *CircuitBreaker
	group             singleflight.Group
	stats             Stats
	negativeFn        func() error
}

type entry struct {
	value      any
	expiresAt  time.Time
	negative   bool
	loadErr    error
	accesses   int64
	lastAccess time.Time
}

func DefaultOptions() Options {
	return Options{
		TTL:              5 * time.Minute,
		NegativeTTL:      30 * time.Second,
		EarlyExpiryRatio: 0.25,
		MaxEntries:       1000,
		Policy:           PolicyLRU,
		Circuit: CircuitBreakerConfig{
			FailureThreshold: 5,
			ResetTimeout:     30 * time.Second,
			MaxHalfOpen:      2,
		},
	}
}

func New(options Options) *Cache {
	cfg := DefaultOptions()
	if options.TTL > 0 {
		cfg.TTL = options.TTL
	}
	if options.NegativeTTL > 0 {
		cfg.NegativeTTL = options.NegativeTTL
	}
	if options.EarlyExpiryRatio > 0 {
		cfg.EarlyExpiryRatio = options.EarlyExpiryRatio
	}
	if options.MaxEntries > 0 {
		cfg.MaxEntries = options.MaxEntries
	}
	if options.Policy != "" {
		cfg.Policy = options.Policy
	}
	if options.Loader != nil {
		cfg.Loader = options.Loader
	}
	if options.Circuit.FailureThreshold > 0 {
		cfg.Circuit.FailureThreshold = options.Circuit.FailureThreshold
	}
	if options.Circuit.ResetTimeout > 0 {
		cfg.Circuit.ResetTimeout = options.Circuit.ResetTimeout
	}
	if options.Circuit.MaxHalfOpen > 0 {
		cfg.Circuit.MaxHalfOpen = options.Circuit.MaxHalfOpen
	}

	c := &Cache{
		entries:          make(map[string]*entry),
		maxEntries:       cfg.MaxEntries,
		ttl:              cfg.TTL,
		negativeTTL:      cfg.NegativeTTL,
		earlyExpiryRatio: cfg.EarlyExpiryRatio,
		loader:           cfg.Loader,
		breaker:          NewCircuitBreaker(cfg.Circuit),
	}
	c.policy = newEvictionPolicy(cfg.Policy, c)
	return c
}

func (c *Cache) SetLoader(loader LoaderFunc) {
	c.loader = loader
}

func (c *Cache) Get(ctx context.Context, key string) (any, error) {
	if c == nil {
		return nil, errors.New("nil cache")
	}
	if c.loader == nil {
		return nil, errors.New("cache loader is not configured")
	}

	if val, found, err := c.getCachedValue(key); found {
		if err != nil {
			return nil, err
		}
		return val, nil
	}

	if !c.breaker.Allow() {
		atomic.AddInt64(&c.stats.CircuitOpenEvents, 1)
		return nil, ErrCircuitOpen
	}

	result, err, _ := c.group.Do(key, func() (interface{}, error) {
		if val, found, err := c.getCachedValue(key); found {
			if err != nil {
				return nil, err
			}
			return val, nil
		}

		atomic.AddInt64(&c.stats.Loads, 1)
		value, err := c.loader(ctx, key)
		if err != nil {
			c.breaker.RecordFailure()
			if c.negativeTTL > 0 {
				c.setNegative(key, err)
			}
			return nil, err
		}

		c.breaker.RecordSuccess()
		c.setValue(key, value, c.ttl)
		return value, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Cache) setValue(key string, value any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := &entry{value: value, expiresAt: time.Now().Add(ttl), lastAccess: time.Now()}
	c.entries[key] = entry
	c.policy.onWrite(key, entry)
	c.trimToCapacityLocked()
}

func (c *Cache) setNegative(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := &entry{negative: true, loadErr: err, expiresAt: time.Now().Add(c.negativeTTL), lastAccess: time.Now()}
	c.entries[key] = entry
	c.policy.onWrite(key, entry)
	atomic.AddInt64(&c.stats.NegativeCaches, 1)
	c.trimToCapacityLocked()
}

func (c *Cache) getCachedValue(key string) (any, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.entries[key]
	if !ok {
		atomic.AddInt64(&c.stats.Misses, 1)
		return nil, false, nil
	}

	item.lastAccess = time.Now()
	atomic.AddInt64(&c.stats.Hits, 1)
	item.accesses++
	c.policy.onRead(key, item)

	if item.negative && time.Now().After(item.expiresAt) {
		delete(c.entries, key)
		return nil, false, nil
	}
	if item.expiresAt.Before(time.Now()) {
		delete(c.entries, key)
		return nil, false, nil
	}
	if item.negative {
		return nil, true, item.loadErr
	}
	if c.shouldRefreshEarly(item) {
		atomic.AddInt64(&c.stats.EarlyRefreshes, 1)
		delete(c.entries, key)
		return nil, false, nil
	}
	return item.value, true, nil
}

func (c *Cache) shouldRefreshEarly(item *entry) bool {
	if c.earlyExpiryRatio <= 0 || c.ttl <= 0 {
		return false
	}
	remaining := time.Until(item.expiresAt)
	if remaining <= 0 {
		return false
	}
	threshold := time.Duration(float64(c.ttl) * c.earlyExpiryRatio)
	if remaining > threshold {
		return false
	}
	return rand.Float64() < 0.5
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	c.policy.onDelete(key)
}

func (c *Cache) Set(key string, value any, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.ttl
	}
	c.setValue(key, value, ttl)
}

func (c *Cache) Stats() Stats {
	stats := c.stats
	return stats
}

func (c *Cache) trimToCapacityLocked() {
	if c.maxEntries <= 0 || len(c.entries) <= c.maxEntries {
		return
	}
	for len(c.entries) > c.maxEntries {
		keyToDrop := c.policy.pickVictim()
		if keyToDrop == "" {
			for candidate := range c.entries {
				keyToDrop = candidate
				break
			}
		}
		if keyToDrop == "" {
			return
		}
		delete(c.entries, keyToDrop)
		atomic.AddInt64(&c.stats.Evictions, 1)
	}
}

func (c *Cache) WithOptions(options Options) *Cache {
	if options.TTL > 0 {
		c.ttl = options.TTL
	}
	if options.NegativeTTL > 0 {
		c.negativeTTL = options.NegativeTTL
	}
	if options.EarlyExpiryRatio > 0 {
		c.earlyExpiryRatio = options.EarlyExpiryRatio
	}
	if options.MaxEntries > 0 {
		c.maxEntries = options.MaxEntries
	}
	if options.Policy != "" {
		c.policy = newEvictionPolicy(options.Policy, c)
	}
	return c
}

func (c *Cache) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return fmt.Sprintf("cache{entries=%d, ttl=%s, negativeTTL=%s, capacity=%d}", len(c.entries), c.ttl, c.negativeTTL, c.maxEntries)
}

// TieredCache provides a two-level cache and keeps the hot path fast.
type TieredCache struct {
	primary   *Cache
	secondary *Cache
}

func NewTiered(primary, secondary *Cache) *TieredCache {
	return &TieredCache{primary: primary, secondary: secondary}
}

func (t *TieredCache) Get(ctx context.Context, key string) (any, error) {
	if t.primary != nil {
		if value, err := t.primary.getCachedValueForTier(key); err == nil {
			return value, nil
		}
	}
	if t.secondary != nil {
		if value, err := t.secondary.getCachedValueForTier(key); err == nil {
			return value, nil
		}
	}
	if t.primary != nil {
		return t.primary.Get(ctx, key)
	}
	if t.secondary != nil {
		return t.secondary.Get(ctx, key)
	}
	return nil, errors.New("no cache tiers configured")
}

func (c *Cache) getCachedValueForTier(key string) (any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	item, ok := c.entries[key]
	if !ok {
		return nil, errors.New("missing")
	}
	if item.expiresAt.Before(time.Now()) {
		return nil, errors.New("expired")
	}
	if item.negative {
		return nil, item.loadErr
	}
	return item.value, nil
}
