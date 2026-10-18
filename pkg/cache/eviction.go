package cache

import "time"

// evictionPolicy decides which key to evict when the cache is full.
type evictionPolicy struct {
	kind   Policy
	cache  *Cache
	orders map[string]time.Time
}

func newEvictionPolicy(kind Policy, cache *Cache) *evictionPolicy {
	if kind == "" {
		kind = PolicyLRU
	}
	return &evictionPolicy{kind: kind, cache: cache, orders: map[string]time.Time{}}
}

func (p *evictionPolicy) onWrite(key string, item *entry) {
	p.orders[key] = time.Now()
	_ = item
}

func (p *evictionPolicy) onRead(key string, item *entry) {
	p.orders[key] = time.Now()
	_ = item
}

func (p *evictionPolicy) onDelete(key string) {
	delete(p.orders, key)
}

func (p *evictionPolicy) pickVictim() string {
	if len(p.orders) == 0 {
		return ""
	}
	var victim string
	var oldest time.Time
	for key, ts := range p.orders {
		switch p.kind {
		case PolicyFIFO:
			if victim == "" || ts.Before(oldest) {
				victim = key
				oldest = ts
			}
		default:
			if victim == "" || ts.Before(oldest) {
				victim = key
				oldest = ts
			}
		}
	}
	return victim
}
