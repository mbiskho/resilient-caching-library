package cache

import (
	"sync"
	"time"
)

// CircuitState tracks whether the breaker is closed, open or half-open.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half-open"
)

// CircuitBreaker guards a resource from unhealthy repeated calls.
type CircuitBreaker struct {
	mu                 sync.Mutex
	state              CircuitState
	consecutiveFailure int
	openedAt           time.Time
	config             CircuitBreakerConfig
	callsInHalfOpen    int
}

func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 30 * time.Second
	}
	if config.MaxHalfOpen <= 0 {
		config.MaxHalfOpen = 2
	}
	return &CircuitBreaker{state: CircuitClosed, config: config}
}

func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == CircuitOpen {
		if time.Since(b.openedAt) >= b.config.ResetTimeout {
			b.state = CircuitHalfOpen
			b.callsInHalfOpen = 0
			return true
		}
		return false
	}
	if b.state == CircuitHalfOpen {
		if b.callsInHalfOpen >= b.config.MaxHalfOpen {
			return false
		}
		b.callsInHalfOpen++
		return true
	}
	return true
}

func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFailure = 0
	b.state = CircuitClosed
	b.callsInHalfOpen = 0
}

func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFailure++
	if b.consecutiveFailure >= b.config.FailureThreshold {
		b.state = CircuitOpen
		b.openedAt = time.Now()
		b.callsInHalfOpen = 0
	}
}
