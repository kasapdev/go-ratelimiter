package ratelimiter

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

var (
	// ErrCostExceedsCapacity is returned by Wait when the requested cost is
	// larger than the bucket's capacity, so no amount of waiting could ever
	// satisfy it.
	ErrCostExceedsCapacity = errors.New("ratelimiter: cost exceeds bucket capacity")

	// ErrNoRefill is returned by Wait when the bucket does not currently hold
	// enough tokens and its refill rate is zero, so it never will.
	ErrNoRefill = errors.New("ratelimiter: bucket has a zero refill rate and too few tokens")
)

// TokenBucket is a thread-safe token-bucket rate limiter. It holds up to
// capacity tokens and refills at refillRate tokens per second. Each call to
// Allow consumes tokens if enough are available.
//
// Refilling is computed lazily from elapsed wall-clock time inside Allow —
// there is no background goroutine or ticker involved, so an idle
// TokenBucket consumes no resources between calls.
type TokenBucket struct {
	mu sync.Mutex

	capacity   float64 // maximum number of tokens the bucket can hold
	tokens     float64 // current number of tokens available
	refillRate float64 // tokens added per second
	lastRefill time.Time
}

// NewTokenBucket creates a new TokenBucket with the given capacity (maximum
// burst size) and refillRate (tokens added per second). The bucket starts
// full, i.e. with capacity tokens available.
//
// If capacity is <= 0 it is clamped to 1, since a bucket that can never hold
// a token would make Allow always return false. If refillRate is negative it
// is clamped to 0 (a bucket that never refills).
func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	if capacity <= 0 {
		capacity = 1
	}
	if refillRate < 0 {
		refillRate = 0
	}
	return &TokenBucket{
		capacity:   float64(capacity),
		tokens:     float64(capacity),
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Allow reports whether cost tokens can be consumed from the bucket right
// now. If enough tokens are available they are deducted and Allow returns
// true; otherwise no tokens are deducted and Allow returns false.
//
// Before checking, Allow refills the bucket based on the wall-clock time
// elapsed since the previous call, at refillRate tokens per second, capped
// at the bucket's capacity.
//
// A non-positive cost is treated as a no-op that is always allowed (no
// tokens are consumed).
//
// Allow is safe for concurrent use by multiple goroutines.
func (tb *TokenBucket) Allow(cost int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refillLocked(time.Now())

	if cost <= 0 {
		return true
	}

	c := float64(cost)
	if c > tb.tokens {
		return false
	}
	tb.tokens -= c
	return true
}

// refillLocked adds tokens for the time elapsed since the last refill. The
// caller must hold tb.mu.
func (tb *TokenBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	tb.lastRefill = now
	if tb.refillRate == 0 {
		return
	}
	tb.tokens = min(tb.capacity, tb.tokens+elapsed*tb.refillRate)
}

// Wait blocks until cost tokens are available, then consumes them and returns
// nil. It is the blocking counterpart of Allow, meant for callers that would
// rather be paced than rejected.
//
// Wait returns early, consuming nothing, with:
//   - ctx.Err() if ctx is canceled or its deadline passes while waiting;
//   - ErrCostExceedsCapacity if cost is larger than the bucket's capacity;
//   - ErrNoRefill if tokens are short and the refill rate is zero.
//
// The last two are reported immediately rather than blocking until ctx ends,
// because waiting could never succeed. A non-positive cost returns nil
// straight away, like Allow. Waiters are not served in strict FIFO order.
//
// Wait is safe for concurrent use by multiple goroutines.
func (tb *TokenBucket) Wait(ctx context.Context, cost int) error {
	if cost <= 0 {
		return nil
	}
	c := float64(cost)

	for {
		tb.mu.Lock()
		if c > tb.capacity {
			tb.mu.Unlock()
			return ErrCostExceedsCapacity
		}
		tb.refillLocked(time.Now())
		if c <= tb.tokens {
			tb.tokens -= c
			tb.mu.Unlock()
			return nil
		}
		if tb.refillRate == 0 {
			tb.mu.Unlock()
			return ErrNoRefill
		}
		missing := c - tb.tokens
		wait := time.Duration(math.Ceil(missing / tb.refillRate * float64(time.Second)))
		tb.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			// Re-check under the lock: another goroutine may have taken the tokens.
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}
