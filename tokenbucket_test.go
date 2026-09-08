package ratelimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucket_AllowUpToCapacity(t *testing.T) {
	tests := []struct {
		name       string
		capacity   int
		refillRate float64
		costs      []int
		wantAllow  []bool
	}{
		{
			name:       "allow up to capacity then deny",
			capacity:   3,
			refillRate: 0,
			costs:      []int{1, 1, 1, 1},
			wantAllow:  []bool{true, true, true, false},
		},
		{
			name:       "single request costing full capacity",
			capacity:   5,
			refillRate: 0,
			costs:      []int{5, 1},
			wantAllow:  []bool{true, false},
		},
		{
			name:       "cost larger than capacity always denied",
			capacity:   2,
			refillRate: 0,
			costs:      []int{3},
			wantAllow:  []bool{false},
		},
		{
			name:       "non-positive cost always allowed and free",
			capacity:   1,
			refillRate: 0,
			costs:      []int{1, 0, -1, 1},
			wantAllow:  []bool{true, true, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := NewTokenBucket(tt.capacity, tt.refillRate)
			for i, cost := range tt.costs {
				got := tb.Allow(cost)
				if got != tt.wantAllow[i] {
					t.Errorf("call %d: Allow(%d) = %v, want %v", i, cost, got, tt.wantAllow[i])
				}
			}
		})
	}
}

func TestTokenBucket_ConstructorClamping(t *testing.T) {
	tb := NewTokenBucket(0, -5)
	if !tb.Allow(1) {
		t.Errorf("expected clamped bucket (capacity>=1) to allow a single request")
	}
	if tb.Allow(1) {
		t.Errorf("expected clamped bucket with capacity 1 to deny a second request")
	}
}

func TestTokenBucket_RefillOverTime(t *testing.T) {
	// Small capacity, fast refill rate so a short real sleep produces a
	// measurable, deterministic-enough refill.
	tb := NewTokenBucket(2, 100) // 100 tokens/sec => 1 token per 10ms

	if !tb.Allow(2) {
		t.Fatalf("expected initial full bucket to allow consuming full capacity")
	}
	if tb.Allow(1) {
		t.Fatalf("expected empty bucket to deny immediately after draining")
	}

	// Sleep long enough to refill well over one token, but keep the test fast.
	time.Sleep(50 * time.Millisecond)

	if !tb.Allow(1) {
		t.Fatalf("expected bucket to have refilled at least 1 token after 50ms at 100 tokens/sec")
	}
}

func TestTokenBucket_RefillCapsAtCapacity(t *testing.T) {
	tb := NewTokenBucket(1, 1000) // fast refill

	time.Sleep(20 * time.Millisecond)

	// Even though more than 1 token's worth of time has elapsed, the bucket
	// must not exceed its capacity.
	if !tb.Allow(1) {
		t.Fatalf("expected bucket to allow consuming its single token")
	}
	if tb.Allow(1) {
		t.Fatalf("expected bucket to deny a second request immediately, since capacity caps refill at 1")
	}
}

// TestTokenBucket_ConcurrentAllow proves that Allow's mutex prevents
// overcounting under concurrent access: with capacity=10 and refillRate=0,
// no more than 10 of many concurrent Allow(1) calls may succeed. Run with
// -race to additionally prove there is no data race.
func TestTokenBucket_ConcurrentAllow(t *testing.T) {
	const capacity = 10
	const numGoroutines = 100

	tb := NewTokenBucket(capacity, 0)

	var successCount int64
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if tb.Allow(1) {
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	if successCount != capacity {
		t.Errorf("successCount = %d, want exactly %d (capacity)", successCount, capacity)
	}
	if successCount > capacity {
		t.Fatalf("successCount = %d exceeds capacity %d: mutex failed to prevent overcounting", successCount, capacity)
	}
}
