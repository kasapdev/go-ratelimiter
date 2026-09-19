package ratelimiter

import (
	"context"
	"errors"
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

func TestTokenBucket_Wait_ImmediateWhenTokensAvailable(t *testing.T) {
	tb := NewTokenBucket(3, 1)
	start := time.Now()
	if err := tb.Wait(context.Background(), 2); err != nil {
		t.Fatalf("Wait returned %v, want nil", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("Wait should not block when tokens are available")
	}
	if !tb.Allow(1) || tb.Allow(1) {
		t.Fatal("Wait(2) should have left exactly 1 token")
	}
}

func TestTokenBucket_Wait_BlocksUntilRefilled(t *testing.T) {
	tb := NewTokenBucket(1, 100) // one token every 10ms
	if !tb.Allow(1) {
		t.Fatal("bucket should start full")
	}
	start := time.Now()
	if err := tb.Wait(context.Background(), 1); err != nil {
		t.Fatalf("Wait returned %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("Wait blocked for %v, want roughly 10ms", elapsed)
	}
}

func TestTokenBucket_Wait_ContextTimeoutConsumesNothing(t *testing.T) {
	tb := NewTokenBucket(2, 0.01) // a token every 100s: effectively never
	tb.Allow(1)                   // 1 token left
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := tb.Wait(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait returned %v, want DeadlineExceeded", err)
	}
	if !tb.Allow(1) {
		t.Fatal("a failed Wait must not consume tokens")
	}
}

func TestTokenBucket_Wait_ContextAlreadyCanceled(t *testing.T) {
	tb := NewTokenBucket(1, 0.01)
	tb.Allow(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tb.Wait(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait returned %v, want Canceled", err)
	}
}

func TestTokenBucket_Wait_ImpossibleRequestsFailFast(t *testing.T) {
	tb := NewTokenBucket(2, 1)
	if err := tb.Wait(context.Background(), 3); !errors.Is(err, ErrCostExceedsCapacity) {
		t.Fatalf("cost > capacity: got %v, want ErrCostExceedsCapacity", err)
	}

	frozen := NewTokenBucket(1, 0)
	frozen.Allow(1)
	if err := frozen.Wait(context.Background(), 1); !errors.Is(err, ErrNoRefill) {
		t.Fatalf("zero refill: got %v, want ErrNoRefill", err)
	}
}

func TestTokenBucket_Wait_NonPositiveCostIsNoop(t *testing.T) {
	tb := NewTokenBucket(1, 0)
	tb.Allow(1)
	if err := tb.Wait(context.Background(), 0); err != nil {
		t.Fatalf("Wait(0) = %v, want nil", err)
	}
	if err := tb.Wait(context.Background(), -5); err != nil {
		t.Fatalf("Wait(-5) = %v, want nil", err)
	}
}

func TestTokenBucket_Wait_ConcurrentWaitersNeverOverConsume(t *testing.T) {
	tb := NewTokenBucket(1, 200) // 5ms per token
	tb.Allow(1)
	const waiters = 4
	var done atomic.Int32
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tb.Wait(context.Background(), 1); err == nil {
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	if done.Load() != waiters {
		t.Fatalf("%d/%d waiters succeeded", done.Load(), waiters)
	}
	// 4 tokens at 200/s cannot be produced in much less than ~20ms.
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("waiters finished in %v: tokens were over-consumed", elapsed)
	}
}
