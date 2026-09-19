package ratelimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSlidingWindow_AllowUpToMax(t *testing.T) {
	tests := []struct {
		name        string
		maxRequests int
		numCalls    int
		wantAllowed int
	}{
		{name: "allow up to max within window", maxRequests: 3, numCalls: 3, wantAllowed: 3},
		{name: "deny over max within window", maxRequests: 3, numCalls: 5, wantAllowed: 3},
		{name: "single max request", maxRequests: 1, numCalls: 4, wantAllowed: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw := NewSlidingWindow(tt.maxRequests, time.Minute)
			allowed := 0
			for i := 0; i < tt.numCalls; i++ {
				if sw.Allow() {
					allowed++
				}
			}
			if allowed != tt.wantAllowed {
				t.Errorf("allowed = %d, want %d", allowed, tt.wantAllowed)
			}
		})
	}
}

func TestSlidingWindow_ConstructorClamping(t *testing.T) {
	sw := NewSlidingWindow(0, -time.Second)
	if !sw.Allow() {
		t.Errorf("expected clamped window (maxRequests>=1) to allow a single request")
	}
	if sw.Allow() {
		t.Errorf("expected clamped window with maxRequests 1 to deny a second immediate request")
	}
}

func TestSlidingWindow_AllowedAgainAfterWindowPasses(t *testing.T) {
	window := 40 * time.Millisecond
	sw := NewSlidingWindow(2, window)

	if !sw.Allow() || !sw.Allow() {
		t.Fatalf("expected first two requests within an empty window to be allowed")
	}
	if sw.Allow() {
		t.Fatalf("expected third request to be denied while first two are still within window")
	}

	// Wait for the window to fully elapse.
	time.Sleep(window + 20*time.Millisecond)

	if !sw.Allow() {
		t.Fatalf("expected request to be allowed again after the window passed")
	}
}

// TestSlidingWindow_ConcurrentAllow proves that Allow's mutex prevents
// overcounting under concurrent access: with maxRequests=10 and a long
// window, no more than 10 of many concurrent Allow calls may succeed. Run
// with -race to additionally prove there is no data race.
func TestSlidingWindow_ConcurrentAllow(t *testing.T) {
	const maxRequests = 10
	const numGoroutines = 100

	sw := NewSlidingWindow(maxRequests, time.Minute)

	var successCount int64
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if sw.Allow() {
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	if successCount != maxRequests {
		t.Errorf("successCount = %d, want exactly %d (maxRequests)", successCount, maxRequests)
	}
	if successCount > maxRequests {
		t.Fatalf("successCount = %d exceeds maxRequests %d: mutex failed to prevent overcounting", successCount, maxRequests)
	}
}
