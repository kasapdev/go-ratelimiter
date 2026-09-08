package ratelimiter

import (
	"sync"
	"time"
)

// SlidingWindow is a thread-safe sliding-window-log rate limiter. It allows
// at most maxRequests calls to Allow within any trailing window of duration
// window, measured continuously (as opposed to a fixed-window counter that
// resets on a clock boundary).
//
// Internally it tracks the timestamp of every request that is still within
// the current window and prunes older entries on each call.
type SlidingWindow struct {
	mu sync.Mutex

	maxRequests int
	window      time.Duration
	timestamps  []time.Time
}

// NewSlidingWindow creates a new SlidingWindow that allows at most
// maxRequests calls to Allow within any trailing period of the given window
// duration.
//
// If maxRequests is <= 0 it is clamped to 1, since a limiter that never
// allows any request would make Allow always return false. If window is <= 0
// it is clamped to 1 second.
func NewSlidingWindow(maxRequests int, window time.Duration) *SlidingWindow {
	if maxRequests <= 0 {
		maxRequests = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &SlidingWindow{
		maxRequests: maxRequests,
		window:      window,
		timestamps:  make([]time.Time, 0, maxRequests),
	}
}

// Allow reports whether a new request is permitted right now. It first
// prunes any recorded timestamps older than the configured window, then, if
// fewer than maxRequests remain within the window, records the current time
// and returns true. Otherwise it returns false without recording anything.
//
// Allow is safe for concurrent use by multiple goroutines.
func (sw *SlidingWindow) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-sw.window)

	i := 0
	for i < len(sw.timestamps) && sw.timestamps[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		sw.timestamps = sw.timestamps[i:]
	}

	if len(sw.timestamps) >= sw.maxRequests {
		return false
	}

	sw.timestamps = append(sw.timestamps, now)
	return true
}
