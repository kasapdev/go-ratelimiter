# go-ratelimiter

Thread-safe, zero-dependency rate limiters for Go: a **token bucket** (burst
+ steady refill rate) and a **sliding window** log (max N requests per
trailing duration).

Both limiters use only the Go standard library (`sync`, `time`) — no
third-party dependencies.

## Installation

```sh
go get github.com/kasapdev/go-ratelimiter
```

## Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/kasapdev/go-ratelimiter"
)

func main() {
	// TokenBucket: capacity of 5 tokens, refilling at 2 tokens/sec.
	bucket := ratelimiter.NewTokenBucket(5, 2)

	for i := 0; i < 7; i++ {
		if bucket.Allow(1) {
			fmt.Printf("request %d: allowed\n", i)
		} else {
			fmt.Printf("request %d: rate limited\n", i)
		}
	}

	// SlidingWindow: at most 3 requests per 1-second trailing window.
	window := ratelimiter.NewSlidingWindow(3, time.Second)

	for i := 0; i < 5; i++ {
		if window.Allow() {
			fmt.Printf("sliding request %d: allowed\n", i)
		} else {
			fmt.Printf("sliding request %d: rate limited\n", i)
		}
	}
}
```

## API

### `type TokenBucket`

A thread-safe token-bucket limiter. Holds up to `capacity` tokens and
refills at `refillRate` tokens per second, computed lazily from elapsed
wall-clock time on each call (no background goroutine or ticker).

- `func NewTokenBucket(capacity int, refillRate float64) *TokenBucket` —
  creates a bucket that starts full. `capacity <= 0` is clamped to 1;
  negative `refillRate` is clamped to 0.
- `func (tb *TokenBucket) Allow(cost int) bool` — attempts to consume `cost`
  tokens; returns whether they were available. Non-positive `cost` is
  always allowed and consumes nothing. Safe for concurrent use.

### `type SlidingWindow`

A thread-safe sliding-window-log limiter. Allows at most `maxRequests`
calls to `Allow` within any trailing period of `window` duration, tracking
and pruning individual request timestamps.

- `func NewSlidingWindow(maxRequests int, window time.Duration) *SlidingWindow` —
  creates a new limiter. `maxRequests <= 0` is clamped to 1; `window <= 0`
  is clamped to 1 second.
- `func (sw *SlidingWindow) Allow() bool` — reports whether a new request is
  permitted right now, recording it if so. Safe for concurrent use.

## Testing

```sh
go test ./...
```

Concurrency correctness (including a dedicated test that hammers a
`TokenBucket` from 100 goroutines and asserts successful allows never
exceed capacity) can be verified with the race detector:

```sh
go test ./... -race -v
```

## License

MIT — see [LICENSE](LICENSE).
