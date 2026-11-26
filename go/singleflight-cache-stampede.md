# Preventing Cache Stampedes with Go `singleflight`

When a popular cache key expires—or a cold service starts receiving traffic—hundreds of concurrent requests can miss the cache simultaneously and slam the underlying database or upstream API with identical queries. This **thundering herd** (cache stampede) spikes tail latency and can trigger cascading outages.

Go's `golang.org/x/sync/singleflight` package eliminates duplicate in-flight work by **coalescing concurrent calls** for the same key: only one goroutine executes the expensive lookup while all other concurrent callers block and share its result.

## 🚀 Core Guarantees

- **In-Flight Request Coalescing**: While a call for `key` is running, N concurrent callers wait on the single execution instead of issuing N database queries.
- **Zero Lock Contention Across Keys**: Calls for different keys execute independently in parallel.
- **Shared Flag Visibility**: The returned `shared` boolean tells callers whether the result was deduplicated across multiple goroutines (useful for metrics and defensive copying).

---

## 🛠️ Production Pattern: `DoChan` + Detached Timeout

Calling `group.Do(key, fn)` with a single request's `ctx` has a subtle bug: if the **first** caller disconnects, `ctx` cancels and fails the shared query for every other waiting caller. Use `DoChan` with `context.WithoutCancel` (Go 1.21+) so each caller honors its own timeout without aborting the shared upstream fetch.

```go
package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type UserProfile struct {
	ID    string
	Name  string
	Roles []string
}

type Store interface {
	FetchUser(ctx context.Context, id string) (UserProfile, error)
}

type UserCache struct {
	store Store
	group singleflight.Group

	mu      sync.RWMutex
	entries map[string]cachedUser
}

type cachedUser struct {
	profile   UserProfile
	expiresAt time.Time
}

func NewUserCache(store Store) *UserCache {
	return &UserCache{
		store:   store,
		entries: make(map[string]cachedUser),
	}
}

func (c *UserCache) Get(ctx context.Context, userID string) (UserProfile, error) {
	if profile, ok := c.getFresh(userID); ok {
		return profile, nil
	}

	// DoChan lets each caller select on its own ctx.Done() independently
	ch := c.group.DoChan(userID, func() (any, error) {
		// Detach caller cancellation so one client disconnect does not poison all waiters,
		// while preserving trace/request values and enforcing a bounded upstream timeout.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()

		profile, err := c.store.FetchUser(fetchCtx, userID)
		if err != nil {
			return UserProfile{}, err
		}

		c.set(userID, profile, 30*time.Second)
		return profile, nil
	})

	select {
	case <-ctx.Done():
		return UserProfile{}, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return UserProfile{}, res.Err
		}
		profile, ok := res.Val.(UserProfile)
		if !ok {
			return UserProfile{}, errors.New("unexpected singleflight value type")
		}
		// Clone mutable slice fields so concurrent callers cannot race on shared backing arrays
		return cloneProfile(profile), nil
	}
}

func (c *UserCache) Invalidate(userID string) {
	c.mu.Lock()
	delete(c.entries, userID)
	c.mu.Unlock()

	// Prevent an in-flight read (started before a write) from populating stale data for new callers
	c.group.Forget(userID)
}

func (c *UserCache) getFresh(userID string) (UserProfile, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.entries[userID]
	if !ok || time.Now().After(item.expiresAt) {
		return UserProfile{}, false
	}
	return cloneProfile(item.profile), true
}

func (c *UserCache) set(userID string, profile UserProfile, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[userID] = cachedUser{
		profile:   cloneProfile(profile),
		expiresAt: time.Now().Add(ttl),
	}
}

func cloneProfile(p UserProfile) UserProfile {
	if p.Roles != nil {
		roles := make([]string, len(p.Roles))
		copy(roles, p.Roles)
		p.Roles = roles
	}
	return p
}
```

> **Tip**: Pair `singleflight` with **TTL jitter** (e.g., `30s ± 5s`) or **stale-while-revalidate** so hot keys expire gradually instead of triggering synchronized stampedes across instances.

---

## ⚠️ Common Pitfalls to Avoid

1. **Passing a Caller's Cancellable Context into `Do`**: If Client A times out at 50ms or closes its browser tab, passing `r.Context()` directly into the shared closure cancels the DB call for Clients B, C, and D too. Always detach cancellation (`context.WithoutCancel`) and apply a shared upstream timeout.
2. **Mutating Shared Returned Reference Types**: `singleflight` hands the exact same `Val` to all waiting goroutines. If `Val` is a pointer, slice, or map and one caller mutates it, you get a data race (`go test -race` will catch it). Return immutable structs or defensive clones.
3. **Ignoring Cache Invalidation Races (`Forget`)**: If a write updates the database and deletes the cache key while a read is already in flight, new readers coalescing onto that old `singleflight` call can still receive stale pre-write data. Call `group.Forget(key)` during invalidation so subsequent reads start a fresh fetch.
4. **Blocking Indefinitely on `Do` Without Caller Timeouts**: `group.Do` ignores `ctx.Done()` while waiting on the leader goroutine. Use `DoChan` + `select` on `ctx.Done()` so callers fail fast within their own SLA even if the upstream is slow.
5. **Assuming Global Deduplication Across Pods**: `singleflight.Group` only deduplicates within a single Go process. In a 40-pod cluster, a hot cache miss still hits the database up to 40 times unless combined with distributed locking, consistent-hash routing, or Redis-level coalescing.
