// Package cache is the Go implementation of the codefly.dev/cache interface: a
// stack of cache layers over an optional authoritative origin.
//
// A consumer composes caching; nothing it caches knows about it. The stack is
// read top-down — an in-process Memory layer, then shared layers such as the
// Redis driver, then the origin (object storage, a database, a remote API) —
// and filled bottom-up. A miss reaches the origin once: singleflight collapses
// callers inside a process, and a lease on the first shared layer collapses
// processes. An expired entry that carries a version is revalidated with a
// conditional load, so an unchanged value never moves again.
//
// Drivers implement Layer, and optionally Leaser (fill-once across processes)
// and Notifier (evict in-process copies when another process writes). Origins
// implement Source, and Store when writes should go through the stack. The
// conformance suite in cachetest is the definition of a correct driver.
package cache

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrMiss is returned by a Layer that holds no entry for a key. The stack
	// never returns it to a caller when an origin is configured.
	ErrMiss = errors.New("cache: miss")

	// ErrNotFound means the origin holds no value for the key. The stack caches
	// it as a negative entry for the configured negative TTL.
	ErrNotFound = errors.New("cache: not found")

	// ErrNotModified is returned by Source.Load when the origin's current
	// version equals the one the caller already holds.
	ErrNotModified = errors.New("cache: not modified")

	// ErrLeaseLost is returned by Leaser.Fill when the lease expired, or when
	// the key was written or deleted after the lease was acquired: the loaded
	// value may predate that write and must not be cached.
	ErrLeaseLost = errors.New("cache: lease lost")

	// ErrTooLarge is returned by a Layer that refuses a value over its size
	// cap. The stack skips that layer for the value; it is not a failure.
	ErrTooLarge = errors.New("cache: value too large for layer")

	// ErrReadOnlyOrigin is returned by Stack.Set and Stack.Delete when the
	// origin cannot be written: writing the layers alone would make them
	// disagree with the origin.
	ErrReadOnlyOrigin = errors.New("cache: origin is read-only")
)

// Entry is one cached value.
type Entry struct {
	// Value is the cached bytes. An empty Value is a value, not a miss.
	Value []byte
	// Version is the origin's validator for Value (an ETag, a row version).
	// Empty means the entry cannot be revalidated, only reloaded.
	Version string
	// Missing marks a negative entry: the origin reported the key absent.
	Missing bool
	// FreshUntil is when the entry stops being served without revalidation.
	// The zero time means fresh for as long as the layer keeps it.
	FreshUntil time.Time
}

// Fresh reports whether the entry may be served at now without asking the
// origin.
func (e Entry) Fresh(now time.Time) bool {
	return e.FreshUntil.IsZero() || now.Before(e.FreshUntil)
}

// Layer holds copies of values. Implementations must be safe for concurrent
// use.
type Layer interface {
	// Get returns the entry for key, or ErrMiss.
	Get(ctx context.Context, key string) (Entry, error)
	// Set stores e for key, to be dropped after ttl. It must invalidate any
	// outstanding lease on key (see Leaser).
	Set(ctx context.Context, key string, e Entry, ttl time.Duration) error
	// Delete removes key. Deleting an absent key is not an error. It must
	// invalidate any outstanding lease on key.
	Delete(ctx context.Context, key string) error
}

// Lease is the right to fill one key, granted to exactly one holder at a time.
type Lease struct {
	Key   string
	Token string
}

// Leaser is a Layer shared between processes that can grant fill leases, so a
// miss reaches the origin once across every process sharing the layer.
type Leaser interface {
	Layer
	// Acquire tries to take the fill lease on key for ttl. ok is false when
	// another holder has it.
	Acquire(ctx context.Context, key string, ttl time.Duration) (lease Lease, ok bool, err error)
	// Fill stores e under the lease and releases it. It returns ErrLeaseLost,
	// storing nothing, when the lease expired or key was Set or Deleted since
	// it was acquired.
	Fill(ctx context.Context, lease Lease, e Entry, ttl time.Duration) error
	// Release gives the lease up without storing anything, when it is still
	// held.
	Release(ctx context.Context, lease Lease) error
}

// Notifier is a Layer that reports keys other processes Set or Deleted, so the
// stack can evict its in-process copies. Delivery may be lossy; layers above a
// Notifier are still bounded by their own TTL.
type Notifier interface {
	Layer
	// Subscribe calls fn with each key another process changed, until stop is
	// called. It returns once the subscription is active.
	Subscribe(ctx context.Context, fn func(key string)) (stop func(), err error)
}

// Source is the authoritative origin under a stack.
type Source interface {
	// Load returns the current value for key. When ifNotVersion is non-empty
	// and still current, it returns ErrNotModified without the value. An
	// absent key is ErrNotFound.
	Load(ctx context.Context, key string, ifNotVersion string) (Entry, error)
}

// Store is a Source that accepts writes through the stack.
type Store interface {
	Source
	// Put writes value and returns its new version.
	Put(ctx context.Context, key string, value []byte) (version string, err error)
	// Remove deletes key. Removing an absent key is not an error.
	Remove(ctx context.Context, key string) error
}

// SourceFunc adapts a function to Source, for origins that are consumer code —
// a database query, a remote call.
type SourceFunc func(ctx context.Context, key string, ifNotVersion string) (Entry, error)

// Load calls f.
func (f SourceFunc) Load(ctx context.Context, key string, ifNotVersion string) (Entry, error) {
	return f(ctx, key, ifNotVersion)
}
