// Package cache is the Go implementation of the codefly.dev/cache interface: a
// stack of cache layers over an optional authoritative origin.
//
// A consumer composes caching; nothing it caches knows about it. The stack is
// read top-down through any number of layers — say an in-process Memory, then
// a shared Redis — down to the origin (object storage, a database, a remote
// API), and filled bottom-up. A miss reaches the origin once: singleflight
// collapses callers inside a process, and a lease on the deepest leasing layer
// collapses processes. An expired entry that carries a version is revalidated with a
// conditional load, so an unchanged value never moves again.
//
// Every stack operation carries a Partition naming who may share its data, and
// fails closed without one. The stack prefixes keys with it in every layer, so
// values, fill leases, negative entries and invalidations never cross
// partitions, and drivers never see one. The cache stores data, never
// decisions: the caller's authorization check still runs before every read.
//
// Drivers implement Layer, and optionally Leaser (fill-once across processes)
// and Notifier (evict in-process copies of a key that changed, whoever changed
// it), which is also a Resyncer when it can lose notices. Origins
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

// Notifier is a Layer that reports every key that changed in it, by any
// writer: another process through its driver, a client that bypasses the
// driver, or the server itself. The stack evicts the key from the layers above,
// so a Notifier is what keeps in-process copies from outliving a change made
// anywhere. It is optional; without one, layers above are bounded by their own
// TTL.
//
// A Notifier may also report changes made through its own client: eviction is
// idempotent, so the stack does not rely on either. It may report expiry and
// eviction as changes. A Notifier either never loses a notice, as Memory, or is
// a Resyncer and says when it may have.
type Notifier interface {
	Layer
	// Subscribe calls fn with each key that changed, until stop is called. It
	// returns once the subscription is active.
	Subscribe(ctx context.Context, fn func(key string)) (stop func(), err error)
}

// Resyncer is a Notifier that can lose notices — a dropped connection, a
// server restart, a full buffer — and signals each gap. On the signal the
// stack flushes every layer above this one, all partitions and generations
// alike, because any key may have changed unreported. A Notifier that can lose
// notices and does not implement Resyncer breaks the contract: the copies above
// it would outlive changes until their TTL.
type Resyncer interface {
	Notifier
	// SubscribeResync calls fn after each gap in which notices may have been
	// lost, once key notices flow again, until stop is called. It returns once
	// the subscription is active.
	SubscribeResync(ctx context.Context, fn func()) (stop func(), err error)
}

// Flusher is a Layer that can drop every entry it holds. A layer above a
// Resyncer must be one, since a resync cannot name the keys to drop; New
// refuses a stack where it is not.
type Flusher interface {
	Layer
	// Flush drops every entry and revokes every fill lease.
	Flush(ctx context.Context) error
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
