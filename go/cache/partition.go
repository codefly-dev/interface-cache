package cache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
)

var (
	// ErrNoPartition is returned by every Stack operation given the zero
	// Partition, or one built from an empty key. The stack fails closed: a
	// caller that does not say whose data it is reading gets nothing.
	ErrNoPartition = errors.New("cache: operation carries no partition")

	// ErrWrongPartition is returned when the partition does not match the
	// stack: Global on a partitioned stack, or a caller's partition on a stack
	// built WithGlobal, which would otherwise store per-viewer data where every
	// viewer reads it.
	ErrWrongPartition = errors.New("cache: partition does not match the stack")
)

// Partition scopes one Stack operation to the callers allowed to share its
// data. The stack prefixes every key with it in every layer, and so also
// scopes fill leases, negative entries and invalidation notices: two
// partitions never observe each other.
//
// The key is opaque to this package. The caller derives it from who is asking
// (codefly's SDK derives it from a verified Work Context): it always names the
// tenant, and adds a digest of the effective authorization view — scopes and
// authorization revision — when the data itself varies by viewer. A revision
// bump then moves callers to a fresh partition, so nothing cached before it is
// served after it. It never includes session or task ids, which carry no
// authorization meaning and would make every read a miss.
//
// Data varies by viewer whenever the origin reads the caller's identity from
// the context — per-caller credentials, row-level security. A shared load runs
// with the context of the caller that started it, so every caller in the
// partition receives what that one caller was allowed to see: such an origin
// needs the authorization-view digest in the key.
//
// Partition is comparable. Its zero value is no partition, which every
// operation refuses.
type Partition struct {
	key         string
	writeAround bool
	global      bool
}

// PartitionOption configures a Partition.
type PartitionOption func(*Partition)

// WriteAround marks a partition whose reads go to the origin and store
// nothing, in any layer: work done under an approval grant, whose result must
// not be served to callers who never held the grant. Such a read neither
// consults the layers nor takes a fill lease; its writes still reach the
// origin and invalidate the key as any write does.
func WriteAround() PartitionOption { return func(p *Partition) { p.writeAround = true } }

// NewPartition returns the partition for key. An empty key yields a partition
// every operation refuses with ErrNoPartition.
func NewPartition(key string, opts ...PartitionOption) Partition {
	p := Partition{key: key}
	for _, opt := range opts {
		opt(&p)
	}
	return p
}

// Global is the partition of a stack built WithGlobal, for data that does not
// vary by viewer. It is refused by every other stack.
func Global() Partition { return Partition{global: true} }

// The keys a stack uses in its layers. The leading byte names the family, and
// every variable part before the caller's key is length-prefixed, so no
// partition, generation and key can spell another's layer key.
//
//	value, no origin      "p" len ":" partition ":" key    |  "g:" key
//	copy of an origin key "P" len ":" partition ":" len ":" generation ":" key
//	                      "G" len ":" generation ":" key
//	generation            "i:" key
//
// Write-around does not change the key.

// layerKey is where a stack without an origin stores key in p.
func (p Partition) layerKey(key string) string {
	if p.global {
		return "g:" + key
	}
	return "p" + strconv.Itoa(len(p.key)) + ":" + p.key + ":" + key
}

// entryKey is where a stack with an origin stores its copy of key in p, under
// the key's current generation.
func (p Partition) entryKey(generation, key string) string {
	g := strconv.Itoa(len(generation)) + ":" + generation + ":" + key
	if p.global {
		return "G" + g
	}
	return "P" + strconv.Itoa(len(p.key)) + ":" + p.key + ":" + g
}

// generationKey is where the layers hold key's generation. It is shared by
// every partition: a write replaces it, which orphans every partition's copy.
func generationKey(key string) string { return "i:" + key }

// newGeneration returns a token no earlier generation of any key used, so an
// expired or evicted generation is replaced by one no stored copy is under.
func newGeneration() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
