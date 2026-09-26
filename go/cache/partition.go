package cache

import (
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
// origin and drop the key's cached copies in the partition.
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

// Key returns the partition's opaque key; empty for Global and the zero value.
func (p Partition) Key() string { return p.key }

// IsWriteAround reports whether reads in p store nothing.
func (p Partition) IsWriteAround() bool { return p.writeAround }

// IsGlobal reports whether p is Global.
func (p Partition) IsGlobal() bool { return p.global }

// LayerKey returns the key the stack reads and writes in its layers for key
// under p, and "" for a partition every stack refuses. Operators use it to
// find an entry in a shared layer; conformance tests use it to check what the
// stack stored. The encoding is part of the interface version:
//
//	partitioned  "p" + len(partition key) + ":" + partition key + ":" + key
//	global       "g:" + key
//
// The length makes it unambiguous, so no pair of partition and key can reach
// another's entry. Write-around does not change the layer key.
func (p Partition) LayerKey(key string) string {
	switch {
	case p.global:
		return "g:" + key
	case p.key == "":
		return ""
	default:
		return "p" + strconv.Itoa(len(p.key)) + ":" + p.key + ":" + key
	}
}
