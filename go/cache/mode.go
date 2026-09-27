package cache

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// A Mode is the consistency a stack keeps for a read or a write, chosen along
// four independent dimensions: how fresh a read must be, how a write reaches
// the layers, how concurrent fills are coordinated, and what a failing layer
// does. Each value is documented with its guarantee as a property the
// conformance suite and the simulation in cachetest check.
//
// A stack has a default mode (WithMode), may override it for key spaces
// (WithKeySpace), and a call may override it again (Get(ctx, p, key,
// Validated())). New refuses a mode the stack's layers and origin cannot keep,
// naming the missing capability; a call with such a mode returns
// ErrUnsupportedMode. Nothing degrades silently to a weaker mode.
type Mode struct {
	Freshness Freshness
	Write     WritePropagation
	Fill      FillCoordination
	Failure   FailurePolicy
}

// ModeOption sets one dimension of a Mode. Every mode value below is one.
type ModeOption interface{ apply(*Mode) }

// ErrUnsupportedMode is returned by a call whose mode the stack cannot keep.
// The message names the missing capability.
var ErrUnsupportedMode = errors.New("cache: mode not supported by this stack")

// Freshness is how fresh a value a read may return.
type Freshness struct {
	kind  freshnessKind
	bound time.Duration
}

type freshnessKind int

const (
	freshDefault freshnessKind = iota // TTLBounded(the longest tier TTL)
	freshTTL
	freshSWR
	freshInvalidate
	freshRYW
	freshValidated
	freshBypass
)

func (f Freshness) apply(m *Mode) { m.Freshness = f }

// TTLBounded never serves a value last confirmed by the origin more than d
// ago, measured with every process's clock assumed within WithClockSkew of
// the reader's. A copy older than d is revalidated (conditionally when it
// carries a version) or reloaded. It needs nothing from the layers.
//
// The stack's default is TTLBounded with d the longest tier TTL.
func TTLBounded(d time.Duration) Freshness { return Freshness{kind: freshTTL, bound: d} }

// StaleWhileRevalidate serves a copy up to d past the stack's TTL bound (the
// longest tier TTL) while one background revalidation refreshes it, so a hot
// key never waits on the origin. It never serves a value last confirmed more
// than the TTL bound plus d ago; a copy older than that is reloaded before it
// is served. It needs an origin.
func StaleWhileRevalidate(d time.Duration) Freshness { return Freshness{kind: freshSWR, bound: d} }

// InvalidateOnWrite serves a copy for as long as its layer keeps it, until a
// write through any stack sharing the deepest tier invalidates it. It never
// serves a value a write through a stack superseded more than the notifier's
// declared NoticeBound before the read began; while a notifier is in a gap it
// serves nothing from the layers above it, and after a resync nothing those
// layers held. A write the origin receives some other way is covered only
// through Stack.Invalidate or an origin that declares ChangeFeed.
//
// Every tier but the deepest needs a Notifier below it that declares a
// NoticeBound.
func InvalidateOnWrite() Freshness { return Freshness{kind: freshInvalidate} }

// ReadYourWrites is the TTL bound of the stack's default, plus: a process
// never reads older than its own last acknowledged write of the key, whatever
// the layers hold (a replica that has not caught up, an evicted copy refilled
// from a stale tier). A copy that does not carry the version the process's
// own write produced is checked against the origin. It needs a Store origin
// or none.
func ReadYourWrites() Freshness { return Freshness{kind: freshRYW} }

// Validated checks the origin's version on every read with a conditional
// load, so it never serves a version the origin had superseded when the read
// began. It pays one conditional round trip per read and joins no other
// caller's load. It needs an origin that declares ConditionalLoads.
func Validated() Freshness { return Freshness{kind: freshValidated} }

// Bypass reads the origin alone: no layer is consulted or filled, and the load
// is shared with no other caller. It always returns the origin's value as of
// the read. It needs an origin.
func Bypass() Freshness { return Freshness{kind: freshBypass} }

func (f Freshness) String() string {
	switch f.kind {
	case freshTTL:
		return fmt.Sprintf("TTLBounded(%s)", f.bound)
	case freshSWR:
		return fmt.Sprintf("StaleWhileRevalidate(%s)", f.bound)
	case freshInvalidate:
		return "InvalidateOnWrite"
	case freshRYW:
		return "ReadYourWrites"
	case freshValidated:
		return "Validated"
	case freshBypass:
		return "Bypass"
	}
	return "TTLBounded(longest tier TTL)"
}

// WritePropagation is what a write through the stack does to the layers.
type WritePropagation int

const (
	// PropagateInvalidate writes the origin, then replaces the key's
	// generation in every layer, so no partition reads a copy made before the
	// write. The default.
	PropagateInvalidate WritePropagation = iota
	// PropagateThrough is PropagateInvalidate, then stores the new value and
	// its version as the writer's copy, so the writer's next read is a hit.
	// Another write can land between the origin taking this one and the new
	// generation, so the copy is made after a conditional load of the version
	// the origin returned: one round trip that transfers nothing when the
	// value is unchanged, and stores the newer value when it is not.
	PropagateThrough
	// PropagateOriginOnly writes the origin and touches no layer: copies
	// catch up through their freshness mode. InvalidateOnWrite refuses it.
	PropagateOriginOnly
	// PropagateBehind stores the value as the writer's copy and acknowledges,
	// then writes the origin in the background. It loses the write if the
	// process dies first.
	PropagateBehind
)

func (w WritePropagation) apply(m *Mode) { m.Write = w }

// WriteInvalidate is PropagateInvalidate.
func WriteInvalidate() WritePropagation { return PropagateInvalidate }

// WriteThrough is PropagateThrough.
func WriteThrough() WritePropagation { return PropagateThrough }

// WriteOriginOnly is PropagateOriginOnly: the write goes around the layers.
// (The partition option WriteAround is a different thing: a partition whose
// reads and writes all bypass the layers, for work under an approval grant.)
func WriteOriginOnly() WritePropagation { return PropagateOriginOnly }

// WriteBehind is PropagateBehind. It is opt-in by name: pass AcceptWriteLoss,
// since a write acknowledged but not yet written to the origin is lost when
// the process dies. Stack.Drain waits for the queue; Stack.Close does not, and
// reports every write it abandons through Drain's error and
// WithWriteBehindErrors. At most WithWriteBehindQueue writes may be waiting:
// past that a write is refused with ErrWriteBehindFull rather than
// acknowledged, so a stalled origin cannot grow the queue without bound.
//
// Until the origin has it, the writing process reads its own pending write in
// the writing partition (except with Bypass, which reads the origin);
// everyone else reads the origin's value. Once it lands,
// the key's generation is replaced again, so copies loaded in between are
// dropped.
func WriteBehind(accepted WriteLossAccepted) WritePropagation {
	_ = accepted
	return PropagateBehind
}

// WriteLossAccepted is the acknowledgement WriteBehind requires.
type WriteLossAccepted struct{ _ bool }

// AcceptWriteLoss acknowledges that WriteBehind loses writes if the process
// dies before they reach the origin.
var AcceptWriteLoss = WriteLossAccepted{}

func (w WritePropagation) String() string {
	switch w {
	case PropagateThrough:
		return "WriteThrough"
	case PropagateOriginOnly:
		return "WriteOriginOnly"
	case PropagateBehind:
		return "WriteBehind"
	}
	return "WriteInvalidate"
}

// FillCoordination is how concurrent misses of one key share the origin.
type FillCoordination int

const (
	// CoordinateSingleflight collapses concurrent misses of one key in one
	// partition within a process into one load. The default.
	CoordinateSingleflight FillCoordination = iota
	// CoordinateNone lets every miss load the origin.
	CoordinateNone
	// CoordinateLease adds a fill lease on the deepest leasing tier, so a
	// miss reaches the origin once across every process sharing it.
	CoordinateLease
	// CoordinateVersionFenced is singleflight, and every fill is stored with
	// Fencer.SetFenced: a fill never replaces a copy of a newer version, with
	// or without leases.
	CoordinateVersionFenced
)

func (c FillCoordination) apply(m *Mode) { m.Fill = c }

// FillUncoordinated is CoordinateNone.
func FillUncoordinated() FillCoordination { return CoordinateNone }

// FillSingleflight is CoordinateSingleflight.
func FillSingleflight() FillCoordination { return CoordinateSingleflight }

// FillLease is CoordinateLease. It needs a tier that declares Leases.
func FillLease() FillCoordination { return CoordinateLease }

// FillVersionFenced is CoordinateVersionFenced. It needs an origin that
// declares Sequenced and every tier to declare Fences.
func FillVersionFenced() FillCoordination { return CoordinateVersionFenced }

func (c FillCoordination) String() string {
	switch c {
	case CoordinateNone:
		return "FillUncoordinated"
	case CoordinateLease:
		return "FillLease"
	case CoordinateVersionFenced:
		return "FillVersionFenced"
	}
	return "FillSingleflight"
}

// FailurePolicy is what a failing layer does to a read or a write.
type FailurePolicy int

const (
	// FailDegrade skips a failing layer behind a breaker: reads fall through
	// to the next layer or the origin. The default.
	FailDegrade FailurePolicy = iota
	// FailClosedPolicy returns ErrUnavailable rather than skip a layer: a read
	// that cannot consult every tier fails, and a write replaces the key's
	// generation in every tier before it touches the origin, failing with the
	// origin unchanged when one refuses.
	FailClosedPolicy
)

func (f FailurePolicy) apply(m *Mode) { m.Failure = f }

// Degrade is FailDegrade.
func Degrade() FailurePolicy { return FailDegrade }

// FailClosed is FailClosedPolicy.
func FailClosed() FailurePolicy { return FailClosedPolicy }

func (f FailurePolicy) String() string {
	if f == FailClosedPolicy {
		return "FailClosed"
	}
	return "Degrade"
}

// ErrUnavailable is returned by a FailClosed operation that would otherwise
// have skipped a failing layer.
var ErrUnavailable = errors.New("cache: layer unavailable")

func (m Mode) String() string {
	return strings.Join([]string{m.Freshness.String(), m.Write.String(), m.Fill.String(), m.Failure.String()}, ", ")
}

// with returns m with opts applied.
func (m Mode) with(opts []ModeOption) Mode {
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&m)
		}
	}
	return m
}

// keySpace is a mode for the keys a pattern matches.
type keySpace struct {
	pattern string
	prefix  bool
	mode    Mode
}

func (k keySpace) matches(key string) bool {
	if k.prefix {
		return strings.HasPrefix(key, k.pattern)
	}
	return key == k.pattern
}
