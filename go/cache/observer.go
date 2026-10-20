package cache

import (
	"errors"
	"time"
)

// Event is one thing a stack did at one tier or at the origin, reported to
// WithObserver as it happens: which tier answered a read, whether this
// process loaded the origin or waited for another process's fill, a tier
// skipped behind its breaker, a tier that failed where another was merely
// empty, and what a write reached.
type Event struct {
	// Op is the operation the event belongs to.
	Op Operation
	// Key is the caller's key.
	Key string
	// LayerKey is the key as the tier saw it — schema, partition and
	// generation included — or the key's generation key when a write
	// replaced the generation. Empty for a write reaching the origin.
	LayerKey string
	// Tier is the index of the tier, top first, or Origin.
	Tier int
	// Outcome is what happened there.
	Outcome Outcome
	// Err is what failed, for Failed.
	Err error
	// Elapsed is how long the call took: the tier's, the origin's, or the
	// wait for another process's fill. Zero for a skipped tier.
	Elapsed time.Duration
}

// Origin is the Tier of an event about the origin.
const Origin = -1

// Operation is the stack operation an Event belongs to.
type Operation int

const (
	// OpGet is Get, GetEntry or a Typed read.
	OpGet Operation = iota
	// OpRefresh is a StaleWhileRevalidate refresh, in the background.
	OpRefresh
	// OpSet is Set, including a WriteBehind write of it reaching the origin.
	OpSet
	// OpDelete is Delete, including a WriteBehind delete of it reaching the
	// origin.
	OpDelete
	// OpInvalidate is Invalidate, and an invalidation from the origin's
	// change feed.
	OpInvalidate
)

func (o Operation) String() string {
	switch o {
	case OpRefresh:
		return "Refresh"
	case OpSet:
		return "Set"
	case OpDelete:
		return "Delete"
	case OpInvalidate:
		return "Invalidate"
	}
	return "Get"
}

// Outcome is what happened at one tier or at the origin.
type Outcome int

const (
	// Hit: the tier held a copy the read used.
	Hit Outcome = iota
	// Stale: the tier held a copy too old for the mode; the read went on
	// below it.
	Stale
	// Miss: the tier held nothing.
	Miss
	// SkippedBreaker: the tier was not consulted, its breaker open after
	// failures (WithBreaker).
	SkippedBreaker
	// SkippedGap: the tier was not consulted, a notifier below it in a gap.
	SkippedGap
	// SkippedQuarantine: the tier was not consulted, an invalidation it
	// missed not yet replayed there.
	SkippedQuarantine
	// Failed: the tier or the origin returned an error, in Err.
	Failed
	// Loaded: this process loaded the value from the origin.
	Loaded
	// NotModified: the origin confirmed the copy unchanged, on a conditional
	// load.
	NotModified
	// NotFound: the origin reported the key absent.
	NotFound
	// Waited: another process held the fill lease; this one waited for its
	// fill and got it.
	Waited
	// Written: the origin took the write, or the tier took the value or the
	// key's new generation.
	Written
)

func (o Outcome) String() string {
	switch o {
	case Stale:
		return "Stale"
	case Miss:
		return "Miss"
	case SkippedBreaker:
		return "SkippedBreaker"
	case SkippedGap:
		return "SkippedGap"
	case SkippedQuarantine:
		return "SkippedQuarantine"
	case Failed:
		return "Failed"
	case Loaded:
		return "Loaded"
	case NotModified:
		return "NotModified"
	case NotFound:
		return "NotFound"
	case Waited:
		return "Waited"
	case Written:
		return "Written"
	}
	return "Hit"
}

// WithObserver reports every Event to fn as it happens, on the goroutine
// doing the operation: fn must be quick, must not block, and must not call
// the stack. Nothing is reported for the stack's own bookkeeping — the
// generation reads that precede every read over an origin. Keys are passed as
// they are; what to aggregate on, and at what cardinality, is fn's decision.
// Default: nothing is reported, and nothing is spent.
func WithObserver(fn func(Event)) Option {
	return func(s *Stack) error {
		if fn == nil {
			return errors.New("cache: WithObserver: nil observer")
		}
		s.observer = fn
		return nil
	}
}

// observe reports what happened to f at tier, with the time elapsed since
// start when start is set. A generation fill is bookkeeping: not reported.
func (s *Stack) observe(f fill, tier int, outcome Outcome, err error, start time.Time) {
	if s.observer == nil || f.generation {
		return
	}
	s.report(Event{Op: f.kind, Key: keyOf(f.layer), LayerKey: f.layer, Tier: tier, Outcome: outcome, Err: err}, start)
}

// report sends ev to the observer, with Elapsed measured from start when
// start is set.
//
// A panicking observer is contained here. Every event is reported on the
// goroutine doing the operation, so without this an observer that panics —
// a nil map written to, a type assertion on an Err it did not expect — unwinds
// out through the caller's Get, turning a read that the cache answered
// correctly into a failed request. Telemetry is not worth a read, so the event
// is dropped and the operation goes on.
//
// The observer is NOT disabled afterwards: a panic from a transient condition
// would then cost every later event too, which is a worse failure than the one
// being contained. A consumer that wants to know installs WithObserverErrors;
// with none, the panic is discarded, because this package has no logger to put
// it in. A handler that panics in turn is contained the same way, so one broken
// callback cannot reach the caller through the other.
func (s *Stack) report(ev Event, start time.Time) {
	if s.observer == nil {
		return
	}
	if !start.IsZero() {
		ev.Elapsed = s.clock.Now().Sub(start)
	}
	defer func() {
		panicked := recover()
		if panicked == nil || s.observerErrors == nil {
			return
		}
		defer func() { _ = recover() }()
		s.observerErrors(ev, panicked)
	}()
	s.observer(ev)
}

// timing returns now when an observer wants elapsed times, and the zero time
// — nothing read from the clock — when none does.
func (s *Stack) timing() time.Time {
	if s.observer == nil {
		return time.Time{}
	}
	return s.clock.Now()
}

// written is the outcome of a write that returned err.
func written(err error) Outcome {
	if err != nil {
		return Failed
	}
	return Written
}

// opOf is the operation of a write: a delete or a set.
func opOf(del bool) Operation {
	if del {
		return OpDelete
	}
	return OpSet
}
