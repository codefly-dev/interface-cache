package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Capabilities is what a layer or an origin declares it can do beyond its base
// contract (Layer, or Source). A stack checks every mode it is asked for
// against them, and refuses at construction — naming the missing capability —
// a mode whose guarantee they cannot keep.
//
// The declaration is what counts: a layer that implements Leaser but does not
// declare Leases grants no leases to the stack, and New refuses a layer that
// declares a capability without implementing the interface behind it. A
// layer or origin that declares nothing (no Capabilities method) has none.
// The conformance suite in cachetest holds each declaration to its behaviour.
type Capabilities struct {
	// Leases: the layer is a Leaser. Needed by FillLease.
	Leases bool
	// Notifies: the layer is a Notifier, reporting every change by any
	// writer.
	Notifies bool
	// NoticeBound, with Notifies: every change is reported within this long
	// of the write being acknowledged, or the layer, a Resyncer, reports the
	// gap as lost within it. Zero declares no bound, which InvalidateOnWrite
	// refuses. A layer that reports a change before the writing call
	// returns, as Memory does, declares NoticesBeforeReturn.
	NoticeBound time.Duration
	// Resyncs: the layer is a Resyncer — it can lose notices, and reports each
	// gap.
	Resyncs bool
	// Flushes: the layer is a Flusher. Needed above a Resyncer.
	Flushes bool
	// Fences: the layer is a Fencer, storing a fill only when it holds no
	// newer version. Needed by FillVersionFenced in every tier.
	Fences bool
	// ByteBudget is the most bytes the layer holds, keys, values and
	// versions counted; it evicts to stay under it. Zero declares none.
	ByteBudget int64

	// ConditionalLoads: the origin honours Load's ifNotVersion, answering
	// ErrNotModified when the version is current. Needed by Validated.
	ConditionalLoads bool
	// Sequenced: every entry the origin loads carries a Sequence that grows
	// with each write of its key, deletes included (a deleted key loads as a
	// Missing entry carrying the delete's sequence). Needed by
	// FillVersionFenced.
	Sequenced bool
	// ChangeFeed: the origin is a ChangeFeed, reporting keys written around
	// the stack.
	ChangeFeed bool
}

// NoticesBeforeReturn is the NoticeBound of a Notifier that reports a change
// to every other subscriber before the write that made it returns.
const NoticesBeforeReturn time.Duration = -1

// Declarer is a layer or an origin that declares its capabilities.
type Declarer interface {
	Capabilities() Capabilities
}

// CapabilitiesOf returns what v declares: nothing when it is not a Declarer.
func CapabilitiesOf(v any) Capabilities {
	if d, ok := v.(Declarer); ok {
		return d.Capabilities()
	}
	return Capabilities{}
}

// Fencer is a Layer that stores a fill only when it would not replace a newer
// version of the value.
type Fencer interface {
	Layer
	// SetFenced stores e for key as Set does, unless key holds an entry whose
	// Sequence is greater than e.Sequence; then it stores nothing and returns
	// ErrFenced. Like Set, it revokes any fill lease on key when it stores.
	SetFenced(ctx context.Context, key string, e Entry, ttl time.Duration) error
}

// ErrFenced is returned by Fencer.SetFenced when the layer holds a newer
// version than the one offered.
var ErrFenced = errors.New("cache: layer holds a newer version")

// ChangeFeed is an origin that reports keys changed by writes that did not go
// through a stack. The stack replaces the key's generation on each, as
// Stack.Invalidate does, so InvalidateOnWrite also covers those writes.
type ChangeFeed interface {
	Source
	// SubscribeChanges calls fn with each key written, until stop is called.
	SubscribeChanges(ctx context.Context, fn func(key string)) (stop func(), err error)
}

// checkDeclared refuses a declaration its implementation does not back.
func checkDeclared(what string, v any, c Capabilities) error {
	var missing []string
	need := func(declared bool, name string, ok bool) {
		if declared && !ok {
			missing = append(missing, fmt.Sprintf("declares %s but does not implement cache.%s", capabilityOf(name), name))
		}
	}
	_, leaser := v.(Leaser)
	_, notifier := v.(Notifier)
	_, resyncer := v.(Resyncer)
	_, flusher := v.(Flusher)
	_, fencer := v.(Fencer)
	_, feed := v.(ChangeFeed)
	need(c.Leases, "Leaser", leaser)
	need(c.Notifies, "Notifier", notifier)
	need(c.Resyncs, "Resyncer", resyncer)
	need(c.Flushes, "Flusher", flusher)
	need(c.Fences, "Fencer", fencer)
	need(c.ChangeFeed, "ChangeFeed", feed)
	if c.Resyncs && !c.Notifies {
		missing = append(missing, "declares Resyncs without Notifies")
	}
	if c.NoticeBound != 0 && !c.Notifies {
		missing = append(missing, "declares a NoticeBound without Notifies")
	}
	if len(missing) > 0 {
		return fmt.Errorf("cache: %s (%T) %s", what, v, strings.Join(missing, "; "))
	}
	return nil
}

func capabilityOf(iface string) string {
	switch iface {
	case "Leaser":
		return "Leases"
	case "Notifier":
		return "Notifies"
	case "Resyncer":
		return "Resyncs"
	case "Flusher":
		return "Flushes"
	case "Fencer":
		return "Fences"
	}
	return iface
}

// supports reports why the stack cannot keep m, or nil. Every refusal names
// the missing capability and where it is missing.
func (s *Stack) supports(m Mode) error {
	var problems []string
	refuse := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	originName := func() string { return fmt.Sprintf("the origin (%T)", s.origin) }

	switch m.Freshness.kind {
	case freshTTL:
		if m.Freshness.bound <= 0 {
			refuse("TTLBounded needs a positive bound, got %s", m.Freshness.bound)
		}
	case freshSWR:
		if m.Freshness.bound <= 0 {
			refuse("StaleWhileRevalidate needs a positive window, got %s", m.Freshness.bound)
		}
		if s.origin == nil {
			refuse("StaleWhileRevalidate needs an origin to revalidate from; the stack has none")
		}
	case freshInvalidate:
		for i := range len(s.tiers) - 1 {
			if s.tiers[i].notifier < 0 {
				refuse("InvalidateOnWrite needs a layer below tier %d (%T) that declares Notifies; there is none, so a write through another stack would never evict its copies",
					i, s.tiers[i].layer)
				continue
			}
			if bound := s.tiers[s.tiers[i].notifier].caps.NoticeBound; bound == 0 {
				n := s.tiers[i].notifier
				refuse("InvalidateOnWrite needs tier %d (%T), which notifies tier %d, to declare a NoticeBound; it declares none, so its copies above have no staleness bound",
					n, s.tiers[n].layer, i)
			}
		}
		if m.Write == PropagateOriginOnly {
			refuse("InvalidateOnWrite cannot be kept with WriteOriginOnly: those writes invalidate nothing")
		}
	case freshRYW:
		if s.origin != nil {
			if _, ok := s.origin.(Store); !ok {
				refuse("ReadYourWrites needs %s to be a Store: the process cannot write through the stack", originName())
			}
		}
	case freshValidated:
		switch {
		case s.origin == nil:
			refuse("Validated needs an origin to validate against; the stack has none")
		case !s.originCaps.ConditionalLoads:
			refuse("Validated needs %s to declare ConditionalLoads; it does not, so every read would reload the value", originName())
		}
	case freshBypass:
		if s.origin == nil {
			refuse("Bypass needs an origin to read; the stack has none")
		}
	}

	switch m.Write {
	case PropagateOriginOnly:
		if s.origin == nil {
			refuse("WriteOriginOnly needs an origin; the stack has none")
		}
	case PropagateBehind:
		if _, ok := s.origin.(Store); !ok {
			refuse("WriteBehind needs a Store origin to write to in the background; %s", describeOrigin(s.origin))
		}
	}

	switch m.Fill {
	case CoordinateLease:
		if !s.declaresLeases() {
			refuse("FillLease needs a tier that declares Leases; none does")
		}
	case CoordinateVersionFenced:
		if s.origin == nil {
			refuse("FillVersionFenced needs an origin whose entries carry sequences; the stack has none")
		} else if !s.originCaps.Sequenced {
			refuse("FillVersionFenced needs %s to declare Sequenced; it does not, so no fill can be ordered against another", originName())
		}
		for i, t := range s.tiers {
			if !t.caps.Fences {
				refuse("FillVersionFenced needs every tier to declare Fences; tier %d (%T) does not", i, t.layer)
			}
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s: %s", ErrUnsupportedMode, m, strings.Join(problems, "; "))
}

// declaresLeases reports whether any tier declares Leases, whatever its state.
func (s *Stack) declaresLeases() bool {
	for _, t := range s.tiers {
		if t.caps.Leases {
			return true
		}
	}
	return false
}

func describeOrigin(o Source) string {
	if o == nil {
		return "the stack has none"
	}
	return fmt.Sprintf("the origin (%T) is read-only", o)
}
