// Package cachetest is the conformance suite for codefly.dev/cache drivers.
// A driver is correct when Run and RunStack pass against a real server — not a
// fake of one — so the suite is also what a provider service runs to prove it
// serves the interface.
//
// Run holds a layer to its contracts and to what it declares (Capabilities).
// RunStack holds the stack over it: fill-once, partitions, eviction on
// external writes and resyncs, a property test for every mode the layer's
// declaration supports (Modes), every named pitfall (Pitfalls), and a fixed
// seed set of the model-based simulation (Simulation). Simulate runs the
// simulation for any seed, to replay a failure or search longer.
package cachetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// Harness builds the layers under test.
type Harness struct {
	// New returns a layer bound to namespace. For a layer shared between
	// processes (a Leaser or a Notifier), two calls with the same namespace
	// must return independent clients that see the same data, the way two
	// processes would; different namespaces must not see each other's keys.
	New func(t *testing.T, namespace string) cache.Layer

	// ExternalWrite, when set, changes key in namespace straight in the
	// backend, bypassing the driver — the way another program, an operator or
	// the server itself would (for Redis: a plain SET or DEL). The value
	// written does not matter. With it, the suite checks that a Notifier
	// reports changes by any writer, not only those made through a driver.
	ExternalWrite func(t *testing.T, namespace, key string)

	// Interrupt, required when the layer declares Resyncs, breaks l's
	// notification stream the way a dropped connection or a server restart
	// would, so the suite can check that the gap is reported.
	Interrupt func(t *testing.T, l cache.Layer)
}

// Namespace returns a namespace no other test uses.
func Namespace(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "cachetest-" + hex.EncodeToString(b[:])
}

// Run checks the Layer contract, the layer's declaration against its
// implementation, and the contract of every capability it declares: Leases,
// Notifies (with its NoticeBound), Resyncs, Fences and a ByteBudget. A
// Notifier must report other clients' writes, and with ExternalWrite set,
// writes that bypass the driver; a Resyncer must report a gap after Interrupt
// and keep reporting after it.
func Run(t *testing.T, h Harness) {
	caps := cache.CapabilitiesOf(h.New(t, Namespace(t)))
	t.Run("Layer", func(t *testing.T) { runLayer(t, h) })
	t.Run("Declaration", func(t *testing.T) { runDeclaration(t, h) })
	if caps.Leases {
		t.Run("Leaser", func(t *testing.T) { runLeaser(t, h) })
	}
	if caps.Notifies {
		t.Run("Notifier", func(t *testing.T) { runNotifier(t, h) })
	}
	if caps.Fences {
		t.Run("Fencer", func(t *testing.T) { runFencer(t, h) })
	}
}

// runDeclaration holds what the layer declares to what it implements: a
// capability it declares must be backed by its interface, and an interface it
// implements must be declared, or the stack would silently not use it.
func runDeclaration(t *testing.T, h Harness) {
	l := h.New(t, Namespace(t))
	caps := cache.CapabilitiesOf(l)
	if _, err := cache.New(context.Background(), cache.WithTier(l, time.Minute)); err != nil {
		t.Fatalf("a stack refuses the layer's declaration: %v", err)
	}
	for _, c := range []struct {
		name     string
		declared bool
		ok       bool
	}{
		{"Leases (cache.Leaser)", caps.Leases, implements[cache.Leaser](l)},
		{"Notifies (cache.Notifier)", caps.Notifies, implements[cache.Notifier](l)},
		{"Resyncs (cache.Resyncer)", caps.Resyncs, implements[cache.Resyncer](l)},
		{"Flushes (cache.Flusher)", caps.Flushes, implements[cache.Flusher](l)},
		{"Fences (cache.Fencer)", caps.Fences, implements[cache.Fencer](l)},
	} {
		if c.ok && !c.declared {
			t.Errorf("the layer implements %s but does not declare it: the stack will not use it", c.name)
		}
	}
	if caps.Notifies && caps.NoticeBound == 0 {
		t.Log("the layer declares Notifies without a NoticeBound: InvalidateOnWrite will refuse it")
	}

	t.Run("ByteBudget", func(t *testing.T) {
		if caps.ByteBudget <= 0 {
			t.Skip("the layer declares no byte budget")
		}
		// Write three budgets' worth of values; what the layer still holds
		// must fit the budget, counting only keys, values and versions, which
		// is less than any implementation spends.
		value := make([]byte, min(max(64, caps.ByteBudget/64), 64<<10))
		n := int(3*caps.ByteBudget/int64(len(value))) + 1
		for i := range n {
			if err := l.Set(context.Background(), fmt.Sprintf("budget-%d", i), cache.Entry{Value: value, Version: "v"}, time.Minute); err != nil &&
				!errors.Is(err, cache.ErrTooLarge) {
				t.Fatal(err)
			}
		}
		var held int64
		for i := range n {
			key := fmt.Sprintf("budget-%d", i)
			if e, err := l.Get(context.Background(), key); err == nil {
				held += int64(len(key) + len(e.Value) + len(e.Version))
			}
		}
		if held > caps.ByteBudget {
			t.Fatalf("the layer holds %d bytes of keys, values and versions, over its declared budget of %d", held, caps.ByteBudget)
		}
		// The most recent write is kept: eviction is least recently used.
		if _, err := l.Get(context.Background(), fmt.Sprintf("budget-%d", n-1)); err != nil {
			t.Fatalf("the most recent write was evicted: %v", err)
		}
	})
}

func implements[I any](l cache.Layer) bool { _, ok := l.(I); return ok }

// runFencer checks SetFenced: it stores a value of an equal or greater
// sequence, refuses a lower one with ErrFenced and leaves the newer value, and
// revokes a fill lease when it stores.
func runFencer(t *testing.T, h Harness) {
	ctx := context.Background()
	ns := Namespace(t)
	a, b := h.New(t, ns).(cache.Fencer), h.New(t, ns).(cache.Fencer)
	entry := func(v string, seq uint64) cache.Entry { return cache.Entry{Value: []byte(v), Sequence: seq} }
	if err := a.SetFenced(ctx, "k", entry("five", 5), time.Minute); err != nil {
		t.Fatalf("SetFenced on an absent key: %v", err)
	}
	if err := b.SetFenced(ctx, "k", entry("three", 3), time.Minute); !errors.Is(err, cache.ErrFenced) {
		t.Fatalf("SetFenced of an older sequence = %v, want ErrFenced", err)
	}
	if got, err := a.Get(ctx, "k"); err != nil || string(got.Value) != "five" {
		t.Fatalf("after a refused older fill the layer holds %q, %v; want the newer value", got.Value, err)
	}
	if err := b.SetFenced(ctx, "k", entry("five again", 5), time.Minute); err != nil {
		t.Fatalf("SetFenced of an equal sequence: %v", err)
	}
	if err := b.SetFenced(ctx, "k", entry("seven", 7), time.Minute); err != nil {
		t.Fatalf("SetFenced of a newer sequence: %v", err)
	}
	if got, _ := a.Get(ctx, "k"); string(got.Value) != "seven" || got.Sequence != 7 {
		t.Fatalf("the layer holds %q at sequence %d, want seven at 7", got.Value, got.Sequence)
	}
	if l, ok := a.(cache.Leaser); ok && cache.CapabilitiesOf(a).Leases {
		lease := mustAcquire(t, l, "leased", 5*time.Second)
		if err := b.SetFenced(ctx, "leased", entry("fenced write", 1), time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := l.Fill(ctx, lease, entry("stale fill", 1), time.Minute); !errors.Is(err, cache.ErrLeaseLost) {
			t.Fatalf("Fill after a SetFenced that stored = %v, want ErrLeaseLost", err)
		}
	}
}

func runLayer(t *testing.T, h Harness) {
	ctx := context.Background()

	t.Run("MissOnEmpty", func(t *testing.T) {
		l := h.New(t, Namespace(t))
		if _, err := l.Get(ctx, "absent"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("Get(absent) = %v, want ErrMiss", err)
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		l := h.New(t, Namespace(t))
		confirmed := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
		entries := map[string]cache.Entry{
			"binary":              {Value: []byte{0, 1, 2, 0xff, 0}, Version: "v1", Confirmed: confirmed},
			"empty":               {Value: []byte{}, Version: "v2", Sequence: 1<<63 + 7},
			"negative":            {Missing: true, Confirmed: confirmed, Sequence: 3},
			"no-meta":             {Value: []byte("plain")},
			"k:with/odd {chars}}": {Value: []byte("odd key"), Version: `"etag-1"`},
		}
		for key, want := range entries {
			if err := l.Set(ctx, key, want, time.Minute); err != nil {
				t.Fatalf("Set(%q): %v", key, err)
			}
		}
		for key, want := range entries {
			got, err := l.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get(%q): %v", key, err)
			}
			assertEntry(t, key, got, want)
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		l := h.New(t, Namespace(t))
		mustSet(t, l, "k", cache.Entry{Value: []byte("one"), Version: "1"}, time.Minute)
		mustSet(t, l, "k", cache.Entry{Value: []byte("two"), Version: "2"}, time.Minute)
		got, err := l.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		assertEntry(t, "k", got, cache.Entry{Value: []byte("two"), Version: "2"})
	})

	t.Run("Delete", func(t *testing.T) {
		l := h.New(t, Namespace(t))
		mustSet(t, l, "k", cache.Entry{Value: []byte("v")}, time.Minute)
		if err := l.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("Get after Delete = %v, want ErrMiss", err)
		}
		if err := l.Delete(ctx, "never-set"); err != nil {
			t.Fatalf("Delete(absent) = %v, want nil", err)
		}
	})

	t.Run("TTLExpires", func(t *testing.T) {
		l := h.New(t, Namespace(t))
		mustSet(t, l, "k", cache.Entry{Value: []byte("v")}, 150*time.Millisecond)
		if _, err := l.Get(ctx, "k"); err != nil {
			t.Fatalf("Get before expiry: %v", err)
		}
		eventually(t, 3*time.Second, func() bool {
			_, err := l.Get(ctx, "k")
			return errors.Is(err, cache.ErrMiss)
		}, "entry never expired")
	})

	t.Run("ValueIsCopied", func(t *testing.T) {
		l := h.New(t, Namespace(t))
		v := []byte("original")
		mustSet(t, l, "k", cache.Entry{Value: v}, time.Minute)
		v[0] = 'X'
		got, err := l.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Value) != "original" {
			t.Fatalf("stored value changed with the caller's slice: %q", got.Value)
		}
	})

	t.Run("NamespacesIsolated", func(t *testing.T) {
		a, b := h.New(t, Namespace(t)), h.New(t, Namespace(t))
		mustSet(t, a, "k", cache.Entry{Value: []byte("a")}, time.Minute)
		if _, err := b.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("another namespace saw the key: %v", err)
		}
	})
}

func runLeaser(t *testing.T, h Harness) {
	ctx := context.Background()
	pair := func(t *testing.T) (cache.Leaser, cache.Leaser) {
		ns := Namespace(t)
		return h.New(t, ns).(cache.Leaser), h.New(t, ns).(cache.Leaser)
	}

	t.Run("ExactlyOneHolder", func(t *testing.T) {
		a, b := pair(t)
		var granted atomic.Int32
		var wg sync.WaitGroup
		for i := range 32 {
			l := a
			if i%2 == 1 {
				l = b
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, ok, err := l.Acquire(ctx, "k", 5*time.Second)
				if err != nil {
					t.Error(err)
				}
				if ok {
					granted.Add(1)
				}
			}()
		}
		wg.Wait()
		if n := granted.Load(); n != 1 {
			t.Fatalf("%d holders granted the lease, want exactly 1", n)
		}
	})

	t.Run("FillStoresAndReleases", func(t *testing.T) {
		a, b := pair(t)
		lease := mustAcquire(t, a, "k", 5*time.Second)
		if err := a.Fill(ctx, lease, cache.Entry{Value: []byte("filled"), Version: "7"}, time.Minute); err != nil {
			t.Fatalf("Fill: %v", err)
		}
		got, err := b.Get(ctx, "k")
		if err != nil {
			t.Fatalf("other client Get after Fill: %v", err)
		}
		assertEntry(t, "k", got, cache.Entry{Value: []byte("filled"), Version: "7"})
		mustAcquire(t, b, "k", 5*time.Second)
	})

	t.Run("DeleteRevokesLease", func(t *testing.T) {
		a, b := pair(t)
		lease := mustAcquire(t, a, "k", 5*time.Second)
		if err := b.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		err := a.Fill(ctx, lease, cache.Entry{Value: []byte("stale")}, time.Minute)
		if !errors.Is(err, cache.ErrLeaseLost) {
			t.Fatalf("Fill after Delete = %v, want ErrLeaseLost", err)
		}
		if _, err := b.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("a revoked fill was stored: %v", err)
		}
	})

	t.Run("SetRevokesLease", func(t *testing.T) {
		a, b := pair(t)
		lease := mustAcquire(t, a, "k", 5*time.Second)
		mustSet(t, b, "k", cache.Entry{Value: []byte("newer")}, time.Minute)
		err := a.Fill(ctx, lease, cache.Entry{Value: []byte("stale")}, time.Minute)
		if !errors.Is(err, cache.ErrLeaseLost) {
			t.Fatalf("Fill after Set = %v, want ErrLeaseLost", err)
		}
		got, err := b.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Value) != "newer" {
			t.Fatalf("revoked fill overwrote a newer value: %q", got.Value)
		}
	})

	t.Run("LeaseExpires", func(t *testing.T) {
		a, b := pair(t)
		lease := mustAcquire(t, a, "k", 150*time.Millisecond)
		eventually(t, 3*time.Second, func() bool {
			_, ok, err := b.Acquire(ctx, "k", 5*time.Second)
			return err == nil && ok
		}, "lease never expired")
		err := a.Fill(ctx, lease, cache.Entry{Value: []byte("late")}, time.Minute)
		if !errors.Is(err, cache.ErrLeaseLost) {
			t.Fatalf("Fill with an expired lease = %v, want ErrLeaseLost", err)
		}
	})

	t.Run("ReleaseFreesLease", func(t *testing.T) {
		a, b := pair(t)
		lease := mustAcquire(t, a, "k", 5*time.Second)
		if err := a.Release(ctx, lease); err != nil {
			t.Fatal(err)
		}
		mustAcquire(t, b, "k", 5*time.Second)
		if err := a.Release(ctx, lease); err != nil {
			t.Fatalf("releasing a lease someone else now holds: %v", err)
		}
		if _, ok, _ := a.Acquire(ctx, "k", 5*time.Second); ok {
			t.Fatal("a stale Release freed another holder's lease")
		}
	})
}

func runNotifier(t *testing.T, h Harness) {
	ctx := context.Background()

	// Every other client's write is reported. A client may also hear its own
	// writes: the contract allows it, and the stack's eviction is idempotent.
	t.Run("OtherClientsWrites", func(t *testing.T) {
		ns := Namespace(t)
		a, b := h.New(t, ns).(cache.Notifier), h.New(t, ns).(cache.Notifier)
		heardA, heardB := subscribe(t, a), subscribe(t, b)

		mustSet(t, a, "set-by-a", cache.Entry{Value: []byte("v")}, time.Minute)
		if err := a.Delete(ctx, "deleted-by-a"); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, "deleted-by-b"); err != nil {
			t.Fatal(err)
		}
		heardB.wait(t, "set-by-a", "deleted-by-a")
		heardA.wait(t, "deleted-by-b")
	})

	// A notice arrives within the declared NoticeBound of the write that made
	// it; one declared before the write returns has arrived when it does.
	t.Run("NoticeBound", func(t *testing.T) {
		ns := Namespace(t)
		a, b := h.New(t, ns).(cache.Notifier), h.New(t, ns)
		bound := cache.CapabilitiesOf(a).NoticeBound
		if bound == 0 {
			t.Skip("the layer declares no NoticeBound")
		}
		heardA := subscribe(t, a)
		for i := range 20 {
			key := fmt.Sprintf("bounded-%d", i)
			mustSet(t, b, key, cache.Entry{Value: []byte("v")}, time.Minute)
			acked := time.Now()
			if bound == cache.NoticesBeforeReturn {
				if !heardA.has(key) {
					t.Fatalf("the layer declares NoticesBeforeReturn, but %q was not reported when its write returned", key)
				}
				continue
			}
			for !heardA.has(key) {
				if time.Since(acked) > bound {
					t.Fatalf("the notice of %q took longer than the declared NoticeBound %s", key, bound)
				}
				time.Sleep(time.Millisecond)
			}
		}
	})

	t.Run("ExternalWrites", func(t *testing.T) {
		if h.ExternalWrite == nil {
			t.Skip("the harness has no ExternalWrite")
		}
		ns := Namespace(t)
		heard := subscribe(t, h.New(t, ns).(cache.Notifier))
		h.ExternalWrite(t, ns, "written-externally")
		heard.wait(t, "written-externally")
	})

	t.Run("Resync", func(t *testing.T) {
		ns := Namespace(t)
		l := h.New(t, ns)
		if !cache.CapabilitiesOf(l).Resyncs {
			t.Skip("the layer does not declare Resyncs: it must never lose a notice")
		}
		if h.Interrupt == nil {
			t.Fatal("the layer declares Resyncs, so the harness must set Interrupt to prove it reports a gap")
		}
		var mu sync.Mutex
		var events []string
		stop, err := l.(cache.Resyncer).SubscribeGaps(ctx,
			func() { mu.Lock(); events = append(events, "lost"); mu.Unlock() },
			func() { mu.Lock(); events = append(events, "resynced"); mu.Unlock() })
		if err != nil {
			t.Fatal(err)
		}
		defer stop()
		heard := subscribe(t, l.(cache.Notifier))
		bound := cache.CapabilitiesOf(l).NoticeBound
		interrupted := time.Now()
		h.Interrupt(t, l)
		eventually(t, 10*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(events) > 0
		}, "an interrupted Resyncer never reported the gap")
		if lostAfter := time.Since(interrupted); bound > 0 && lostAfter > bound+time.Second {
			t.Errorf("the gap was reported %s after the interruption, beyond the declared NoticeBound %s", lostAfter, bound)
		}
		eventually(t, 10*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(events) >= 2
		}, "an interrupted Resyncer never reported that notices flow again")
		mu.Lock()
		first, second := events[0], events[1]
		mu.Unlock()
		if first != "lost" || second != "resynced" {
			t.Fatalf("gap events %q, %q; want lost, then resynced", first, second)
		}
		// Notices flow again after the signal.
		other := h.New(t, ns)
		mustSet(t, other, "after-the-gap", cache.Entry{Value: []byte("v")}, time.Minute)
		heard.wait(t, "after-the-gap")
	})
}

// heard collects the keys a Notifier reports.
type heard struct {
	mu   sync.Mutex
	keys map[string]bool
}

func subscribe(t *testing.T, n cache.Notifier) *heard {
	t.Helper()
	h := &heard{keys: map[string]bool{}}
	stop, err := n.Subscribe(context.Background(), func(key string) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.keys[key] = true
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return h
}

func (h *heard) has(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keys[key]
}

func (h *heard) wait(t *testing.T, keys ...string) {
	t.Helper()
	eventually(t, 3*time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, key := range keys {
			if !h.keys[key] {
				return false
			}
		}
		return true
	}, fmt.Sprintf("the Notifier never reported all of %q", keys))
}
