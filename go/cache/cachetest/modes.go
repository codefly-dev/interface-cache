package cachetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// RunModes checks every mode's guarantee as a property, over two processes
// that each hold a Memory tier above the layer under test. A mode that needs a
// capability the layer does not declare is checked to be refused at
// construction, naming that capability, and not run.
//
// Every stack reads time from a clock the test moves, so a bound is checked
// at its edge, not by sleeping.
func RunModes(t *testing.T, h Harness) {
	caps := cache.CapabilitiesOf(h.New(t, Namespace(t)))
	t.Run("Freshness", func(t *testing.T) { runFreshness(t, h, caps) })
	t.Run("Write", func(t *testing.T) { runWrites(t, h) })
	t.Run("Fill", func(t *testing.T) { runFills(t, h, caps) })
	t.Run("Failure", func(t *testing.T) { runFailures(t, h) })
	t.Run("Refusals", func(t *testing.T) { runRefusals(t, h, caps) })
}

// modeWorld is two processes, a and b, over one namespace of the layer under
// test and one origin, on one clock.
type modeWorld struct {
	origin *store
	clock  *fakeClock
	tasks  *tasks
	ns     string
	h      Harness
}

func newModeWorld(t *testing.T, h Harness) *modeWorld {
	clock := newFakeClock()
	clock.real = true
	return &modeWorld{origin: newStore(), clock: clock, tasks: &tasks{}, ns: Namespace(t), h: h}
}

// process builds a stack over a new client of the namespace; the returned
// recorder counts the calls it makes on the layer under test.
func (w *modeWorld) process(t *testing.T, opts ...cache.Option) (*cache.Stack, *recorder, *cache.Memory) {
	t.Helper()
	shared, rec := record(w.h.New(t, w.ns))
	top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
	return stackOf(t, top, shared, w.origin, w.clock, w.tasks.run, opts...), rec, top
}

var tenantU = cache.NewPartition("tenant-u")

func runFreshness(t *testing.T, h Harness, caps cache.Capabilities) {
	// TTLBounded(d) never serves a value confirmed more than d ago: a write
	// around the stack is served until d has passed since the load, and not
	// once it has.
	t.Run("TTLBounded", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t, cache.WithMode(cache.TTLBounded(10*time.Second)))
		e, err := a.GetEntry(context.Background(), tenantU, "k")
		if err != nil {
			t.Fatal(err)
		}
		if !e.Confirmed.Equal(w.clock.Now()) {
			t.Fatalf("a load is confirmed at %s, want the start of the load, %s", e.Confirmed, w.clock.Now())
		}
		w.origin.put("k", "two")
		w.clock.Advance(10*time.Second - time.Millisecond)
		wantString(t, a, tenantU, "k", "one")
		w.clock.Advance(time.Millisecond)
		wantString(t, a, tenantU, "k", "two")
	})

	// StaleWhileRevalidate(d) serves a copy past the TTL bound, up to d past
	// it, while one background refresh runs; past bound+d it reloads first.
	t.Run("StaleWhileRevalidate", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t, cache.WithMode(cache.StaleWhileRevalidate(30*time.Second)))
		wantString(t, a, tenantU, "k", "one")
		w.origin.put("k", "two")
		w.clock.Advance(time.Minute + time.Second) // past the bound (the tier TTL)
		loads := w.origin.loadCount()
		wantString(t, a, tenantU, "k", "one")
		wantString(t, a, tenantU, "k", "one")
		if w.origin.loadCount() != loads {
			t.Fatal("a stale-while-revalidate read waited on the origin")
		}
		if w.tasks.len() == 0 {
			t.Fatal("a stale read scheduled no refresh")
		}
		w.tasks.runAll()
		if n := w.origin.loadCount() - loads; n != 1 {
			t.Fatalf("two stale reads refreshed %d times, want once", n)
		}
		wantString(t, a, tenantU, "k", "two")
		w.origin.put("k", "three")
		w.clock.Advance(time.Minute + 31*time.Second) // past bound + window
		wantString(t, a, tenantU, "k", "three")
	})

	// InvalidateOnWrite serves a copy at any age until a write through any
	// stack invalidates it.
	t.Run("InvalidateOnWrite", func(t *testing.T) {
		if !caps.Notifies || caps.NoticeBound == 0 {
			t.Skip("needs the layer to declare Notifies with a NoticeBound (Refusals checks the refusal)")
		}
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t, cache.WithMode(cache.InvalidateOnWrite()))
		b, _, _ := w.process(t, cache.WithMode(cache.InvalidateOnWrite()))
		wantString(t, a, tenantU, "k", "one")
		w.clock.Advance(100 * time.Hour)
		loads := w.origin.loadCount()
		wantString(t, a, tenantU, "k", "one")
		if w.origin.loadCount() != loads {
			t.Fatal("InvalidateOnWrite reloaded a copy no write invalidated")
		}
		if err := b.Set(context.Background(), tenantU, "k", []byte("two")); err != nil {
			t.Fatal(err)
		}
		within := time.Duration(caps.NoticeBound)
		if within < 0 {
			wantString(t, a, tenantU, "k", "two") // delivered before the write returned
			return
		}
		eventually(t, within+time.Second, func() bool { return getString(t, a, tenantU, "k") == "two" },
			"a write through another stack was still unseen past the declared NoticeBound")
	})

	// ReadYourWrites: a process reads its own acknowledged write even while
	// its layers hold an older copy, here left by a write that went around
	// them.
	t.Run("ReadYourWrites", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t)
		b, _, _ := w.process(t)
		wantString(t, a, tenantU, "k", "one")
		wantString(t, b, tenantU, "k", "one")
		if err := a.Set(context.Background(), tenantU, "k", []byte("two"), cache.WriteOriginOnly()); err != nil {
			t.Fatal(err)
		}
		wantString(t, a, tenantU, "k", "one") // the default mode serves the older copy...
		wantString(t, a, tenantU, "k", "two", cache.ReadYourWrites())
		wantString(t, a, tenantU, "k", "two", cache.ReadYourWrites()) // ...this mode never does, and now holds it
		if got := getString(t, b, tenantU, "k", cache.ReadYourWrites()); got != "one" && got != "two" {
			t.Fatalf("b read %q", got) // not b's write: either, bounded by the TTL
		}
		if err := a.Delete(context.Background(), tenantU, "k", cache.WriteOriginOnly()); err != nil {
			t.Fatal(err)
		}
		wantString(t, a, tenantU, "k", "<not found>", cache.ReadYourWrites())
	})

	// Validated never serves a version the origin had superseded, and pays a
	// conditional load, not a transfer, when nothing changed.
	t.Run("Validated", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t)
		wantString(t, a, tenantU, "k", "one")
		w.origin.put("k", "two")
		wantString(t, a, tenantU, "k", "one") // the default mode: the TTL bound
		wantString(t, a, tenantU, "k", "two", cache.Validated())
		before := w.origin.notModifiedCount()
		wantString(t, a, tenantU, "k", "two", cache.Validated())
		if w.origin.notModifiedCount() != before+1 {
			t.Fatal("a validated read of an unchanged value did not revalidate conditionally")
		}
		w.origin.remove("k")
		wantString(t, a, tenantU, "k", "<not found>", cache.Validated())
	})

	// Bypass reads the origin alone, touching no layer.
	t.Run("Bypass", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, rec, top := w.process(t)
		wantString(t, a, tenantU, "k", "one")
		w.origin.put("k", "two")
		rec.reset()
		entries := top.Len()
		wantString(t, a, tenantU, "k", "two", cache.Bypass())
		if rec.calls() != 0 || top.Len() != entries {
			t.Fatalf("a bypass read made %d calls on the shared layer and changed the Memory tier", rec.calls())
		}
	})
}

func runWrites(t *testing.T, h Harness) {
	ctx := context.Background()
	notifies := cache.CapabilitiesOf(h.New(t, Namespace(t))).Notifies

	// WriteInvalidate: the writer reads its write at once; another process
	// once the notice lands (or, without notices, after its TTL bound).
	t.Run("Invalidate", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t)
		b, _, _ := w.process(t)
		wantString(t, a, tenantU, "k", "one")
		wantString(t, b, tenantU, "k", "one")
		if err := a.Set(ctx, tenantU, "k", []byte("two")); err != nil {
			t.Fatal(err)
		}
		wantString(t, a, tenantU, "k", "two")
		if !notifies {
			w.clock.Advance(time.Minute)
		}
		eventually(t, 5*time.Second, func() bool { return getString(t, b, tenantU, "k") == "two" },
			"another process never read the write")
	})

	// WriteThrough stores the write as the writer's copy: its next read does
	// not reach the origin.
	t.Run("Through", func(t *testing.T) {
		w := newModeWorld(t, h)
		a, _, _ := w.process(t, cache.WithMode(cache.WriteThrough()))
		if err := a.Set(ctx, tenantU, "k", []byte("two")); err != nil {
			t.Fatal(err)
		}
		loads := w.origin.loadCount()
		wantString(t, a, tenantU, "k", "two")
		if w.origin.loadCount() != loads {
			t.Fatal("a read after a write-through reached the origin")
		}
	})

	// WriteOriginOnly touches no layer; copies catch up by their freshness.
	t.Run("OriginOnly", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, rec, _ := w.process(t, cache.WithMode(cache.WriteOriginOnly(), cache.TTLBounded(10*time.Second)))
		wantString(t, a, tenantU, "k", "one")
		rec.reset()
		if err := a.Set(ctx, tenantU, "k", []byte("two")); err != nil {
			t.Fatal(err)
		}
		if rec.calls() != 0 {
			t.Fatalf("a write around the layers made %d calls on the shared layer", rec.calls())
		}
		if w.origin.current("k").value != "two" {
			t.Fatal("the write did not reach the origin")
		}
		wantString(t, a, tenantU, "k", "one")
		wantString(t, a, tenantU, "k", "two", cache.Validated())
	})

	// WriteBehind acknowledges before the origin has the write: the writer
	// reads it in its partition, everyone else reads the origin, until the
	// background write lands. Closing without draining loses it.
	t.Run("Behind", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		behind := cache.WithMode(cache.WriteBehind(cache.AcceptWriteLoss))
		a, _, _ := w.process(t, behind)
		b, _, _ := w.process(t, behind)
		other := cache.NewPartition("tenant-v")
		wantString(t, a, other, "k", "one")
		if err := a.Set(ctx, tenantU, "k", []byte("two")); err != nil {
			t.Fatal(err)
		}
		if w.origin.current("k").value != "one" {
			t.Fatal("a write-behind reached the origin before its background write ran")
		}
		wantString(t, a, tenantU, "k", "two")
		wantString(t, a, other, "k", "one")
		wantString(t, b, tenantU, "k", "one")
		w.tasks.runAll()
		if err := a.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if w.origin.current("k").value != "two" {
			t.Fatal("the drained write never reached the origin")
		}
		wantString(t, a, other, "k", "two") // the landing replaced the generation
		if !notifies {
			w.clock.Advance(time.Minute)
		}
		eventually(t, 5*time.Second, func() bool { return getString(t, b, tenantU, "k") == "two" },
			"another process never read the landed write")

		// A process that dies — closes without draining — loses its queue.
		c, _, _ := w.process(t, behind)
		if err := c.Set(ctx, tenantU, "k", []byte("lost")); err != nil {
			t.Fatal(err)
		}
		c.Close()
		w.tasks.runAll()
		if got := w.origin.current("k").value; got != "two" {
			t.Fatalf("the origin holds %q after the writer closed without draining, want the write lost", got)
		}
	})
}

// stampede starts n concurrent reads of one missing key in each stack, holds
// the origin until n*len(stacks) loads have started or wait has passed, and
// returns how many loads there were.
func stampede(t *testing.T, w *modeWorld, stacks []*cache.Stack, n int, wait time.Duration) int {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	started := make(chan struct{}, n*len(stacks))
	w.origin.setOnLoad(func(context.Context, string) {
		started <- struct{}{}
		<-release
	})
	defer w.origin.setOnLoad(nil)
	w.origin.put("hot", "v")
	loads := w.origin.loadCount()
	var wg sync.WaitGroup
	for _, s := range stacks {
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if got, err := s.Get(context.Background(), tenantU, "hot"); err != nil || string(got) != "v" {
					t.Errorf("a stampeding read = %q, %v", got, err)
				}
			}()
		}
	}
	deadline := time.After(wait)
	for count := 0; count < n*len(stacks); count++ {
		select {
		case <-started:
			continue
		case <-deadline:
		}
		break
	}
	once.Do(func() { close(release) })
	wg.Wait()
	return w.origin.loadCount() - loads
}

func runFills(t *testing.T, h Harness, caps cache.Capabilities) {
	t.Run("Uncoordinated", func(t *testing.T) {
		w := newModeWorld(t, h)
		a, _, _ := w.process(t, cache.WithMode(cache.FillUncoordinated()))
		if n := stampede(t, w, []*cache.Stack{a}, 8, 5*time.Second); n != 8 {
			t.Fatalf("8 uncoordinated misses loaded the origin %d times, want 8", n)
		}
	})
	t.Run("Singleflight", func(t *testing.T) {
		w := newModeWorld(t, h)
		a, _, _ := w.process(t, cache.WithMode(cache.FillSingleflight()))
		b, _, _ := w.process(t, cache.WithMode(cache.FillSingleflight()))
		if n := stampede(t, w, []*cache.Stack{a, b}, 16, 300*time.Millisecond); n < 1 || n > 2 {
			t.Fatalf("two processes' misses loaded the origin %d times, want at most once per process", n)
		}
	})
	t.Run("Lease", func(t *testing.T) {
		if !caps.Leases {
			t.Skip("needs the layer to declare Leases (Refusals checks the refusal)")
		}
		w := newModeWorld(t, h)
		a, _, _ := w.process(t, cache.WithMode(cache.FillLease()))
		b, _, _ := w.process(t, cache.WithMode(cache.FillLease()))
		if n := stampede(t, w, []*cache.Stack{a, b}, 16, 300*time.Millisecond); n != 1 {
			t.Fatalf("two processes' leased misses loaded the origin %d times, want once", n)
		}
	})
	// VersionFenced: a slow load of an older version, finishing after a newer
	// one was stored under the same generation — a write the origin received
	// around the stack — does not replace it. Without fencing it does.
	t.Run("VersionFenced", func(t *testing.T) {
		if !caps.Fences {
			t.Skip("needs the layer to declare Fences (Refusals checks the refusal)")
		}
		for _, c := range []struct {
			name   string
			fill   cache.FillCoordination
			stored string
		}{
			{"fenced", cache.FillVersionFenced(), "two"},
			{"unfenced", cache.FillSingleflight(), "one"},
		} {
			t.Run(c.name, func(t *testing.T) {
				if got := staleSet(t, h, c.fill); got != c.stored {
					t.Fatalf("the shared layer holds %q after a slow older fill, want %q", got, c.stored)
				}
			})
		}
	})
}

// staleSet runs the stale-set race and returns what a fresh process reads
// from the shared layer afterwards: process a loads "one" slowly; meanwhile
// the origin takes "two" around the stack and process b validates and stores
// it under the same generation; then a's load returns and stores.
func staleSet(t *testing.T, h Harness, fill cache.FillCoordination) string {
	t.Helper()
	w := newModeWorld(t, h)
	w.origin.put("k", "one")
	a, _, _ := w.process(t, cache.WithMode(fill))
	b, _, _ := w.process(t, cache.WithMode(fill))
	// Both processes learn the generation first, so both copies go under it.
	if _, err := a.Get(context.Background(), cache.NewPartition("warm"), "k"); err != nil {
		t.Fatal(err)
	}
	wantString(t, b, cache.NewPartition("warm"), "k", "one")

	inLoad, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	w.origin.setOnLoad(func(context.Context, string) { // the next load is a's
		once.Do(func() { close(inLoad); <-resume })
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		wantString(t, a, tenantU, "k", "one") // a's load read "one" before the write
	}()
	<-inLoad
	w.origin.put("k", "two")
	w.origin.setOnLoad(nil)
	wantString(t, b, tenantU, "k", "two", cache.Validated())
	close(resume)
	<-done
	c, _, _ := w.process(t, cache.WithMode(cache.TTLBounded(time.Hour)))
	return getString(t, c, tenantU, "k")
}

func runFailures(t *testing.T, h Harness) {
	ctx := context.Background()
	failingWorld := func(t *testing.T, opts ...cache.Option) (*modeWorld, *cache.Stack, *faulty) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		shared := newFaulty(h.New(t, w.ns))
		top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
		return w, stackOf(t, top, shared, w.origin, w.clock, w.tasks.run, opts...), shared
	}
	// Degrade: a failing layer is skipped; the read is served from the origin.
	t.Run("Degrade", func(t *testing.T) {
		_, s, shared := failingWorld(t)
		shared.failing.Store(true)
		wantString(t, s, tenantU, "k", "one")
		if err := s.Set(ctx, tenantU, "k", []byte("two")); !errors.Is(err, cache.ErrPartialWrite) {
			t.Fatalf("a write with the shared layer down = %v, want ErrPartialWrite", err)
		}
		wantString(t, s, tenantU, "k", "two")
	})
	// FailClosed: a read that cannot consult every tier fails, and a write
	// fails before touching the origin.
	t.Run("FailClosed", func(t *testing.T) {
		w, s, shared := failingWorld(t, cache.WithMode(cache.FailClosed()))
		wantString(t, s, tenantU, "k", "one")
		shared.failing.Store(true)
		// The Memory tier still holds the copy: a FailClosed read fails only
		// when it must consult the failing tier.
		if _, err := s.Get(ctx, tenantU, "k2"); !errors.Is(err, cache.ErrUnavailable) {
			t.Fatalf("a FailClosed read past a failing tier = %v, want ErrUnavailable", err)
		}
		seq := w.origin.current("k").seq
		if err := s.Set(ctx, tenantU, "k", []byte("two")); !errors.Is(err, cache.ErrUnavailable) {
			t.Fatalf("a FailClosed write with a failing tier = %v, want ErrUnavailable", err)
		}
		if w.origin.current("k").seq != seq {
			t.Fatal("a FailClosed write that failed changed the origin")
		}
		// The same read under Degrade is served.
		wantString(t, s, tenantU, "k2", "<not found>", cache.Degrade())
	})
}

// runRefusals checks that a mode needing a capability the layer lacks is
// refused at construction, naming it, and that one it has is accepted.
func runRefusals(t *testing.T, h Harness, caps cache.Capabilities) {
	ns := Namespace(t)
	top := cache.NewMemory()
	for _, c := range []struct {
		name     string
		declared bool
		opts     []cache.Option
		names    []string
	}{
		{"FillLease", caps.Leases, []cache.Option{cache.WithMode(cache.FillLease())}, []string{"FillLease", "Leases"}},
		{"FillVersionFenced", caps.Fences, []cache.Option{cache.WithMode(cache.FillVersionFenced())}, []string{"FillVersionFenced", "Fences"}},
		{"InvalidateOnWrite", caps.Notifies && caps.NoticeBound != 0, []cache.Option{cache.WithMode(cache.InvalidateOnWrite())}, []string{"InvalidateOnWrite", "Notif"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			opts := append([]cache.Option{cache.WithTier(top, time.Minute), cache.WithTier(h.New(t, ns), time.Minute), cache.WithOrigin(newStore())}, c.opts...)
			if !c.declared {
				refuses(t, c.names, opts...)
				return
			}
			s, err := cache.New(context.Background(), opts...)
			if err != nil {
				t.Fatalf("New refused a mode the layer declares the capability for: %v", err)
			}
			s.Close()
		})
	}
}
