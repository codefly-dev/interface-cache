package cachetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// RunPitfalls runs every named pitfall of a multi-layer cache as a scenario
// over the layer under test, each stating the modes it applies to and the
// correct outcome under each (the README's pitfall table is the same list).
// Identity — no cross-partition leak, cross-partition staleness and the
// generation check — is RunStack's Partitions.
func RunPitfalls(t *testing.T, h Harness) {
	caps := cache.CapabilitiesOf(h.New(t, Namespace(t)))
	t.Run("StaleSet", func(t *testing.T) { runStaleSet(t, h, caps) })
	t.Run("StaleRefillAfterInvalidation", func(t *testing.T) { runStaleRefill(t, h, caps) })
	t.Run("ReplicaRefill", func(t *testing.T) { runReplicaRefill(t, h, caps) })
	t.Run("Invalidations", func(t *testing.T) { runInvalidations(t, h, caps) })
	t.Run("Stampede", func(t *testing.T) { runStampede(t, h, caps) })
	t.Run("NegativeCaching", func(t *testing.T) { runNegative(t, h) })
	t.Run("Pressure", func(t *testing.T) { runPressure(t, h) })
	t.Run("ClockSkew", func(t *testing.T) { runClockSkew(t, h) })
	t.Run("CodecSkew", func(t *testing.T) { runCodecSkew(t, h) })
	t.Run("PartialFailure", func(t *testing.T) { runPartialFailure(t, h, caps) })
	t.Run("Identity", func(t *testing.T) { runIdentityAcrossModes(t, h) })
	t.Run("OutOfBandOriginWrites", func(t *testing.T) { runOutOfBand(t, h, caps) })
}

// StaleSet: a slow load writes an old value after a newer write.
//
//   - A write through a stack, any mode: never. The load is stored under the
//     generation the write replaced, which no read looks up again.
//   - A write around the stack, FillVersionFenced: never; the older fill is
//     refused by the layer holding the newer version.
//   - A write around the stack, other fills: the older value is stored, and
//     served until the mode's freshness bound (checked here for Singleflight).
func runStaleSet(t *testing.T, h Harness, caps cache.Capabilities) {
	t.Run("WriteThroughAStack", func(t *testing.T) {
		for _, fill := range []cache.FillCoordination{cache.FillUncoordinated(), cache.FillSingleflight(), cache.FillLease(), cache.FillVersionFenced()} {
			t.Run(fill.String(), func(t *testing.T) {
				if (fill == cache.CoordinateLease && !caps.Leases) || (fill == cache.CoordinateVersionFenced && !caps.Fences) {
					t.Skip("the layer lacks the capability")
				}
				w := newModeWorld(t, h)
				w.origin.put("k", "before")
				a, _, _ := w.process(t, cache.WithMode(fill))
				b, _, _ := w.process(t, cache.WithMode(fill))
				inLoad, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				w.origin.setOnLoad(func(context.Context, string) { once.Do(func() { close(inLoad); <-resume }) })
				done := make(chan struct{})
				go func() { defer close(done); _, _ = a.Get(context.Background(), tenantU, "k") }()
				<-inLoad
				if err := b.Set(context.Background(), cache.NewPartition("someone-else"), "k", []byte("after")); err != nil {
					t.Fatal(err)
				}
				close(resume)
				<-done
				w.origin.setOnLoad(nil)
				wantString(t, a, tenantU, "k", "after")
				c, _, _ := w.process(t)
				wantString(t, c, tenantU, "k", "after")
			})
		}
	})
	t.Run("WriteAroundTheStack", func(t *testing.T) {
		if !caps.Fences {
			t.Skip("needs the layer to declare Fences")
		}
		if got := staleSet(t, h, cache.FillVersionFenced()); got != "two" {
			t.Fatalf("FillVersionFenced: the shared layer holds %q, want the newer %q", got, "two")
		}
		if got := staleSet(t, h, cache.FillSingleflight()); got != "one" {
			t.Fatalf("FillSingleflight: the shared layer holds %q; the documented outcome is the older %q, bounded by freshness", got, "one")
		}
	})
}

// StaleRefillAfterInvalidation (the race reported from module-saas-starter#942,
// F2): process b misses a key in its Memory tier and reads the key's old
// generation (or copy) from the shared layer; process a invalidates it; the
// notice reaches b and evicts nothing, since b's tier is still empty; then b
// stores what it read. Every mode: b must not keep it. A notice or local write
// between a fill's read of a lower tier and its store into an upper one undoes
// the store, for generations and copies alike.
func runStaleRefill(t *testing.T, h Harness, caps cache.Capabilities) {
	if !caps.Notifies {
		t.Skip("needs the layer to declare Notifies: without notices a copy above is bounded by its TTL")
	}
	for _, c := range []struct {
		name  string
		layer func(key string) string // the layer key b is held on
		opts  []cache.Option
	}{
		{"Generation", cache.GenerationKey, nil},
		{"GenerationInvalidateOnWrite", cache.GenerationKey, []cache.Option{cache.WithMode(cache.InvalidateOnWrite())}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if len(c.opts) > 0 && caps.NoticeBound == 0 {
				t.Skip("InvalidateOnWrite needs a NoticeBound")
			}
			ctx := context.Background()
			w := newModeWorld(t, h)
			w.origin.put("k", "before")
			a, _, _ := w.process(t, c.opts...)
			// b's shared layer holds b's read once it has read the key's
			// generation from it.
			shared := newFaulty(h.New(t, w.ns))
			rec := &recorder{}
			sharedRec := &recordedLayer{forward: forward{shared}, r: rec}
			top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
			b := stackOf(t, top, sharedRec, w.origin, w.clock, w.tasks.run, c.opts...)
			wantString(t, a, tenantU, "k", "before") // the shared layer holds G1
			key := c.layer("k")
			var once sync.Once
			shared.afterGet = func(ctx context.Context, got string) {
				if got != key {
					return
				}
				once.Do(func() {
					// b has read G1. a invalidates; wait for the notice to
					// reach b's stack before b goes on to store G1.
					w.origin.put("k", "after")
					if err := a.Invalidate(ctx, tenantU, "k"); err != nil {
						t.Error(err)
					}
					eventually(t, 10*time.Second, func() bool { return rec.heardAll([]string{key}) },
						"the invalidation's notice never reached b")
				})
			}
			if _, err := b.Get(ctx, tenantU, "k"); err != nil {
				t.Fatal(err)
			}
			shared.afterGet = nil
			// Whatever b's first read returned, b must not keep G1: its next
			// read, at any age, is the new value.
			wantString(t, b, tenantU, "k", "after")
			w.clock.Advance(30 * time.Second)
			wantString(t, b, tenantU, "k", "after")
		})
	}
}

// ReplicaRefill: after an invalidation, a reader refills its Memory tier from
// a shared layer that has not caught up (a replica). Default and
// InvalidateOnWrite: once the write's notice has reached the reader, the
// reader serves the write. On Memory this holds trivially; through a replica
// it holds only when notices come from the server the reads do.
func runReplicaRefill(t *testing.T, h Harness, caps cache.Capabilities) {
	if !caps.Notifies {
		t.Skip("needs the layer to declare Notifies")
	}
	w := newModeWorld(t, h)
	w.origin.put("k", "v0")
	a, _, _ := w.process(t)
	shared, rec := record(h.New(t, w.ns))
	top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
	b := stackOf(t, top, shared, w.origin, w.clock, nil)
	for i := range 30 {
		wantString(t, b, tenantU, "k", fmt.Sprintf("v%d", i))
		rec.mu.Lock()
		rec.heard = nil
		rec.mu.Unlock()
		want := fmt.Sprintf("v%d", i+1)
		if err := a.Set(context.Background(), tenantU, "k", []byte(want)); err != nil {
			t.Fatal(err)
		}
		eventually(t, 10*time.Second, func() bool { return rec.heardAll([]string{cache.GenerationKey("k")}) },
			"the write's notice never reached the reader")
		if got := getString(t, b, tenantU, "k"); got != want {
			t.Fatalf("after the notice of %q the reader refilled %q", want, got)
		}
	}
}

// Invalidations lost, delayed, reordered or duplicated. Every mode:
//   - Lost inside a reported gap: nothing the layers above held is served
//     after it, and nothing is served from them during it.
//   - Delayed: a copy is served until its notice lands (InvalidateOnWrite's
//     bound); a fill racing the late notice is undone (StaleRefill).
//   - Reordered or duplicated: harmless; a notice only evicts.
//   - A delete through the stack twice: no error, not found.
func runInvalidations(t *testing.T, h Harness, caps cache.Capabilities) {
	ctx := context.Background()
	if !caps.Notifies {
		t.Skip("needs the layer to declare Notifies")
	}
	setup := func(t *testing.T) (*modeWorld, *cache.Stack, *cache.Stack, *faulty, *cache.Memory) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t)
		shared := newFaulty(h.New(t, w.ns))
		shared.gaps = true
		top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
		b := stackOf(t, top, shared, w.origin, w.clock, nil)
		wantString(t, b, tenantU, "k", "one")
		return w, a, b, shared, top
	}
	t.Run("LostInAGap", func(t *testing.T) {
		_, a, b, shared, top := setup(t)
		shared.loseNotices()
		if top.Len() != 0 {
			t.Fatal("a reported gap left entries in the tier above")
		}
		if err := a.Set(ctx, tenantU, "k", []byte("two")); err != nil {
			t.Fatal(err)
		}
		wantString(t, b, tenantU, "k", "two") // read below, the tier above bypassed
		if top.Len() != 0 {
			t.Fatal("a read during the gap filled the tier above")
		}
		shared.resync()
		wantString(t, b, tenantU, "k", "two")
	})
	// A process that writes during a gap puts the new generation in its own
	// tier above; a write elsewhere, whose notice the gap loses, must still be
	// read: the tier above is not read during the gap.
	t.Run("OwnWriteInAGap", func(t *testing.T) {
		_, a, b, shared, _ := setup(t)
		shared.loseNotices()
		if err := b.Set(ctx, tenantU, "k", []byte("b's")); err != nil {
			t.Fatal(err)
		}
		if err := a.Set(ctx, tenantU, "k", []byte("a's")); err != nil {
			t.Fatal(err)
		}
		wantString(t, b, tenantU, "k", "a's")
		shared.resync()
		wantString(t, b, tenantU, "k", "a's")
	})
	// The same, with the shared layer's breaker open: fills must not fall back
	// to leasing on, reading from and filling the tier the gap bypasses.
	t.Run("LeaseDuringAGap", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		a, _, _ := w.process(t)
		shared := newFaulty(h.New(t, w.ns))
		shared.gaps = true
		top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
		b := stackOf(t, top, shared, w.origin, w.clock, nil, cache.WithMode(cache.FillLease()), cache.WithBreaker(1, time.Hour))
		shared.loseNotices()
		shared.failing.Store(true)
		if err := b.Set(ctx, tenantU, "k", []byte("b's")); !errors.Is(err, cache.ErrPartialWrite) {
			t.Fatalf("a write with the shared layer down = %v", err)
		}
		wantString(t, b, tenantU, "k", "b's")
		shared.failing.Store(false) // back, but the breaker stays open
		if err := a.Set(ctx, tenantU, "k", []byte("a's")); err != nil {
			t.Fatal(err)
		}
		wantString(t, b, tenantU, "k", "a's")
	})
	t.Run("DelayedReorderedDuplicated", func(t *testing.T) {
		_, a, b, shared, _ := setup(t)
		shared.holdNotices()
		for _, v := range []string{"two", "three"} {
			if err := a.Set(ctx, tenantU, "k", []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
		eventually(t, 10*time.Second, func() bool { return len(shared.heldKeys()) >= 2 }, "the writes' notices never arrived")
		wantString(t, b, tenantU, "k", "one") // delayed: the old copy until its notice lands
		shared.deliver(true, true, true)
		wantString(t, b, tenantU, "k", "three")
		if err := a.Delete(ctx, tenantU, "k"); err != nil {
			t.Fatal(err)
		}
		if err := a.Delete(ctx, tenantU, "k"); err != nil {
			t.Fatalf("a second delete = %v", err)
		}
		eventually(t, 10*time.Second, func() bool { return getString(t, b, tenantU, "k") == "<not found>" }, "the delete was never seen")
	})
}

// Stampede, mass expiry and missing-key penetration.
//   - Stampede: FillUncoordinated loads once per miss, FillSingleflight once
//     per process, FillLease once across processes (see Modes/Fill).
//   - Mass expiry: with TTL jitter f, copies filled together expire over
//     [(1-f)·TTL, TTL]; with none, together.
//   - Penetration: repeated reads of a missing key load the origin once per
//     negative TTL; with negative caching off, every time.
func runStampede(t *testing.T, h Harness, caps cache.Capabilities) {
	t.Run("HotKey", func(t *testing.T) {
		w := newModeWorld(t, h)
		fill := cache.FillSingleflight()
		want := 2
		if caps.Leases {
			fill, want = cache.FillLease(), 1
		}
		a, _, _ := w.process(t, cache.WithMode(fill))
		b, _, _ := w.process(t, cache.WithMode(fill))
		if n := stampede(t, w, []*cache.Stack{a, b}, 64, 300*time.Millisecond); n < 1 || n > want {
			t.Fatalf("128 reads of a hot key loaded the origin %d times, want at most %d", n, want)
		}
	})
	t.Run("MassExpiry", func(t *testing.T) {
		for _, c := range []struct {
			jitter       float64
			expiredLow   int
			expiredHigh  int
			atFractionOf float64
		}{{0.5, 10, 90, 0.75}, {0, 0, 0, 0.75}} {
			w := newModeWorld(t, h)
			top := cache.NewMemory(cache.MemoryClock(w.clock.Now))
			s := stackOf(t, top, nil, w.origin, w.clock, nil, cache.WithTTLJitter(c.jitter))
			for i := range 100 {
				key := fmt.Sprintf("k%d", i)
				w.origin.put(key, "v")
				wantString(t, s, tenantU, key, "v")
			}
			w.clock.Advance(time.Duration(c.atFractionOf * float64(time.Minute)))
			// Without a shared tier, a copy that expired reloads from the
			// origin: count those loads.
			before := w.origin.loadCount()
			for i := range 100 {
				wantString(t, s, tenantU, fmt.Sprintf("k%d", i), "v")
			}
			expired := w.origin.loadCount() - before
			if expired < c.expiredLow || expired > c.expiredHigh {
				t.Fatalf("jitter %v: %d of 100 copies filled together expired at %.0f%% of the TTL, want %d to %d",
					c.jitter, expired, c.atFractionOf*100, c.expiredLow, c.expiredHigh)
			}
		}
	})
	t.Run("Penetration", func(t *testing.T) {
		for _, c := range []struct {
			ttl  time.Duration
			want int
		}{{time.Minute, 1}, {0, 20}} {
			w := newModeWorld(t, h)
			a, _, _ := w.process(t, cache.WithNegativeTTL(c.ttl))
			for range 20 {
				wantString(t, a, tenantU, "absent", "<not found>")
			}
			if n := w.origin.loadCount(); n != c.want {
				t.Fatalf("negative TTL %s: 20 reads of a missing key loaded the origin %d times, want %d", c.ttl, n, c.want)
			}
		}
	})
}

// Negative-cache poisoning: a key created right after its "not found" was
// cached.
//   - Created through a stack, any mode but WriteOriginOnly: read at once.
//   - WriteOriginOnly, or created around the stack: TTLBounded serves "not
//     found" until the negative TTL; Validated reads it at once;
//     InvalidateOnWrite after Stack.Invalidate or the origin's change feed.
func runNegative(t *testing.T, h Harness) {
	ctx := context.Background()
	t.Run("CreatedThroughAStack", func(t *testing.T) {
		w := newModeWorld(t, h)
		a, _, _ := w.process(t, cache.WithNegativeTTL(time.Minute))
		wantString(t, a, tenantU, "k", "<not found>")
		if err := a.Set(ctx, tenantU, "k", []byte("created")); err != nil {
			t.Fatal(err)
		}
		wantString(t, a, tenantU, "k", "created")
	})
	t.Run("CreatedAroundTheStack", func(t *testing.T) {
		w := newModeWorld(t, h)
		a, _, _ := w.process(t, cache.WithNegativeTTL(10*time.Second))
		wantString(t, a, tenantU, "k", "<not found>")
		w.origin.put("k", "created")
		wantString(t, a, tenantU, "k", "<not found>")
		wantString(t, a, tenantU, "k", "created", cache.Validated())
		w.origin.put("k2", "x")
		w.origin.remove("k2")
		wantString(t, a, tenantU, "k2", "<not found>")
		w.origin.put("k2", "created")
		w.clock.Advance(10 * time.Second)
		wantString(t, a, tenantU, "k2", "created")
		w.origin.remove("k3")
		wantString(t, a, tenantU, "k3", "<not found>")
		w.origin.put("k3", "created")
		if err := a.Invalidate(ctx, tenantU, "k3"); err != nil {
			t.Fatal(err)
		}
		wantString(t, a, tenantU, "k3", "created")
	})
}

// Hot keys, memory pressure and per-view duplication: under a byte budget far
// smaller than the working set, every read is still correct and the Memory
// tier never exceeds its budget; each partition holds its own copy, so views
// multiply what a key costs.
func runPressure(t *testing.T, h Harness) {
	const budget = 16 << 10
	w := newModeWorld(t, h)
	shared := h.New(t, w.ns)
	top := cache.NewMemory(cache.MaxBytes(budget), cache.MemoryClock(w.clock.Now))
	s := stackOf(t, top, shared, w.origin, w.clock, nil)
	value := string(make([]byte, 300))
	for i := range 100 {
		w.origin.put(fmt.Sprintf("k%d", i), fmt.Sprintf("%d%s", i, value))
	}
	for round := range 3 {
		for i := range 100 {
			key := fmt.Sprintf("k%d", i)
			if got := getString(t, s, tenantU, key); got != fmt.Sprintf("%d%s", i, value) {
				t.Fatalf("round %d: %s read a wrong value under memory pressure", round, key)
			}
			if top.Bytes() > budget {
				t.Fatalf("the Memory tier holds %d bytes, over its budget of %d", top.Bytes(), budget)
			}
		}
	}
	// Per-view duplication: one key read by 20 partitions costs 20 copies.
	views := cache.NewMemory(cache.MemoryClock(w.clock.Now))
	v := stackOf(t, views, nil, w.origin, w.clock, nil)
	for i := range 20 {
		wantString(t, v, cache.NewPartition(fmt.Sprintf("view-%d", i)), "k1", "1"+value)
	}
	if n := views.Len(); n != 21 { // 20 copies and the key's generation
		t.Fatalf("20 views of one key hold %d entries, want 21", n)
	}
	t.Logf("per-view duplication: one %d-byte value read by 20 views costs %d bytes in the Memory tier", len(value)+1, views.Bytes())
}

// Clock skew: Confirmed is stamped on the loading process's clock. A reader
// whose clock is behind the loader's sees copies as younger than they are:
// TTLBounded(d) then serves a value up to d plus the skew old, unless the
// reader sets WithClockSkew to at least the skew, which makes it hold in real
// time.
func runClockSkew(t *testing.T, h Harness) {
	const bound, skew = time.Minute, 20 * time.Second
	for _, c := range []struct {
		name        string
		allowance   time.Duration
		servesStale bool
	}{{"without WithClockSkew", 0, true}, {"with WithClockSkew", skew, false}} {
		t.Run(c.name, func(t *testing.T) {
			w := newModeWorld(t, h)
			w.origin.put("k", "one")
			shared := h.New(t, w.ns)
			loader := stackOf(t, nil, shared, w.origin, skewed{w.clock, skew}, nil, cache.WithMode(cache.TTLBounded(bound)))
			reader := stackOf(t, nil, h.New(t, w.ns), w.origin, w.clock, nil,
				cache.WithMode(cache.TTLBounded(bound)), cache.WithClockSkew(c.allowance))
			wantString(t, loader, tenantU, "k", "one")
			w.origin.put("k", "two")
			w.clock.Advance(bound + skew/2) // real age past the bound, measured age inside it
			got := getString(t, reader, tenantU, "k")
			if (got == "one") != c.servesStale {
				t.Fatalf("a copy %s old, loaded by a clock %s ahead: read %q", bound+skew/2, skew, got)
			}
		})
	}
}

// Codec skew: a deploy with a new value shape shares the layers with the old
// one. With WithSchema per shape, neither decodes the other's bytes (each
// loads the origin), and a write by either invalidates both.
func runCodecSkew(t *testing.T, h Harness) {
	w := newModeWorld(t, h)
	w.origin.put("k", "one")
	oldShape, _, _ := w.process(t, cache.WithSchema("v1"))
	newShape, _, _ := w.process(t, cache.WithSchema("v2"))
	wantString(t, oldShape, tenantU, "k", "one")
	loads := w.origin.loadCount()
	wantString(t, newShape, tenantU, "k", "one")
	if w.origin.loadCount() != loads+1 {
		t.Fatal("a stack of another schema was served bytes cached by the old one")
	}
	if err := newShape.Set(context.Background(), tenantU, "k", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if !cache.CapabilitiesOf(h.New(t, w.ns)).Notifies {
		w.clock.Advance(time.Minute)
	}
	eventually(t, 10*time.Second, func() bool { return getString(t, oldShape, tenantU, "k") == "two" },
		"a write by the new schema never invalidated the old schema's copies")
}

// Partial failure.
//   - An invalidation that reaches some layers only: the write returns
//     ErrPartialWrite; the writer never reads the layer that missed it until
//     the invalidation is replayed there.
//   - A layer down mid-write: Degrade writes the origin and reports
//     ErrPartialWrite; FailClosed fails with ErrUnavailable and leaves the
//     origin unchanged.
//   - Breaker recovery: once the layer is back, the stack replays what it
//     missed before reading it, so it never serves the generation it missed;
//     another process reads the write once that replay's notice lands.
func runPartialFailure(t *testing.T, h Harness, caps cache.Capabilities) {
	ctx := context.Background()
	t.Run("InvalidationReachesSomeLayersOnly", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		top := newFaulty(cache.NewMemory(cache.MemoryClock(w.clock.Now)))
		s, err := buildStack(nil, nil, w.origin, w.clock, nil,
			cache.WithTier(top, time.Minute), cache.WithTier(h.New(t, w.ns), time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		wantString(t, s, tenantU, "k", "one")
		top.failing.Store(true)
		if err := s.Set(ctx, tenantU, "k", []byte("two")); !errors.Is(err, cache.ErrPartialWrite) {
			t.Fatalf("a write the top tier missed = %v, want ErrPartialWrite", err)
		}
		top.failing.Store(false) // the tier is back, still holding the old generation
		wantString(t, s, tenantU, "k", "two")
		wantString(t, s, tenantU, "k", "two")
	})
	// A tier that refuses writes but still answers reads keeps the generation
	// the write replaced: it is not read until the write lands there.
	t.Run("TierRefusesWritesButAnswersReads", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		top := newFaulty(cache.NewMemory(cache.MemoryClock(w.clock.Now)))
		s, err := buildStack(nil, nil, w.origin, w.clock, nil,
			cache.WithTier(top, time.Minute), cache.WithTier(h.New(t, w.ns), time.Minute), cache.WithBreaker(100, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		wantString(t, s, tenantU, "k", "one")
		top.failWrites.Store(true)
		if err := s.Set(ctx, tenantU, "k", []byte("two")); !errors.Is(err, cache.ErrPartialWrite) {
			t.Fatalf("a write the top tier refused = %v, want ErrPartialWrite", err)
		}
		for range 3 {
			wantString(t, s, tenantU, "k", "two")
		}
		if _, err := s.Get(ctx, tenantU, "k", cache.FailClosed()); !errors.Is(err, cache.ErrUnavailable) {
			t.Fatalf("a FailClosed read past the quarantined tier = %v, want ErrUnavailable", err)
		}
		top.failWrites.Store(false)
		wantString(t, s, tenantU, "k", "two")
	})
	t.Run("LayerDownMidWrite", func(t *testing.T) {
		for _, c := range []struct {
			policy  cache.FailurePolicy
			want    error
			written bool
		}{{cache.Degrade(), cache.ErrPartialWrite, true}, {cache.FailClosed(), cache.ErrUnavailable, false}} {
			t.Run(c.policy.String(), func(t *testing.T) {
				w := newModeWorld(t, h)
				w.origin.put("k", "one")
				shared := newFaulty(h.New(t, w.ns))
				s := stackOf(t, cache.NewMemory(cache.MemoryClock(w.clock.Now)), shared, w.origin, w.clock, nil, cache.WithMode(c.policy))
				wantString(t, s, tenantU, "k", "one")
				shared.failing.Store(true)
				if err := s.Set(ctx, tenantU, "k", []byte("two")); !errors.Is(err, c.want) {
					t.Fatalf("a write with the shared layer down = %v, want %v", err, c.want)
				}
				if written := w.origin.current("k").value == "two"; written != c.written {
					t.Fatalf("the origin was written: %v, want %v", written, c.written)
				}
			})
		}
	})
	t.Run("BreakerRecovery", func(t *testing.T) {
		w := newModeWorld(t, h)
		w.origin.put("k", "one")
		shared := newFaulty(h.New(t, w.ns))
		writer := stackOf(t, cache.NewMemory(cache.MemoryClock(w.clock.Now)), shared, w.origin, w.clock, nil, cache.WithBreaker(1, time.Second))
		other, _, _ := w.process(t)
		wantString(t, writer, tenantU, "k", "one")
		wantString(t, other, tenantU, "k", "one")
		shared.failing.Store(true)
		if err := writer.Set(ctx, tenantU, "k", []byte("two")); !errors.Is(err, cache.ErrPartialWrite) {
			t.Fatalf("a write with the shared layer down = %v, want ErrPartialWrite", err)
		}
		wantString(t, writer, tenantU, "k", "two")
		// The other process reads the shared layer, which missed the write.
		wantString(t, other, tenantU, "k", "one")
		shared.failing.Store(false)
		w.clock.Advance(2 * time.Second) // past the breaker's cooldown
		wantString(t, writer, tenantU, "k", "two")
		eventually(t, 10*time.Second, func() bool {
			if !caps.Notifies {
				w.clock.Advance(time.Minute)
			}
			return getString(t, other, tenantU, "k") == "two"
		}, "the replayed invalidation never reached the other process")
	})
}

// Identity across modes: whatever the freshness, two partitions never read
// each other's view of a key.
func runIdentityAcrossModes(t *testing.T, h Harness) {
	for _, f := range []cache.Freshness{cache.TTLBounded(time.Minute), cache.StaleWhileRevalidate(time.Minute), cache.ReadYourWrites(), cache.Bypass()} {
		t.Run(f.String(), func(t *testing.T) {
			ns := Namespace(t)
			origin := newViewerOrigin()
			a := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithMode(f))
			b := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithMode(f))
			for range 2 {
				for _, viewer := range []string{"u", "v"} {
					for _, p := range []process{a, b} {
						assertGet(t, p.stack, viewer, partitionOf(viewer), "k", viewValue("k", viewer))
					}
				}
			}
		})
	}
}

// Out-of-band origin writes: a write the origin receives around every stack.
// Measured here as how long each mode keeps serving the old value (logged),
// and held to its bound: TTLBounded(d) at most d; Validated and Bypass not at
// all; InvalidateOnWrite until Stack.Invalidate or, over an origin that
// declares ChangeFeed, until the feed reports it.
func runOutOfBand(t *testing.T, h Harness, caps cache.Capabilities) {
	measure := func(t *testing.T, feed bool, mode ...cache.ModeOption) (time.Duration, bool) {
		w := newModeWorld(t, h)
		w.origin.feed = feed
		w.origin.put("k", "one")
		a, _, _ := w.process(t, cache.WithMode(mode...))
		wantString(t, a, tenantU, "k", "one")
		w.origin.put("k", "two")
		for elapsed := time.Duration(0); elapsed <= 10*time.Minute; elapsed += time.Second {
			if getString(t, a, tenantU, "k") == "two" {
				return elapsed, true
			}
			w.clock.Advance(time.Second)
		}
		return 0, false
	}
	for _, c := range []struct {
		name   string
		mode   []cache.ModeOption
		feed   bool
		within time.Duration // -1: never, without an invalidation
		skip   bool
	}{
		{"TTLBounded(30s)", []cache.ModeOption{cache.TTLBounded(30 * time.Second)}, false, 30 * time.Second, false},
		{"Validated", []cache.ModeOption{cache.Validated()}, false, 0, false},
		{"Bypass", []cache.ModeOption{cache.Bypass()}, false, 0, false},
		{"InvalidateOnWrite without a feed", []cache.ModeOption{cache.InvalidateOnWrite()}, false, -1, !caps.Notifies || caps.NoticeBound == 0},
		{"InvalidateOnWrite with a ChangeFeed", []cache.ModeOption{cache.InvalidateOnWrite()}, true, 0, !caps.Notifies || caps.NoticeBound == 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.skip {
				t.Skip("needs the layer to declare Notifies with a NoticeBound")
			}
			stale, seen := measure(t, c.feed, c.mode...)
			switch {
			case c.within < 0 && seen:
				t.Fatalf("seen after %s: the documented outcome is stale until an invalidation", stale)
			case c.within >= 0 && !seen:
				t.Fatal("never seen")
			case c.within >= 0 && stale > c.within:
				t.Fatalf("stale for %s, beyond the bound %s", stale, c.within)
			}
			if seen {
				t.Logf("%s: an out-of-band write was served stale for %s", c.name, stale)
			} else {
				t.Logf("%s: an out-of-band write was never seen without an invalidation", c.name)
			}
		})
	}
}
