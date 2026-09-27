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

// RunStack checks the stack over the layer under test, shared by several
// stacks the way processes share a server, each with its own Memory tier
// above it. Concurrent reads of one missing key reach the origin once per
// partition when the layer is a Leaser, and at most once per stack otherwise.
// Then, in Partitions: two partitions never observe each other's values, fill
// leases, negative entries or invalidation notices; a write through any
// partition reaches every partition's copy; an operation without a partition
// fails and touches nothing; a write-around partition stores nothing; an
// authorization revision bump, which changes the partition, misses.
//
// When the layer is a Notifier and ExternalWrite is set, a write that bypasses
// the driver evicts the stack's in-process copy; when it is a Resyncer, a gap
// flushes the stack's in-process tier.
//
// Then Modes runs a property test for every mode, Pitfalls every named
// pitfall scenario, and Simulation the model-based simulation over a fixed
// seed set (see Simulate). A mode the layer's declaration cannot support is
// checked to be refused at construction, naming the capability, instead.
func RunStack(t *testing.T, h Harness) {
	runFillOnce(t, h)
	t.Run("Partitions", func(t *testing.T) { runPartitions(t, h) })
	t.Run("ExternalWriteEvictsMemory", func(t *testing.T) { runExternalWriteEvicts(t, h) })
	t.Run("ResyncFlushesMemory", func(t *testing.T) { runResyncFlushes(t, h) })
	t.Run("Modes", func(t *testing.T) { RunModes(t, h) })
	t.Run("Pitfalls", func(t *testing.T) { RunPitfalls(t, h) })
	t.Run("Simulation", func(t *testing.T) { RunSimulation(t, h) })
}

func runExternalWriteEvicts(t *testing.T, h Harness) {
	ns := Namespace(t)
	if !cache.CapabilitiesOf(h.New(t, ns)).Notifies || h.ExternalWrite == nil {
		t.Skip("needs a Notifier and the harness's ExternalWrite")
	}
	ctx := context.Background()
	p := newProcess(t, h, ns)
	u := partitionOf("u")
	if err := p.stack.Set(ctx, u, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	stored := p.shared.lastSet() // the key the stack wrote in the shared layer
	assertGet(t, p.stack, "", u, "k", "v")
	h.ExternalWrite(t, ns, stored)
	eventually(t, 3*time.Second, func() bool {
		p.shared.reset()
		_, _ = p.stack.Get(ctx, u, "k")
		return p.shared.calls() > 0
	}, "a write that bypassed the driver never evicted the stack's in-process copy")
}

func runResyncFlushes(t *testing.T, h Harness) {
	ns := Namespace(t)
	if !cache.CapabilitiesOf(h.New(t, ns)).Resyncs {
		t.Skip("the layer does not declare Resyncs")
	}
	if h.Interrupt == nil {
		t.Fatal("the layer declares Resyncs, so the harness must set Interrupt")
	}
	ctx := context.Background()
	origin := newViewerOrigin()
	p := newProcess(t, h, ns, cache.WithOrigin(origin))
	plain := newProcess(t, h, ns)
	viewers := []string{"u", "v"}
	for _, viewer := range viewers {
		assertGet(t, p.stack, viewer, partitionOf(viewer), "k", viewValue("k", viewer))
		if err := plain.stack.Set(ctx, partitionOf(viewer), "k", []byte(viewer)); err != nil {
			t.Fatal(err)
		}
	}
	// A Notifier may echo a stack's own writes, and asynchronously: an echo
	// that lands after the write evicts the copy that write just stored. Only
	// once every notice of the writes above has been delivered does a copy
	// stay put, so wait for that, then fill the in-process tiers by reading,
	// which notifies no one.
	drainNotices(t, h, ns, p, plain)
	for _, x := range []process{p, plain} {
		for _, viewer := range viewers {
			_, _ = x.stack.Get(asViewer(viewer), partitionOf(viewer), "k")
		}
		if x.top.Len() == 0 {
			t.Fatal("the stack cached nothing in its in-process tier")
		}
	}
	for _, x := range []process{p, plain} {
		h.Interrupt(t, x.raw)
	}
	for _, x := range []process{p, plain} {
		eventually(t, 10*time.Second, func() bool { return x.top.Len() == 0 },
			"a resync left copies (of some partition or generation) in the in-process tier")
	}
}

func runFillOnce(t *testing.T, h Harness) {
	ns := Namespace(t)
	origin := newViewerOrigin()
	origin.delay = 100 * time.Millisecond // long enough for every reader to pile up
	stacks := []*cache.Stack{
		newProcess(t, h, ns, cache.WithOrigin(origin)).stack,
		newProcess(t, h, ns, cache.WithOrigin(origin)).stack,
	}
	viewers := []string{"u", "v"}

	var wg sync.WaitGroup
	for i := range 64 {
		s, viewer := stacks[i%2], viewers[(i/2)%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.Get(asViewer(viewer), partitionOf(viewer), "k")
			if err != nil {
				t.Error(err)
				return
			}
			if want := viewValue("k", viewer); string(v) != want {
				t.Errorf("%s read %q, want %q", viewer, v, want)
			}
		}()
	}
	wg.Wait()

	want := 2
	if cache.CapabilitiesOf(h.New(t, ns)).Leases {
		want = 1
	}
	for _, viewer := range viewers {
		if n := origin.loadsBy(viewer); n > want || n == 0 {
			// Errorf, not Fatalf: the partition cases still run and report.
			t.Errorf("origin loaded %d times for %s, want %d at most", n, viewer, want)
		}
	}
}

func runPartitions(t *testing.T, h Harness) {
	ctx := context.Background()
	u, v := partitionOf("u"), partitionOf("v")

	t.Run("ValuesIsolated", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		a := newProcess(t, h, ns, cache.WithOrigin(origin))
		b := newProcess(t, h, ns, cache.WithOrigin(origin))
		assertGet(t, a.stack, "u", u, "k", viewValue("k", "u"))
		assertGet(t, a.stack, "v", v, "k", viewValue("k", "v"))
		assertGet(t, b.stack, "v", v, "k", viewValue("k", "v"))
		assertGet(t, b.stack, "u", u, "k", viewValue("k", "u"))
		for _, viewer := range []string{"u", "v"} {
			if n := origin.loadsBy(viewer); n != 1 {
				t.Fatalf("origin loaded %d times for %s, want 1: each partition fills once, and only for itself", n, viewer)
			}
		}

		// Without an origin, a value set in one partition is a miss in
		// every other, in either process.
		c := newProcess(t, h, ns)
		d := newProcess(t, h, ns)
		if err := c.stack.Set(ctx, u, "plain", []byte("u's")); err != nil {
			t.Fatal(err)
		}
		for _, s := range []*cache.Stack{c.stack, d.stack} {
			if got, err := s.Get(ctx, v, "plain"); !errors.Is(err, cache.ErrMiss) {
				t.Fatalf("another partition read %q, %v; want ErrMiss", got, err)
			}
		}
		assertGet(t, d.stack, "", u, "plain", "u's")

		// The layer key is unambiguous: no split of partition key and key
		// reaches another's entry.
		if err := c.stack.Set(ctx, cache.NewPartition("x"), "y:z", []byte("x's")); err != nil {
			t.Fatal(err)
		}
		if got, err := d.stack.Get(ctx, cache.NewPartition("x:y"), "z"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("partition %q read partition %q's entry: %q, %v", "x:y", "x", got, err)
		}
	})

	t.Run("FillOnceDoesNotCrossPartitions", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		release := origin.hold("u")
		defer release()
		// A lease long enough that waiting on another partition's fill would
		// outlast the deadline below.
		a := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithLeaseTTL(30*time.Second))
		b := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithLeaseTTL(30*time.Second))

		done := make(chan error, 1)
		go func() {
			got, err := a.stack.Get(asViewer("u"), u, "k")
			if err == nil && string(got) != viewValue("k", "u") {
				err = fmt.Errorf("u read %q", got)
			}
			done <- err
		}()
		eventually(t, 3*time.Second, func() bool { return origin.loadsBy("u") == 1 }, "u's load never started")

		// u's load holds the in-process flight in a and the lease on the
		// shared layer. Readers in other partitions, in the same process and
		// in another, load for themselves instead of waiting for it.
		for _, r := range []struct {
			s      *cache.Stack
			viewer string
		}{{a.stack, "v"}, {b.stack, "w"}} {
			rctx, cancel := context.WithTimeout(asViewer(r.viewer), 3*time.Second)
			got, err := r.s.Get(rctx, partitionOf(r.viewer), "k")
			cancel()
			if err != nil {
				t.Fatalf("%s waited on another partition's fill: %v", r.viewer, err)
			}
			if want := viewValue("k", r.viewer); string(got) != want {
				t.Fatalf("%s read %q, want %q", r.viewer, got, want)
			}
		}
		release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("NegativeEntriesIsolated", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		origin.hide("u")
		a := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithNegativeTTL(time.Minute))
		b := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithNegativeTTL(time.Minute))

		// u's "not found" does not mask v's value...
		if got, err := a.stack.Get(asViewer("u"), u, "k"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("u read %q, %v; want ErrNotFound", got, err)
		}
		assertGet(t, a.stack, "v", v, "k", viewValue("k", "v"))
		assertGet(t, b.stack, "v", v, "k", viewValue("k", "v"))
		// ...and v's value does not answer for u.
		assertGet(t, b.stack, "v", v, "k2", viewValue("k2", "v"))
		for _, s := range []*cache.Stack{a.stack, b.stack} {
			if got, err := s.Get(asViewer("u"), u, "k2"); !errors.Is(err, cache.ErrNotFound) {
				t.Fatalf("u read %q, %v; want ErrNotFound", got, err)
			}
		}
	})

	t.Run("NotificationsIsolated", func(t *testing.T) {
		ns := Namespace(t)
		if !cache.CapabilitiesOf(h.New(t, ns)).Notifies {
			t.Skip("the layer does not declare Notifies")
		}
		a, b := newProcess(t, h, ns), newProcess(t, h, ns)
		for p, value := range map[cache.Partition]string{u: "u1", v: "v1"} {
			if err := a.stack.Set(ctx, p, "k", []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
		// Those writes' notices must reach b before b caches the values, or a
		// late one evicts a copy this test then expects to find.
		settle(t, a, b)
		for p, value := range map[cache.Partition]string{u: "u1", v: "v1"} {
			assertGet(t, b.stack, "", p, "k", value) // now in b's Memory tier
		}
		if err := a.stack.Set(ctx, u, "k", []byte("u2")); err != nil {
			t.Fatal(err)
		}
		eventually(t, 3*time.Second, func() bool {
			got, err := b.stack.Get(ctx, u, "k")
			return err == nil && string(got) == "u2"
		}, "u's write never evicted u's copy in the other process")
		b.shared.reset()
		assertGet(t, b.stack, "", v, "k", "v1")
		if n := b.shared.calls(); n != 0 {
			t.Fatalf("u's write evicted v's copy in the other process: reading it reached the shared layer %d times", n)
		}
	})

	t.Run("WritesReachEveryPartition", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		a := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithNegativeTTL(time.Minute))
		b := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithNegativeTTL(time.Minute))
		notifies := cache.CapabilitiesOf(h.New(t, ns)).Notifies
		// The writing process sees a write at once. Another sees it once the
		// notice arrives; without notices its Memory tier keeps the old
		// generation until its TTL, as the README states.
		expect := func(writer, other process, what string, want func(viewer string) (string, error)) {
			t.Helper()
			processes := []process{writer}
			if notifies {
				processes = append(processes, other)
			}
			for _, pr := range processes {
				for _, viewer := range []string{"u", "v"} {
					wantValue, wantErr := want(viewer)
					eventually(t, 3*time.Second, func() bool {
						got, err := pr.stack.Get(asViewer(viewer), partitionOf(viewer), "k")
						if wantErr != nil {
							return errors.Is(err, wantErr)
						}
						return err == nil && string(got) == wantValue
					}, what+": "+viewer+" still reads a copy made before it")
				}
			}
		}
		for _, pr := range []process{a, b} {
			for _, viewer := range []string{"u", "v"} {
				assertGet(t, pr.stack, viewer, partitionOf(viewer), "k", viewValue("k", viewer))
			}
		}

		if err := a.stack.Set(asViewer("u"), u, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}
		expect(a, b, "a write in u", func(viewer string) (string, error) { return viewValue("new", viewer), nil })

		if err := a.stack.Delete(asViewer("v"), v, "k"); err != nil {
			t.Fatal(err)
		}
		expect(a, b, "a delete in v", func(string) (string, error) { return "", cache.ErrNotFound })

		// A write the origin received some other way, then invalidated
		// through one partition, including over a cached "not found".
		origin.put("k", "out of band")
		if err := b.stack.Invalidate(ctx, u, "k"); err != nil {
			t.Fatal(err)
		}
		expect(b, a, "an invalidation in u", func(viewer string) (string, error) { return viewValue("out of band", viewer), nil })
	})

	t.Run("RequiresPartition", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		refused := map[string]struct {
			p    cache.Partition
			want error
		}{
			"none":                    {cache.Partition{}, cache.ErrNoPartition},
			"empty key":               {cache.NewPartition(""), cache.ErrNoPartition},
			"empty write-around key":  {cache.NewPartition("", cache.WriteAround()), cache.ErrNoPartition},
			"global on a partitioned": {cache.Global(), cache.ErrWrongPartition},
		}
		pr := newProcess(t, h, ns, cache.WithOrigin(origin))
		plain := newProcess(t, h, ns)
		for name, c := range refused {
			assertRefused(t, name, pr.stack, c.p, c.want)
			assertRefused(t, name, plain.stack, c.p, c.want)
		}
		if n := origin.loadsBy(""); n != 0 {
			t.Fatalf("a refused operation reached the origin %d times", n)
		}
		for _, x := range []process{pr, plain} {
			if n := x.shared.calls(); n != 0 || x.top.Len() != 0 {
				t.Fatalf("refused operations made %d calls on the shared layer and left %d entries in the Memory tier, want none", n, x.top.Len())
			}
		}

		// A global stack takes Global only, and shares no entry with the
		// partitioned stacks over the same layer.
		global := newProcess(t, h, ns, cache.WithGlobal()).stack
		assertRefused(t, "none", global, cache.Partition{}, cache.ErrNoPartition)
		assertRefused(t, "a caller's partition on a global", global, u, cache.ErrWrongPartition)
		if err := global.Set(ctx, cache.Global(), "k", []byte("everyone's")); err != nil {
			t.Fatal(err)
		}
		assertGet(t, global, "", cache.Global(), "k", "everyone's")
		assertGet(t, pr.stack, "u", u, "k", viewValue("k", "u"))
	})

	t.Run("WriteAroundStoresNothing", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		a := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithNegativeTTL(time.Minute))
		granted := cache.NewPartition("tenant-u", cache.WriteAround())

		// A write-around read does not serve what the partition's other
		// callers cached...
		assertGet(t, a.stack, "u", u, "k", viewValue("k", "u"))
		origin.setSuffix("u", " under a grant")
		assertGet(t, a.stack, "u", granted, "k", viewValue("k", "u")+" under a grant")
		assertGet(t, a.stack, "u", u, "k", viewValue("k", "u")) // ...nor replaces it.

		// ...and makes no call on any layer, found or not found: no read, no
		// fill lease, no store.
		b := newProcess(t, h, ns, cache.WithOrigin(origin), cache.WithNegativeTTL(time.Minute))
		origin.hide("w")
		before := origin.loadsBy("u")
		for range 2 {
			assertGet(t, b.stack, "u", granted, "fresh", viewValue("fresh", "u")+" under a grant")
			if _, err := b.stack.Get(asViewer("w"), cache.NewPartition("tenant-w", cache.WriteAround()), "fresh"); !errors.Is(err, cache.ErrNotFound) {
				t.Fatalf("write-around read of a missing key = %v, want ErrNotFound", err)
			}
		}
		if n := origin.loadsBy("u") - before; n != 2 {
			t.Fatalf("two write-around reads loaded the origin %d times, want 2", n)
		}
		if n := b.shared.calls(); n != 0 || b.top.Len() != 0 {
			t.Fatalf("write-around reads made %d calls on the shared layer and stored %d entries in the Memory tier, want none", n, b.top.Len())
		}

		// Without an origin, its write stores nothing and drops the
		// partition's value.
		c := newProcess(t, h, ns)
		if err := c.stack.Set(ctx, u, "plain", []byte("old")); err != nil {
			t.Fatal(err)
		}
		if err := c.stack.Set(ctx, cache.NewPartition("tenant-u", cache.WriteAround()), "plain", []byte("new")); err != nil {
			t.Fatal(err)
		}
		if got, err := c.stack.Get(ctx, u, "plain"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("after a write-around write, read %q, %v; want ErrMiss", got, err)
		}
	})

	t.Run("RevisionBumpMisses", func(t *testing.T) {
		ns := Namespace(t)
		origin := newViewerOrigin()
		a := newProcess(t, h, ns, cache.WithOrigin(origin))
		b := newProcess(t, h, ns, cache.WithOrigin(origin))
		before, after := cache.NewPartition("tenant-u/revision-1"), cache.NewPartition("tenant-u/revision-2")
		assertGet(t, a.stack, "u", before, "k", viewValue("k", "u"))
		assertGet(t, b.stack, "u", before, "k", viewValue("k", "u"))
		if n := origin.loadsBy("u"); n != 1 {
			t.Fatalf("origin loaded %d times before the bump, want 1", n)
		}
		origin.setSuffix("u", " after revocation")
		assertGet(t, b.stack, "u", after, "k", viewValue("k", "u")+" after revocation")
		assertGet(t, a.stack, "u", after, "k", viewValue("k", "u")+" after revocation")
		if n := origin.loadsBy("u"); n != 2 {
			t.Fatalf("origin loaded %d times, want 2: the bumped partition misses once, then fills", n)
		}
	})
}

// process is one stack over the layer under test, with its own Memory tier
// above it, the way one process of a service holds one. shared records the
// calls the stack makes on the layer under test. It fills under a lease when
// the layer declares Leases.
type process struct {
	stack  *cache.Stack
	top    *cache.Memory
	shared *recorder
	raw    cache.Layer // the layer under test, unwrapped, for Interrupt
}

func newProcess(t *testing.T, h Harness, namespace string, opts ...cache.Option) process {
	t.Helper()
	raw := h.New(t, namespace)
	shared, rec := record(raw)
	p := process{top: cache.NewMemory(), shared: rec, raw: raw}
	defaults := []cache.Option{
		cache.WithTier(p.top, time.Minute),
		cache.WithTier(shared, time.Minute),
		cache.WithTTLJitter(0),
	}
	if cache.CapabilitiesOf(raw).Leases {
		defaults = append(defaults, cache.WithMode(cache.FillLease()))
	}
	s, err := cache.New(context.Background(), append(defaults, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	p.stack = s
	return p
}

// drainNotices waits until every notice of the writes made so far in
// namespace has reached each process's stack. A server delivers its notices in
// order, so once a stack has heard markers written after them, it has heard
// them. The markers span many keys, so that on a server of several shards
// every shard's stream is drained too.
func drainNotices(t *testing.T, h Harness, namespace string, processes ...process) {
	t.Helper()
	writer := h.New(t, namespace)
	if !cache.CapabilitiesOf(writer).Notifies {
		return
	}
	markers := make([]string, 64)
	for i := range markers {
		markers[i] = fmt.Sprintf("drain-%s-%d", Namespace(t), i)
		mustSet(t, writer, markers[i], cache.Entry{Value: []byte("marker")}, time.Minute)
	}
	for _, x := range processes {
		eventually(t, 10*time.Second, func() bool { return x.shared.heardAll(markers) },
			"the stack never heard notices written after the ones it is waiting for")
	}
}

// settle waits until every invalidation notice a's layer has published so far
// has reached b. Notices from one client arrive in order, so once b has
// heard a later one, it has heard the earlier ones.
func settle(t *testing.T, a, b process) {
	t.Helper()
	ctx := context.Background()
	marker, key := cache.NewPartition(Namespace(t)), "settle"
	if err := b.stack.Set(ctx, marker, key, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := a.stack.Set(ctx, marker, key, []byte("after")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool {
		got, err := b.stack.Get(ctx, marker, key)
		return err == nil && string(got) == "after"
	}, "the other process never heard a's notices")
}
