// Package cachetest is the conformance suite for codefly.dev/cache drivers.
// A driver is correct when Run and RunStack pass against a real server — not a
// fake of one — so the suite is also what a provider service runs to prove it
// serves the interface.
package cachetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
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

// Run checks the Layer contract, plus the Leaser and Notifier contracts when
// the layer implements them.
func Run(t *testing.T, h Harness) {
	t.Run("Layer", func(t *testing.T) { runLayer(t, h) })
	if _, ok := h.New(t, Namespace(t)).(cache.Leaser); ok {
		t.Run("Leaser", func(t *testing.T) { runLeaser(t, h) })
	}
	if _, ok := h.New(t, Namespace(t)).(cache.Notifier); ok {
		t.Run("Notifier", func(t *testing.T) { runNotifier(t, h) })
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
		fresh := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		entries := map[string]cache.Entry{
			"binary":              {Value: []byte{0, 1, 2, 0xff, 0}, Version: "v1", FreshUntil: fresh},
			"empty":               {Value: []byte{}, Version: "v2"},
			"negative":            {Missing: true, FreshUntil: fresh},
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
	ns := Namespace(t)
	a, b := h.New(t, ns).(cache.Notifier), h.New(t, ns).(cache.Notifier)

	var mu sync.Mutex
	seen := map[string][]string{}
	record := func(who string) func(string) {
		return func(key string) {
			mu.Lock()
			defer mu.Unlock()
			seen[who] = append(seen[who], key)
		}
	}
	stopA, err := a.Subscribe(ctx, record("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer stopA()
	stopB, err := b.Subscribe(ctx, record("b"))
	if err != nil {
		t.Fatal(err)
	}
	defer stopB()

	mustSet(t, a, "set-by-a", cache.Entry{Value: []byte("v")}, time.Minute)
	if err := a.Delete(ctx, "deleted-by-a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, "deleted-by-b"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen["b"]) >= 2 && len(seen["a"]) >= 1
	}, "notifications never arrived")

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"set-by-a", "deleted-by-a"}; !equalStrings(seen["b"], want) {
		t.Fatalf("b heard %v, want %v", seen["b"], want)
	}
	// b's delete was published after a's writes, so by the time a hears it, a
	// would also have heard its own writes had they been echoed back.
	if want := []string{"deleted-by-b"}; !equalStrings(seen["a"], want) {
		t.Fatalf("a heard %v, want only other clients' writes %v", seen["a"], want)
	}
}

// RunStack checks the stack over the layer under test, shared by several
// stacks the way processes share a server, each with its own Memory tier
// above it. Concurrent reads of one missing key reach the origin once per
// partition when the layer is a Leaser, and at most once per stack otherwise.
// Then, in Partitions: two partitions never observe each other's values, fill
// leases, negative entries or invalidation notices; a write through any
// partition reaches every partition's copy; an operation without a partition
// fails and touches nothing; a write-around partition stores nothing; an
// authorization revision bump, which changes the partition, misses.
func RunStack(t *testing.T, h Harness) {
	runFillOnce(t, h)
	t.Run("Partitions", func(t *testing.T) { runPartitions(t, h) })
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
	if _, ok := h.New(t, ns).(cache.Leaser); ok {
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
		if _, ok := h.New(t, ns).(cache.Notifier); !ok {
			t.Skip("the layer is not a Notifier")
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
		_, notifies := h.New(t, ns).(cache.Notifier)
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
// calls the stack makes on the layer under test.
type process struct {
	stack  *cache.Stack
	top    *cache.Memory
	shared *recorder
}

func newProcess(t *testing.T, h Harness, namespace string, opts ...cache.Option) process {
	t.Helper()
	shared, rec := record(h.New(t, namespace))
	p := process{top: cache.NewMemory(), shared: rec}
	s, err := cache.New(context.Background(), append([]cache.Option{
		cache.WithTier(p.top, time.Minute),
		cache.WithTier(shared, time.Minute),
		cache.WithTTLJitter(0),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	p.stack = s
	return p
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

// recorder counts the calls a stack makes on the layer it wraps.
type recorder struct {
	mu sync.Mutex
	n  int
}

func (r *recorder) called() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n++
}

func (r *recorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n = 0
}

// record wraps l so every call on it is counted, keeping its Leaser and
// Notifier roles: the stack must treat the wrapped layer exactly as l.
// Subscribe is not counted; the stack calls it once, when it is built.
func record(l cache.Layer) (cache.Layer, *recorder) {
	r := &recorder{}
	base := recordedLayer{layer: l, r: r}
	_, leases := l.(cache.Leaser)
	_, notifies := l.(cache.Notifier)
	switch {
	case leases && notifies:
		return recordedLeaserNotifier{recordedLeaser{base}}, r
	case leases:
		return recordedLeaser{base}, r
	case notifies:
		return recordedNotifier{base}, r
	default:
		return base, r
	}
}

type recordedLayer struct {
	layer cache.Layer
	r     *recorder
}

func (x recordedLayer) Get(ctx context.Context, key string) (cache.Entry, error) {
	x.r.called()
	return x.layer.Get(ctx, key)
}

func (x recordedLayer) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	x.r.called()
	return x.layer.Set(ctx, key, e, ttl)
}

func (x recordedLayer) Delete(ctx context.Context, key string) error {
	x.r.called()
	return x.layer.Delete(ctx, key)
}

type recordedLeaser struct{ recordedLayer }

func (x recordedLeaser) Acquire(ctx context.Context, key string, ttl time.Duration) (cache.Lease, bool, error) {
	x.r.called()
	return x.layer.(cache.Leaser).Acquire(ctx, key, ttl)
}

func (x recordedLeaser) Fill(ctx context.Context, lease cache.Lease, e cache.Entry, ttl time.Duration) error {
	x.r.called()
	return x.layer.(cache.Leaser).Fill(ctx, lease, e, ttl)
}

func (x recordedLeaser) Release(ctx context.Context, lease cache.Lease) error {
	x.r.called()
	return x.layer.(cache.Leaser).Release(ctx, lease)
}

type recordedNotifier struct{ recordedLayer }

func (x recordedNotifier) Subscribe(ctx context.Context, fn func(key string)) (func(), error) {
	return x.layer.(cache.Notifier).Subscribe(ctx, fn)
}

type recordedLeaserNotifier struct{ recordedLeaser }

func (x recordedLeaserNotifier) Subscribe(ctx context.Context, fn func(key string)) (func(), error) {
	return x.layer.(cache.Notifier).Subscribe(ctx, fn)
}

// viewerOrigin answers according to who asks, the way an origin that
// authorizes with the caller's credentials does: the stack passes the
// caller's context to Load, and the viewer rides on it. Each viewer sees its
// own view of the value last written, or of the key's name before any write.
type viewerOrigin struct {
	delay time.Duration

	mu      sync.Mutex
	loads   map[string]int
	hidden  map[string]bool
	suffix  map[string]string
	gates   map[string]chan struct{}
	values  map[string]string
	removed map[string]bool
	version int
}

type viewerKey struct{}

func asViewer(viewer string) context.Context {
	return context.WithValue(context.Background(), viewerKey{}, viewer)
}

// partitionOf is the partition a viewer's reads are scoped to.
func partitionOf(viewer string) cache.Partition { return cache.NewPartition("tenant-" + viewer) }

func viewValue(key, viewer string) string { return key + " as seen by " + viewer }

func newViewerOrigin() *viewerOrigin {
	return &viewerOrigin{
		loads:   map[string]int{},
		hidden:  map[string]bool{},
		suffix:  map[string]string{},
		gates:   map[string]chan struct{}{},
		values:  map[string]string{},
		removed: map[string]bool{},
	}
}

func (o *viewerOrigin) Load(ctx context.Context, key, _ string) (cache.Entry, error) {
	viewer, _ := ctx.Value(viewerKey{}).(string)
	o.mu.Lock()
	o.loads[viewer]++
	gate, hidden, suffix := o.gates[viewer], o.hidden[viewer], o.suffix[viewer]
	value, written := o.values[key]
	removed := o.removed[key]
	o.mu.Unlock()
	if !written {
		value = key
	}
	time.Sleep(o.delay)
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return cache.Entry{}, ctx.Err()
		}
	}
	if hidden || removed {
		return cache.Entry{}, cache.ErrNotFound
	}
	return cache.Entry{Value: []byte(viewValue(value, viewer) + suffix), Version: "1"}, nil
}

// Put implements cache.Store.
func (o *viewerOrigin) Put(_ context.Context, key string, value []byte) (string, error) {
	o.put(key, string(value))
	o.mu.Lock()
	defer o.mu.Unlock()
	return strconv.Itoa(o.version), nil
}

// Remove implements cache.Store.
func (o *viewerOrigin) Remove(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.removed[key] = true
	o.version++
	return nil
}

// put writes key at the origin, bypassing any stack.
func (o *viewerOrigin) put(key, value string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.values[key] = value
	delete(o.removed, key)
	o.version++
}

// hold makes viewer's loads block until the returned func is called.
func (o *viewerOrigin) hold(viewer string) (release func()) {
	gate := make(chan struct{})
	o.mu.Lock()
	o.gates[viewer] = gate
	o.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			o.mu.Lock()
			delete(o.gates, viewer)
			o.mu.Unlock()
			close(gate)
		})
	}
}

func (o *viewerOrigin) hide(viewer string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hidden[viewer] = true
}

// setSuffix changes what viewer's loads return from now on.
func (o *viewerOrigin) setSuffix(viewer, suffix string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.suffix[viewer] = suffix
}

func (o *viewerOrigin) loadsBy(viewer string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.loads[viewer]
}

func assertGet(t *testing.T, s *cache.Stack, viewer string, p cache.Partition, key, want string) {
	t.Helper()
	got, err := s.Get(asViewer(viewer), p, key)
	if err != nil {
		t.Fatalf("Get(%q) as %q: %v", key, viewer, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) as %q = %q, want %q", key, viewer, got, want)
	}
}

func assertRefused(t *testing.T, name string, s *cache.Stack, p cache.Partition, want error) {
	t.Helper()
	ctx := context.Background()
	for op, err := range map[string]error{
		"Get":        func() error { _, err := s.Get(ctx, p, "k"); return err }(),
		"GetEntry":   func() error { _, err := s.GetEntry(ctx, p, "k"); return err }(),
		"Set":        s.Set(ctx, p, "k", []byte("v")),
		"Delete":     s.Delete(ctx, p, "k"),
		"Invalidate": s.Invalidate(ctx, p, "k"),
	} {
		if !errors.Is(err, want) {
			t.Fatalf("%s with %s = %v, want %v", op, name, err, want)
		}
	}
}

func mustSet(t *testing.T, l cache.Layer, key string, e cache.Entry, ttl time.Duration) {
	t.Helper()
	if err := l.Set(context.Background(), key, e, ttl); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

func mustAcquire(t *testing.T, l cache.Leaser, key string, ttl time.Duration) cache.Lease {
	t.Helper()
	lease, ok, err := l.Acquire(context.Background(), key, ttl)
	if err != nil {
		t.Fatalf("Acquire(%q): %v", key, err)
	}
	if !ok {
		t.Fatalf("Acquire(%q): lease not granted", key)
	}
	return lease
}

func assertEntry(t *testing.T, key string, got, want cache.Entry) {
	t.Helper()
	if !bytes.Equal(got.Value, want.Value) || got.Version != want.Version || got.Missing != want.Missing || !got.FreshUntil.Equal(want.FreshUntil) {
		t.Fatalf("Get(%q) = %+v, want %+v", key, got, want)
	}
	if want.Value != nil && got.Value == nil {
		t.Fatalf("Get(%q): empty value came back as nil", key)
	}
}

func eventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
