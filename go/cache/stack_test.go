package cache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
	"github.com/codefly-dev/interface-cache/go/cache/cachetest"
)

// sharedMemory hands out clients of one store per namespace, the way several
// processes hold clients of one server.
func sharedMemory() cachetest.Harness {
	var mu sync.Mutex
	stores := map[string]*cache.Memory{}
	return cachetest.Harness{New: func(_ *testing.T, ns string) cache.Layer {
		mu.Lock()
		defer mu.Unlock()
		if m, ok := stores[ns]; ok {
			return m.Share()
		}
		stores[ns] = cache.NewMemory()
		return stores[ns]
	}}
}

// tenant is the partition the tests that are not about partitioning read in.
var tenant = cache.NewPartition("tenant")

func TestMemoryConformance(t *testing.T) { cachetest.Run(t, sharedMemory()) }

// Two stacks, each with a private Memory over one shared Memory: the origin is
// loaded once, which only holds if the stack leases on the deepest layer.
func TestMemoryStackFillOnce(t *testing.T) { cachetest.RunStack(t, sharedMemory()) }

func TestRemoteWriteEvictsUpperTiers(t *testing.T) {
	ctx := context.Background()
	shared := cache.NewMemory()
	build := func(bottom *cache.Memory) *cache.Stack {
		return newStack(t,
			cache.WithTier(cache.NewMemory(), time.Hour),
			cache.WithTier(cache.NewMemory(), time.Hour),
			cache.WithTier(bottom, time.Hour),
		)
	}
	a, b := build(shared), build(shared.Share())
	if err := a.Set(ctx, tenant, "k", []byte("one")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, b, "k", "one") // now in both of b's upper tiers
	if err := a.Set(ctx, tenant, "k", []byte("two")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, b, "k", "two")
	if err := a.Delete(ctx, tenant, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("b still serves a key a deleted: %v", err)
	}
}

// Any number of layers works, including none: an origin alone still gets
// singleflight.
func TestAnyNumberOfTiers(t *testing.T) {
	for tiers := range 4 {
		t.Run(fmt.Sprintf("%d tiers", tiers), func(t *testing.T) {
			o := newOrigin()
			o.put("k", "v")
			o.delay = 30 * time.Millisecond
			opts := []cache.Option{cache.WithOrigin(o)}
			for range tiers {
				opts = append(opts, cache.WithTier(cache.NewMemory(), time.Minute))
			}
			s := newStack(t, opts...)
			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() { defer wg.Done(); assertGet(t, s, "k", "v") }()
			}
			wg.Wait()
			assertGet(t, s, "k", "v")
			want := int32(1)
			if tiers == 0 {
				want = 2 // nothing holds the value between the burst and the last read
			}
			if n := o.loads.Load(); n != want {
				t.Fatalf("origin loaded %d times, want %d", n, want)
			}
		})
	}
}

// A lost lease stores nothing: a key deleted while its fill was loading is not
// refilled with the value loaded before the delete.
func TestDeleteDuringLoadIsNotOverwritten(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "before")
	o.delay = 100 * time.Millisecond
	shared := cache.NewMemory()
	s := newStack(t, cache.WithTier(shared, time.Hour), cache.WithOrigin(o))

	done := make(chan struct{})
	go func() { defer close(done); _, _ = s.Get(ctx, tenant, "k") }()
	time.Sleep(30 * time.Millisecond) // the load holds the lease now
	o.put("k", "after")
	if err := shared.Share().Delete(ctx, tenant.LayerKey("k")); err != nil { // another process invalidates
		t.Fatal(err)
	}
	<-done
	if _, err := shared.Get(ctx, tenant.LayerKey("k")); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("a fill that lost its lease was stored: %v", err)
	}
	assertGet(t, s, "k", "after")
}

func TestMemoryLimits(t *testing.T) {
	ctx := context.Background()
	m := cache.NewMemory(cache.MaxEntries(2), cache.MaxValueBytes(4))
	if err := m.Set(ctx, "big", cache.Entry{Value: []byte("12345")}, time.Minute); !errors.Is(err, cache.ErrTooLarge) {
		t.Fatalf("oversized Set = %v, want ErrTooLarge", err)
	}
	for _, k := range []string{"a", "b"} {
		mustSet(t, m, k, "v")
	}
	if _, err := m.Get(ctx, "a"); err != nil { // a is now most recently used
		t.Fatal(err)
	}
	mustSet(t, m, "c", "v")
	if _, err := m.Get(ctx, "b"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("least recently used key survived: %v", err)
	}
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

// origin is a real in-process origin: a map with versions, counting loads.
type origin struct {
	mu       sync.Mutex
	values   map[string]string
	versions map[string]int
	loads    atomic.Int32
	notMod   atomic.Int32
	delay    time.Duration
}

func newOrigin() *origin {
	return &origin{values: map[string]string{}, versions: map[string]int{}}
}

func (o *origin) Load(_ context.Context, key, ifNot string) (cache.Entry, error) {
	o.loads.Add(1)
	time.Sleep(o.delay)
	o.mu.Lock()
	defer o.mu.Unlock()
	v, ok := o.values[key]
	if !ok {
		return cache.Entry{}, cache.ErrNotFound
	}
	version := versionOf(o.versions[key])
	if ifNot != "" && ifNot == version {
		o.notMod.Add(1)
		return cache.Entry{}, cache.ErrNotModified
	}
	return cache.Entry{Value: []byte(v), Version: version}, nil
}

func (o *origin) Put(_ context.Context, key string, value []byte) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.values[key] = string(value)
	o.versions[key]++
	return versionOf(o.versions[key]), nil
}

func (o *origin) Remove(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.values, key)
	return nil
}

func (o *origin) put(key, value string) {
	_, _ = o.Put(context.Background(), key, []byte(value))
}

func versionOf(n int) string { return "v" + strings.Repeat("I", n) }

func newStack(t *testing.T, opts ...cache.Option) *cache.Stack {
	t.Helper()
	s, err := cache.New(context.Background(), append([]cache.Option{cache.WithTTLJitter(0)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestPlainCache(t *testing.T) {
	ctx := context.Background()
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute))
	if _, err := s.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get on empty plain cache = %v, want ErrMiss", err)
	}
	if err := s.Set(ctx, tenant, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, s, "k", "v")
	if err := s.Delete(ctx, tenant, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get after Delete = %v, want ErrMiss", err)
	}
}

func TestReadThroughFillsEveryTier(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "v")
	top, bottom := cache.NewMemory(), cache.NewMemory()
	s := newStack(t, cache.WithTier(top, time.Minute), cache.WithTier(bottom, time.Hour), cache.WithOrigin(o))

	assertGet(t, s, "k", "v")
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("origin loaded %d times, want 1", n)
	}
	for name, l := range map[string]cache.Layer{"top": top, "bottom": bottom} {
		if _, err := l.Get(ctx, tenant.LayerKey("k")); err != nil {
			t.Fatalf("%s tier not filled: %v", name, err)
		}
	}

	// A value only the lower tier holds is copied up, never fresher than it
	// was below.
	_ = top.Delete(ctx, tenant.LayerKey("k"))
	assertGet(t, s, "k", "v")
	below, _ := bottom.Get(ctx, tenant.LayerKey("k"))
	above, err := top.Get(ctx, tenant.LayerKey("k"))
	if err != nil {
		t.Fatalf("top tier not refilled from bottom: %v", err)
	}
	if above.FreshUntil.After(below.FreshUntil) {
		t.Fatalf("copy above is fresher (%s) than the entry below (%s)", above.FreshUntil, below.FreshUntil)
	}
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("refill from the bottom tier reached the origin (%d loads)", n)
	}
}

func TestConcurrentMissesLoadOnce(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	o.delay = 50 * time.Millisecond
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o))
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() { defer wg.Done(); assertGet(t, s, "k", "v") }()
	}
	wg.Wait()
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("origin loaded %d times, want 1", n)
	}
}

func TestCallerContextDoesNotCancelSharedLoad(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	o.delay = 100 * time.Millisecond
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o))

	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Get(context.Background(), tenant, "k"); done <- err }()
	if _, err := s.Get(short, tenant, "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("impatient caller got %v, want its own deadline", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("patient caller failed because another gave up: %v", err)
	}
}

func TestNegativeCaching(t *testing.T) {
	o := newOrigin()
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o), cache.WithNegativeTTL(time.Minute))
	for range 3 {
		if _, err := s.Get(context.Background(), tenant, "absent"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get(absent) = %v, want ErrNotFound", err)
		}
	}
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("absent key loaded %d times, want 1", n)
	}

	off := newOrigin()
	s = newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(off), cache.WithNegativeTTL(0))
	for range 3 {
		_, _ = s.Get(context.Background(), tenant, "absent")
	}
	if n := off.loads.Load(); n != 3 {
		t.Fatalf("with negative caching off, absent key loaded %d times, want 3", n)
	}
}

func TestExpiredEntryRevalidates(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	s := newStack(t,
		cache.WithTier(cache.NewMemory(), 50*time.Millisecond),
		cache.WithOrigin(o),
		cache.WithStaleWindow(time.Minute),
	)
	assertGet(t, s, "k", "v")
	time.Sleep(80 * time.Millisecond)
	assertGet(t, s, "k", "v")
	if n := o.notMod.Load(); n != 1 {
		t.Fatalf("expired entry was not revalidated conditionally (%d not-modified answers)", n)
	}

	o.put("k", "v2")
	time.Sleep(80 * time.Millisecond)
	assertGet(t, s, "k", "v2")
}

func TestWritePolicies(t *testing.T) {
	ctx := context.Background()
	t.Run("Invalidate", func(t *testing.T) {
		o := newOrigin()
		o.put("k", "old")
		s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o))
		assertGet(t, s, "k", "old")
		if err := s.Set(ctx, tenant, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}
		assertGet(t, s, "k", "new")
		if n := o.loads.Load(); n != 2 {
			t.Fatalf("loads = %d, want 2: the write should invalidate, then the read reload", n)
		}
	})
	t.Run("Through", func(t *testing.T) {
		o := newOrigin()
		s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o), cache.WithWritePolicy(cache.WriteThrough))
		if err := s.Set(ctx, tenant, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}
		e, err := s.GetEntry(ctx, tenant, "k")
		if err != nil {
			t.Fatal(err)
		}
		if string(e.Value) != "new" || e.Version != versionOf(1) {
			t.Fatalf("GetEntry = %+v, want the written value and its origin version", e)
		}
		if n := o.loads.Load(); n != 0 {
			t.Fatalf("write-through read reached the origin %d times", n)
		}
	})
	t.Run("DeleteRemovesFromOrigin", func(t *testing.T) {
		o := newOrigin()
		o.put("k", "v")
		s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o), cache.WithNegativeTTL(0))
		assertGet(t, s, "k", "v")
		if err := s.Delete(ctx, tenant, "k"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
		}
	})
	t.Run("ReadOnlyOrigin", func(t *testing.T) {
		src := cache.SourceFunc(func(context.Context, string, string) (cache.Entry, error) {
			return cache.Entry{}, cache.ErrNotFound
		})
		s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(src))
		if err := s.Set(ctx, tenant, "k", []byte("v")); !errors.Is(err, cache.ErrReadOnlyOrigin) {
			t.Fatalf("Set over a read-only origin = %v, want ErrReadOnlyOrigin", err)
		}
		if err := s.Delete(ctx, tenant, "k"); !errors.Is(err, cache.ErrReadOnlyOrigin) {
			t.Fatalf("Delete over a read-only origin = %v, want ErrReadOnlyOrigin", err)
		}
	})
}

// brokenLayer fails every call, the way an unreachable shared layer does.
type brokenLayer struct{ calls atomic.Int32 }

var errUnreachable = errors.New("unreachable")

func (b *brokenLayer) Get(context.Context, string) (cache.Entry, error) {
	b.calls.Add(1)
	return cache.Entry{}, errUnreachable
}
func (b *brokenLayer) Set(context.Context, string, cache.Entry, time.Duration) error {
	b.calls.Add(1)
	return errUnreachable
}
func (b *brokenLayer) Delete(context.Context, string) error {
	b.calls.Add(1)
	return errUnreachable
}

func TestFailingLayerDegrades(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	broken := &brokenLayer{}
	s := newStack(t,
		cache.WithTier(broken, time.Minute),
		cache.WithOrigin(o),
		cache.WithBreaker(3, time.Hour),
	)
	for i := range 20 {
		if err := s.Invalidate(context.Background(), tenant, "k"); err == nil && i == 0 {
			t.Fatal("Invalidate hid a failed layer delete")
		}
		assertGet(t, s, "k", "v")
	}
	// Invalidate always tries (20 calls); reads stop trying once the breaker
	// opens after 3 failures (a Get and a Set each count).
	if reads := broken.calls.Load() - 20; reads > 3 {
		t.Fatalf("broken layer was tried %d times on reads after the breaker should have opened", reads)
	}
}

type lookup map[string]string

func (l lookup) Configuration(group, key string) (string, error) { return l.get(group, key) }
func (l lookup) Secret(group, key string) (string, error)        { return l.get(group, key) }
func (l lookup) get(group, key string) (string, error) {
	v, ok := l[group+"."+key]
	if !ok {
		return "", errors.New("not configured: " + group + "." + key)
	}
	return v, nil
}

var (
	openedMu sync.Mutex
	opened   cache.Config
)

// Drivers register once per process, from init, as real ones do.
func init() {
	cache.Register("opentest", func(_ context.Context, cfg cache.Config) (cache.Layer, error) {
		openedMu.Lock()
		defer openedMu.Unlock()
		opened = cfg
		return cache.NewMemory(), nil
	})
}

func TestOpen(t *testing.T) {
	ctx := context.Background()
	l, err := cache.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.(*cache.Memory); !ok {
		t.Fatalf("Open with no provider = %T, want *cache.Memory", l)
	}

	_, err = cache.Open(ctx, lookup{"cache.driver": "nosuch", "cache.connection": "x"})
	if err == nil || !strings.Contains(err.Error(), `driver "nosuch", which is not registered`) || !strings.Contains(err.Error(), "blank-import") {
		t.Fatalf("unregistered driver error = %v, want the driver named and how to register it", err)
	}

	if _, err := cache.Open(ctx, lookup{"cache.driver": "opentest", "cache.connection": "conn://x"}); err != nil {
		t.Fatal(err)
	}
	openedMu.Lock()
	got := opened
	openedMu.Unlock()
	if got != (cache.Config{Driver: "opentest", Connection: "conn://x"}) {
		t.Fatalf("driver received %+v", got)
	}

	if _, err := cache.Open(ctx, lookup{"cache.connection": "x"}); err == nil {
		t.Fatal("Open succeeded without a driver")
	}
}

func TestTyped(t *testing.T) {
	type doc struct {
		Title string `json:"title"`
		Pages int    `json:"pages"`
	}
	ctx := context.Background()
	typed := cache.NewTyped(newStack(t, cache.WithTier(cache.NewMemory(), time.Minute)), cache.JSON[doc]())
	if err := typed.Set(ctx, tenant, "d", doc{Title: "t", Pages: 3}); err != nil {
		t.Fatal(err)
	}
	got, err := typed.Get(ctx, tenant, "d")
	if err != nil {
		t.Fatal(err)
	}
	if got != (doc{Title: "t", Pages: 3}) {
		t.Fatalf("Get = %+v", got)
	}
	if _, err := typed.Get(ctx, cache.Partition{}, "d"); !errors.Is(err, cache.ErrNoPartition) {
		t.Fatalf("Get without a partition = %v, want ErrNoPartition", err)
	}
}

func assertGet(t *testing.T, s *cache.Stack, key, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), tenant, key)
	if err != nil {
		t.Errorf("Get(%q): %v", key, err)
		return
	}
	if string(got) != want {
		t.Errorf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustSet(t *testing.T, l cache.Layer, key, value string) {
	t.Helper()
	if err := l.Set(context.Background(), key, cache.Entry{Value: []byte(value)}, time.Minute); err != nil {
		t.Fatal(err)
	}
}

type viewerKey struct{}

func asViewer(viewer string) context.Context {
	return context.WithValue(context.Background(), viewerKey{}, viewer)
}

// viewerOrigin answers each caller with its own view of a key, as an origin
// that authorizes with the caller's credentials does.
func viewerOrigin(loads *sync.Map, delay time.Duration) cache.Source {
	return cache.SourceFunc(func(ctx context.Context, key, _ string) (cache.Entry, error) {
		viewer, _ := ctx.Value(viewerKey{}).(string)
		n, _ := loads.LoadOrStore(viewer, new(atomic.Int32))
		n.(*atomic.Int32).Add(1)
		time.Sleep(delay)
		return cache.Entry{Value: []byte(key + " for " + viewer)}, nil
	})
}

// Fill-once collapses readers of one key in one partition, in a process and
// across processes, and never hands one partition's load to another.
func TestFillOnceDoesNotCrossPartitions(t *testing.T) {
	var loads sync.Map
	origin := viewerOrigin(&loads, 50*time.Millisecond)
	shared := cache.NewMemory()
	stacks := []*cache.Stack{
		newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithTier(shared, time.Minute), cache.WithOrigin(origin)),
		newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithTier(shared.Share(), time.Minute), cache.WithOrigin(origin)),
	}
	viewers := []string{"u", "v", "w"}
	var wg sync.WaitGroup
	for i := range 60 {
		s, viewer := stacks[i%2], viewers[(i/2)%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Get(asViewer(viewer), cache.NewPartition(viewer), "k")
			if err != nil || string(got) != "k for "+viewer {
				t.Errorf("%s read %q, %v", viewer, got, err)
			}
		}()
	}
	wg.Wait()
	for _, viewer := range viewers {
		n, ok := loads.Load(viewer)
		if !ok || n.(*atomic.Int32).Load() != 1 {
			t.Fatalf("origin loaded %v times for %s, want 1", n, viewer)
		}
	}
}

// A write-around partition writes the origin, drops the partition's cached
// copy, and stores nothing; its reads each load the origin, shared with no
// one.
func TestWriteAroundOverStore(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "old")
	top, bottom := cache.NewMemory(), cache.NewMemory()
	s := newStack(t, cache.WithTier(top, time.Minute), cache.WithTier(bottom, time.Minute), cache.WithOrigin(o),
		cache.WithWritePolicy(cache.WriteThrough))
	granted := cache.NewPartition(tenant.Key(), cache.WriteAround())

	assertGet(t, s, "k", "old")
	if err := s.Set(ctx, granted, "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if top.Len() != 0 || bottom.Len() != 0 {
		t.Fatalf("a write-around write left %d and %d entries in the tiers, want none", top.Len(), bottom.Len())
	}
	assertGet(t, s, "k", "new") // the partition's stale copy was dropped

	o.delay = 30 * time.Millisecond
	before := o.loads.Load()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := s.Get(ctx, granted, "k"); err != nil || string(got) != "new" {
				t.Errorf("write-around read = %q, %v", got, err)
			}
		}()
	}
	wg.Wait()
	if n := o.loads.Load() - before; n != 10 {
		t.Fatalf("10 concurrent write-around reads loaded the origin %d times, want 10: a load under a grant is not shared", n)
	}
	if err := s.Delete(ctx, granted, "k"); err != nil {
		t.Fatal(err)
	}
	if top.Len() != 0 || bottom.Len() != 0 {
		t.Fatalf("a write-around delete left entries in the tiers")
	}
	if _, err := s.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get after a write-around Delete = %v, want ErrNotFound", err)
	}
}

func TestPartitionValue(t *testing.T) {
	if got := cache.NewPartition("tenant").LayerKey("k"); got != "p6:tenant:k" {
		t.Fatalf("partitioned layer key = %q", got)
	}
	if got := cache.NewPartition("tenant", cache.WriteAround()).LayerKey("k"); got != "p6:tenant:k" {
		t.Fatalf("write-around changed the layer key: %q", got)
	}
	if got := cache.Global().LayerKey("k"); got != "g:k" {
		t.Fatalf("global layer key = %q", got)
	}
	if got := (cache.Partition{}).LayerKey("k"); got != "" {
		t.Fatalf("the zero partition has layer key %q, want none", got)
	}
	if cache.NewPartition("a") != cache.NewPartition("a") || cache.NewPartition("a") == cache.NewPartition("a", cache.WriteAround()) {
		t.Fatal("partitions do not compare by key and write-around")
	}
}
