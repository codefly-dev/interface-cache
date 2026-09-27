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
// processes hold clients of one server. An external write goes through a
// client no stack uses, as another program would.
func sharedMemory() cachetest.Harness {
	return memoryHarness(func(m *cache.Memory) cache.Layer { return m })
}

// memoryHarness is sharedMemory with each client wrapped by wrap.
func memoryHarness(wrap func(*cache.Memory) cache.Layer) cachetest.Harness {
	var mu sync.Mutex
	stores := map[string]*cache.Memory{}
	store := func(ns string) *cache.Memory {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := stores[ns]; !ok {
			stores[ns] = cache.NewMemory()
		}
		return stores[ns]
	}
	return cachetest.Harness{
		New: func(_ *testing.T, ns string) cache.Layer { return wrap(store(ns).Share()) },
		ExternalWrite: func(t *testing.T, ns, key string) {
			if err := store(ns).Share().Set(context.Background(), key, cache.Entry{Value: []byte("external")}, time.Minute); err != nil {
				t.Fatal(err)
			}
		},
	}
}

// tenant is the partition the tests that are not about partitioning read in.
var tenant = cache.NewPartition("tenant")

func TestMemoryConformance(t *testing.T) { cachetest.Run(t, sharedMemory()) }

// echoMemory reports its own client's writes too, which the Notifier contract
// allows: it subscribes through another client of the store.
type echoMemory struct{ *cache.Memory }

func (e echoMemory) Subscribe(ctx context.Context, fn func(string)) (func(), error) {
	return e.Share().Subscribe(ctx, fn)
}

// A driver that reports its own writes passes the suite, and the stack over it
// behaves the same.
func TestEchoingNotifierConformance(t *testing.T) {
	h := memoryHarness(func(m *cache.Memory) cache.Layer { return echoMemory{m} })
	cachetest.Run(t, h)
	cachetest.RunStack(t, h)
}

// lossyMemory is a Resyncer over Memory: during a gap it drops the notices
// that arrive, then signals the resync, as a driver whose connection dropped
// would.
type lossyMemory struct {
	*cache.Memory
	mu      *sync.Mutex
	dropped *bool
	gaps    *[][2]func()
}

func newLossyMemory(m *cache.Memory) lossyMemory {
	return lossyMemory{Memory: m, mu: &sync.Mutex{}, dropped: new(bool), gaps: new([][2]func())}
}

func (l lossyMemory) Subscribe(ctx context.Context, fn func(string)) (func(), error) {
	return l.Memory.Subscribe(ctx, func(key string) {
		l.mu.Lock()
		lost := *l.dropped
		l.mu.Unlock()
		if !lost {
			fn(key)
		}
	})
}

func (l lossyMemory) Capabilities() cache.Capabilities {
	c := l.Memory.Capabilities()
	c.Resyncs = true
	return c
}

func (l lossyMemory) SubscribeGaps(_ context.Context, lost, resynced func()) (func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.gaps = append(*l.gaps, [2]func(){lost, resynced})
	return func() {}, nil
}

// interrupt opens a gap, runs lose inside it, then closes it. It reports the
// gap only once it is over when late is set, the way a driver that notices
// the loss only on reconnecting would.
func (l lossyMemory) interrupt(lose func(), late ...bool) {
	l.mu.Lock()
	*l.dropped = true
	gaps := append([][2]func(){}, *l.gaps...)
	l.mu.Unlock()
	if len(late) == 0 {
		for _, g := range gaps {
			g[0]()
		}
	}
	lose()
	l.mu.Lock()
	*l.dropped = false
	l.mu.Unlock()
	for _, g := range gaps {
		if len(late) > 0 {
			g[0]()
		}
		g[1]()
	}
}

func lossyHarness() cachetest.Harness {
	h := memoryHarness(func(m *cache.Memory) cache.Layer { return newLossyMemory(m) })
	h.Interrupt = func(_ *testing.T, l cache.Layer) { l.(lossyMemory).interrupt(func() {}) }
	return h
}

// The resync contract cases run against a Resyncer, and pass.
func TestResyncerConformance(t *testing.T) {
	cachetest.Run(t, lossyHarness())
	cachetest.RunStack(t, lossyHarness())
}

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

// A write that lands while a fill is loading, through another process and
// another partition, is not undone by that fill: the fill is stored under the
// generation the write replaced, so the next read loads the new value.
func TestWriteDuringLoadIsNotOverwritten(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "before")
	o.delay = 100 * time.Millisecond
	shared := cache.NewMemory()
	s := newStack(t, cache.WithTier(shared, time.Hour), cache.WithOrigin(o))
	other := newStack(t, cache.WithTier(shared.Share(), time.Hour), cache.WithOrigin(o))

	done := make(chan struct{})
	go func() { defer close(done); _, _ = s.Get(ctx, tenant, "k") }()
	time.Sleep(30 * time.Millisecond) // the load is running now
	if err := other.Set(ctx, cache.NewPartition("someone-else"), "k", []byte("after")); err != nil {
		t.Fatal(err)
	}
	<-done
	assertGet(t, s, "k", "after")
	assertGet(t, other, "k", "after")
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
	stored := cache.EntryKey(s, tenant, "k")
	for name, l := range map[string]cache.Layer{"top": top, "bottom": bottom} {
		if _, err := l.Get(ctx, stored); err != nil {
			t.Fatalf("%s tier not filled: %v", name, err)
		}
	}

	// A value only the lower tier holds is copied up, never fresher than it
	// was below.
	_ = top.Delete(ctx, stored)
	assertGet(t, s, "k", "v")
	below, _ := bottom.Get(ctx, stored)
	above, err := top.Get(ctx, stored)
	if err != nil {
		t.Fatalf("top tier not refilled from bottom: %v", err)
	}
	if !above.Confirmed.Equal(below.Confirmed) {
		t.Fatalf("copy above is confirmed at %s, the entry below at %s: a copy keeps its confirmation", above.Confirmed, below.Confirmed)
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
		s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o), cache.WithMode(cache.WriteThrough()))
		if err := s.Set(ctx, tenant, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}
		// The write checks its copy is current with one conditional load,
		// answered not-modified; the read after it is a hit.
		if n, notMod := o.loads.Load(), o.notMod.Load(); n != 1 || notMod != 1 {
			t.Fatalf("the write-through made %d loads (%d not modified), want one conditional check", n, notMod)
		}
		e, err := s.GetEntry(ctx, tenant, "k")
		if err != nil {
			t.Fatal(err)
		}
		if string(e.Value) != "new" || e.Version != versionOf(1) {
			t.Fatalf("GetEntry = %+v, want the written value and its origin version", e)
		}
		if n := o.loads.Load(); n != 1 {
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
		newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithTier(shared, time.Minute), cache.WithOrigin(origin), cache.WithMode(cache.FillLease())),
		newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithTier(shared.Share(), time.Minute), cache.WithOrigin(origin), cache.WithMode(cache.FillLease())),
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
		cache.WithMode(cache.WriteThrough()))
	granted := cache.NewPartition("tenant", cache.WriteAround())
	assertStoresNoCopy := func(what string) {
		t.Helper()
		stored := cache.EntryKey(s, tenant, "k")
		for name, l := range map[string]*cache.Memory{"top": top, "bottom": bottom} {
			if e, err := l.Get(ctx, stored); !errors.Is(err, cache.ErrMiss) {
				t.Fatalf("a write-around %s stored %q in the %s tier", what, e.Value, name)
			}
		}
	}

	assertGet(t, s, "k", "old")
	if err := s.Set(ctx, granted, "k", []byte("new")); err != nil {
		t.Fatal(err)
	}
	assertStoresNoCopy("write")
	assertGet(t, s, "k", "new") // the partition's stale copy is no longer read

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
	assertStoresNoCopy("delete")
	if _, err := s.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get after a write-around Delete = %v, want ErrNotFound", err)
	}
}

// A notice lost in a gap leaves a stale copy above the Resyncer; the resync
// that follows flushes it, in every partition and generation.
func TestResyncFlushesWhatAGapLost(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "old")
	shared := cache.NewMemory()
	lossy := newLossyMemory(shared.Share())
	top := cache.NewMemory()
	s := newStack(t, cache.WithTier(top, time.Hour), cache.WithTier(lossy, time.Hour), cache.WithOrigin(o))
	writer := newStack(t, cache.WithTier(shared.Share(), time.Hour), cache.WithOrigin(o))
	other := cache.NewPartition("other")
	assertGet(t, s, "k", "old")
	if got, err := s.Get(ctx, other, "k"); err != nil || string(got) != "old" {
		t.Fatalf("Get = %q, %v", got, err)
	}

	lossy.interrupt(func() {
		if err := writer.Set(ctx, tenant, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}
		assertGet(t, s, "k", "old") // the notice was lost, and the gap not yet reported: the stale copy is served
	}, true)
	if top.Len() != 0 {
		t.Fatalf("the resync left %d entries in the tier above", top.Len())
	}
	assertGet(t, s, "k", "new")
	if got, err := s.Get(ctx, other, "k"); err != nil || string(got) != "new" {
		t.Fatalf("other partition after the resync = %q, %v", got, err)
	}
}

// A fill whose read began before a resync stores nothing in the flushed tier:
// what it read may be what the gap left stale.
func TestFillDuringResyncIsNotStoredAbove(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	o.delay = 100 * time.Millisecond
	lossy := newLossyMemory(cache.NewMemory())
	top := cache.NewMemory()
	s := newStack(t, cache.WithTier(top, time.Hour), cache.WithTier(lossy, time.Hour), cache.WithOrigin(o))

	done := make(chan struct{})
	go func() { defer close(done); assertGet(t, s, "k", "v") }()
	time.Sleep(30 * time.Millisecond) // the origin load is running
	lossy.interrupt(func() {})
	<-done
	if e, err := top.Get(context.Background(), cache.EntryKey(s, tenant, "k")); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("a fill that began before the resync was stored above it: %q, %v", e.Value, err)
	}
}

// A resync cannot name keys, so every tier above a Resyncer must be flushable.
func TestNewRefusesUnflushableTierAboveResyncer(t *testing.T) {
	_, err := cache.New(context.Background(),
		cache.WithTier(&brokenLayer{}, time.Minute),
		cache.WithTier(newLossyMemory(cache.NewMemory()), time.Minute),
	)
	if err == nil || !strings.Contains(err.Error(), "cannot be flushed") {
		t.Fatalf("New = %v, want a refusal naming the unflushable tier", err)
	}
}

// gatedLayer holds a Get of a copy that misses, for the caller whose context
// carries hold, until release is closed: that reader has missed, and is slow
// to act on it.
type gatedLayer struct {
	*cache.Memory
	hold    any
	missed  chan struct{}
	release chan struct{}
}

func (g gatedLayer) Get(ctx context.Context, key string) (cache.Entry, error) {
	e, err := g.Memory.Get(ctx, key)
	if errors.Is(err, cache.ErrMiss) && ctx.Value(g.hold) != nil {
		close(g.missed)
		<-g.release
	}
	return e, err
}

// The fill-once race reported from service-redis#79: a reader misses just
// before another process fills, and reaches its load only after its own
// process's flight for the key has finished, so it starts a new flight whose
// lease is free — the fill released it. The lease holder re-reads the key
// before loading, so the origin is still loaded once.
func TestLateMissAfterAnotherProcessFilledLoadsOnce(t *testing.T) {
	type holdKey struct{}
	o := newOrigin()
	o.put("k", "v")
	o.delay = 100 * time.Millisecond
	shared := cache.NewMemory()
	gate := gatedLayer{Memory: shared.Share(), hold: holdKey{}, missed: make(chan struct{}), release: make(chan struct{})}
	a := newStack(t, cache.WithTier(shared, time.Hour), cache.WithOrigin(o), cache.WithMode(cache.FillLease()))
	b := newStack(t, cache.WithTier(cache.NewMemory(), time.Hour), cache.WithTier(gate, time.Hour), cache.WithOrigin(o), cache.WithMode(cache.FillLease()))

	// Both processes learn the generation first, so the late reader's miss is
	// on the copy.
	if _, err := b.Get(context.Background(), cache.NewPartition("warm"), "k"); err != nil {
		t.Fatal(err)
	}
	loads := o.loads.Load()

	aDone := make(chan struct{})
	go func() { defer close(aDone); assertGet(t, a, "k", "v") }() // a takes the lease and loads
	time.Sleep(20 * time.Millisecond)
	late := make(chan struct{})
	go func() {
		defer close(late)
		ctx := context.WithValue(context.Background(), holdKey{}, true)
		if got, err := b.Get(ctx, tenant, "k"); err != nil || string(got) != "v" {
			t.Errorf("late reader = %q, %v", got, err)
		}
	}()
	<-gate.missed
	assertGet(t, b, "k", "v") // b's own flight waits for a's fill, and finishes
	<-aDone
	close(gate.release) // the late reader reaches load now, after a's fill
	<-late
	if n := o.loads.Load() - loads; n != 1 {
		t.Fatalf("origin loaded %d times, want 1", n)
	}
}

// hookedMemory runs afterSet once a Set of a key it names has landed.
type hookedMemory struct {
	*cache.Memory
	afterSet func(key string)
}

func (h hookedMemory) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	err := h.Memory.Set(ctx, key, e, ttl)
	if h.afterSet != nil {
		h.afterSet(key)
	}
	return err
}

// A write stores bottom-up. When another process's write, and its notice,
// land between this write's store in the shared tier and in the tier above,
// the tier above must not keep this write's value over the newer one: a plain
// cache's value, here, is what the tier holds.
func TestWriteRacingANoticeDoesNotLeaveItsValueAbove(t *testing.T) {
	ctx := context.Background()
	store := cache.NewMemory()
	other := newStack(t, cache.WithTier(cache.NewMemory(), time.Hour), cache.WithTier(store.Share(), time.Hour))
	var once sync.Once
	hooked := hookedMemory{Memory: store.Share()}
	hooked.afterSet = func(string) {
		once.Do(func() {
			if err := other.Set(ctx, tenant, "k", []byte("newer")); err != nil {
				t.Error(err)
			}
		})
	}
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Hour), cache.WithTier(hooked, time.Hour), cache.WithMode(cache.InvalidateOnWrite()))
	if err := s.Set(ctx, tenant, "k", []byte("older")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, s, "k", "newer")
}
