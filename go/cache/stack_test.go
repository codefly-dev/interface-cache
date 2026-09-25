package cache_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
	"github.com/codefly-dev/interface-cache/go/cache/cachetest"
)

func TestMemoryConformance(t *testing.T) {
	cachetest.Run(t, cachetest.Harness{
		New: func(*testing.T, string) cache.Layer { return cache.NewMemory() },
	})
}

func TestMemoryStackFillOnce(t *testing.T) {
	cachetest.RunStack(t, cachetest.Harness{
		New: func(*testing.T, string) cache.Layer { return cache.NewMemory() },
	})
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
	if _, err := s.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get on empty plain cache = %v, want ErrMiss", err)
	}
	if err := s.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, s, "k", "v")
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
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
		if _, err := l.Get(ctx, "k"); err != nil {
			t.Fatalf("%s tier not filled: %v", name, err)
		}
	}

	// A value only the lower tier holds is copied up, never fresher than it
	// was below.
	_ = top.Delete(ctx, "k")
	assertGet(t, s, "k", "v")
	below, _ := bottom.Get(ctx, "k")
	above, err := top.Get(ctx, "k")
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
	go func() { _, err := s.Get(context.Background(), "k"); done <- err }()
	if _, err := s.Get(short, "k"); !errors.Is(err, context.DeadlineExceeded) {
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
		if _, err := s.Get(context.Background(), "absent"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get(absent) = %v, want ErrNotFound", err)
		}
	}
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("absent key loaded %d times, want 1", n)
	}

	off := newOrigin()
	s = newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(off), cache.WithNegativeTTL(0))
	for range 3 {
		_, _ = s.Get(context.Background(), "absent")
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
		if err := s.Set(ctx, "k", []byte("new")); err != nil {
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
		if err := s.Set(ctx, "k", []byte("new")); err != nil {
			t.Fatal(err)
		}
		e, err := s.GetEntry(ctx, "k")
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
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "k"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
		}
	})
	t.Run("ReadOnlyOrigin", func(t *testing.T) {
		src := cache.SourceFunc(func(context.Context, string, string) (cache.Entry, error) {
			return cache.Entry{}, cache.ErrNotFound
		})
		s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(src))
		if err := s.Set(ctx, "k", []byte("v")); !errors.Is(err, cache.ErrReadOnlyOrigin) {
			t.Fatalf("Set over a read-only origin = %v, want ErrReadOnlyOrigin", err)
		}
		if err := s.Delete(ctx, "k"); !errors.Is(err, cache.ErrReadOnlyOrigin) {
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
		if err := s.Invalidate(context.Background(), "k"); err == nil && i == 0 {
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
	if err == nil || !strings.Contains(err.Error(), `_ "github.com/codefly-dev/interface-cache/go/nosuch"`) {
		t.Fatalf("unregistered driver error = %v, want a hint naming the import", err)
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
	if err := typed.Set(ctx, "d", doc{Title: "t", Pages: 3}); err != nil {
		t.Fatal(err)
	}
	got, err := typed.Get(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if got != (doc{Title: "t", Pages: 3}) {
		t.Fatalf("Get = %+v", got)
	}
}

func assertGet(t *testing.T, s *cache.Stack, key, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), key)
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
