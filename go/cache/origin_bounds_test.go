package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// testClock is a clock the test moves; a stack waiting on it waits in
// virtual time.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Sleep(ctx context.Context, d time.Duration) error {
	c.Advance(d)
	return ctx.Err()
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// marking is o with every value it loads carrying the bound expires returns
// and the NoStore flag given, the way an origin that knows them sets them.
func marking(o *origin, expires func() time.Time, noStore bool) cache.Source {
	return cache.SourceFunc(func(ctx context.Context, key, ifNot string) (cache.Entry, error) {
		e, err := o.Load(ctx, key, ifNot)
		if err != nil {
			return e, err
		}
		if expires != nil {
			e.Expires = expires()
		}
		e.NoStore = noStore
		return e, nil
	})
}

// A value the origin marks NoStore reaches the caller, flag included, is in no
// tier afterwards, and the next read loads the origin again.
func TestOriginNoStoreIsServedAndKeptNowhere(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "v")
	top, bottom := cache.NewMemory(), cache.NewMemory()
	s := newStack(t, cache.WithTier(top, time.Minute), cache.WithTier(bottom, time.Hour), cache.WithOrigin(marking(o, nil, true)))

	e, err := s.GetEntry(ctx, tenant, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(e.Value) != "v" || !e.NoStore {
		t.Fatalf("GetEntry = %+v, want the value with NoStore set", e)
	}
	stored := cache.EntryKey(s, tenant, "k")
	for name, l := range map[string]cache.Layer{"top": top, "bottom": bottom} {
		if got, err := l.Get(ctx, stored); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("%s tier holds %+v (%v) after a NoStore load; want nothing", name, got, err)
		}
	}
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 2 {
		t.Fatalf("origin loaded %d times, want 2: a NoStore value is served from no tier", n)
	}
}

// Concurrent misses of a NoStore key still share one load in the process,
// and every caller gets the value.
func TestOriginNoStoreSharesTheLoad(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	o.delay = 50 * time.Millisecond
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(marking(o, nil, true)))
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); assertGet(t, s, "k", "v") }()
	}
	wg.Wait()
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("origin loaded %d times, want 1: NoStore does not undo singleflight", n)
	}
}

// Under FillLease a NoStore load stores nothing in the leasing tier either,
// and gives the lease back rather than leaving the key leased until the
// lease expires.
func TestOriginNoStoreReleasesTheLease(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "v")
	shared := cache.NewMemory()
	s := newStack(t, cache.WithTier(shared.Share(), time.Minute), cache.WithOrigin(marking(o, nil, true)), cache.WithMode(cache.FillLease()))
	assertGet(t, s, "k", "v")
	stored := cache.EntryKey(s, tenant, "k")
	if got, err := shared.Get(ctx, stored); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("the leasing tier holds %+v (%v) after a NoStore load; want nothing", got, err)
	}
	if _, ok, err := shared.Acquire(ctx, stored, time.Minute); err != nil || !ok {
		t.Fatalf("Acquire after a NoStore load = (ok %v, %v): the lease was not released", ok, err)
	}
}

// An origin's Expires narrows a tier's TTL: every tier drops the copy at the
// bound, and a read at the bound loads the origin, where the tiers alone
// would have served the copy for their whole TTL. A value whose bound has
// passed by the time it arrives is served to the caller and stored nowhere.
func TestOriginExpiresClampsTheTierTTL(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	o := newOrigin()
	o.put("k", "v")
	top := cache.NewMemory(cache.MemoryClock(clock.Now))
	bottom := cache.NewMemory(cache.MemoryClock(clock.Now))
	bound := clock.Now().Add(10 * time.Second)
	s := newStack(t, cache.WithClock(clock),
		cache.WithTier(top, time.Minute), cache.WithTier(bottom, time.Hour),
		cache.WithOrigin(marking(o, func() time.Time { return bound }, false)))

	assertGet(t, s, "k", "v")
	stored := cache.EntryKey(s, tenant, "k")
	clock.Advance(10*time.Second - time.Millisecond)
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("a copy inside the origin's bound reached the origin (%d loads)", n)
	}
	clock.Advance(time.Millisecond)
	for name, l := range map[string]cache.Layer{"top": top, "bottom": bottom} {
		if _, err := l.Get(ctx, stored); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("%s tier kept the copy past the origin's bound (%v): the tier's TTL was not clamped to it", name, err)
		}
	}
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 2 {
		t.Fatalf("a read at the origin's bound did not reload (%d loads)", n)
	}
	// The reload carries a bound already passed: stale on arrival, so served
	// and kept nowhere.
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 3 {
		t.Fatalf("a value stale on arrival was served from a tier (%d loads)", n)
	}
}

// An origin's Expires never extends a tier's TTL: under a shorter tier the
// copy still goes at the tier's TTL.
func TestOriginExpiresDoesNotExtendTheTierTTL(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	o := newOrigin()
	o.put("k", "v")
	top := cache.NewMemory(cache.MemoryClock(clock.Now))
	s := newStack(t, cache.WithClock(clock), cache.WithTier(top, 10*time.Second),
		cache.WithOrigin(marking(o, func() time.Time { return clock.Now().Add(time.Hour) }, false)))
	assertGet(t, s, "k", "v")
	stored := cache.EntryKey(s, tenant, "k")
	clock.Advance(10 * time.Second)
	if _, err := top.Get(ctx, stored); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("the tier kept the copy past its own TTL (%v): the origin's bound replaced the tier's TTL instead of clamping it", err)
	}
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 2 {
		t.Fatalf("a read past the tier's TTL did not reload (%d loads)", n)
	}
}

// A copy filled from a lower tier keeps the origin's bound, so the tier above
// drops it at the bound too, never at its own TTL.
func TestOriginExpiresTravelsUpTheStack(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	o := newOrigin()
	o.put("k", "v")
	top := cache.NewMemory(cache.MemoryClock(clock.Now))
	bottom := cache.NewMemory(cache.MemoryClock(clock.Now))
	bound := clock.Now().Add(10 * time.Second)
	s := newStack(t, cache.WithClock(clock),
		cache.WithTier(top, time.Minute), cache.WithTier(bottom, time.Hour),
		cache.WithOrigin(marking(o, func() time.Time { return bound }, false)))
	assertGet(t, s, "k", "v")
	stored := cache.EntryKey(s, tenant, "k")
	_ = top.Delete(ctx, stored)
	clock.Advance(5 * time.Second)
	assertGet(t, s, "k", "v")
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("a refill from the bottom tier reached the origin (%d loads)", n)
	}
	above, err := top.Get(ctx, stored)
	if err != nil {
		t.Fatalf("top tier not refilled from bottom: %v", err)
	}
	if !above.Expires.Equal(bound) {
		t.Fatalf("the copy above expires at %s, the origin's bound is %s", above.Expires, bound)
	}
	clock.Advance(5 * time.Second)
	if _, err := top.Get(ctx, stored); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("the copy above outlived the origin's bound (%v)", err)
	}
}

// ErrNotModified renews a copy's confirmation and leaves its bound alone, so a
// copy past its bound is revalidated on every read; an ErrNotModified that
// carries a new Expires renews the bound, and the copy is served until it.
func TestNotModifiedRenewsTheOriginsBound(t *testing.T) {
	clock := newTestClock()
	o := newOrigin()
	o.put("k", "v")
	var mu sync.Mutex
	var renewed time.Time
	src := cache.SourceFunc(func(ctx context.Context, key, ifNot string) (cache.Entry, error) {
		e, err := o.Load(ctx, key, ifNot)
		if errors.Is(err, cache.ErrNotModified) {
			mu.Lock()
			defer mu.Unlock()
			return cache.Entry{Expires: renewed}, err
		}
		if err != nil {
			return e, err
		}
		e.Expires = clock.Now().Add(10 * time.Second)
		return e, nil
	})
	top := cache.NewMemory(cache.MemoryClock(clock.Now))
	s := newStack(t, cache.WithClock(clock), cache.WithTier(top, time.Minute), cache.WithStaleWindow(time.Hour), cache.WithOrigin(src))

	assertGet(t, s, "k", "v")
	clock.Advance(10 * time.Second)
	assertGet(t, s, "k", "v")
	assertGet(t, s, "k", "v")
	if n := o.notMod.Load(); n != 2 {
		t.Fatalf("a copy past its bound was revalidated %d times over two reads, want 2: ErrNotModified alone does not renew the bound", n)
	}
	mu.Lock()
	renewed = clock.Now().Add(10 * time.Second)
	mu.Unlock()
	assertGet(t, s, "k", "v")
	notMod := o.notMod.Load()
	clock.Advance(5 * time.Second)
	assertGet(t, s, "k", "v")
	if n := o.notMod.Load(); n != notMod {
		t.Fatalf("a copy inside its renewed bound was revalidated (%d, was %d)", n, notMod)
	}
	if n := o.loads.Load(); n != 1+int32(notMod) {
		t.Fatalf("origin loaded %d times, want %d: every revalidation was conditional", n, 1+notMod)
	}
}
