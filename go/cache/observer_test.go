package cache_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// events collects what a stack reports.
type events struct {
	mu   sync.Mutex
	list []cache.Event
}

func (r *events) observe(e cache.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, e)
}

// take returns the events reported since the last take.
func (r *events) take() []cache.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.list
	r.list = nil
	return out
}

// describe spells events as "Op tier:Outcome", in order.
func describe(evs []cache.Event) string {
	parts := make([]string, 0, len(evs))
	for _, e := range evs {
		tier := strconv.Itoa(e.Tier)
		if e.Tier == cache.Origin {
			tier = "origin"
		}
		parts = append(parts, fmt.Sprintf("%s %s:%s", e.Op, tier, e.Outcome))
	}
	return strings.Join(parts, ", ")
}

func wantEvents(t *testing.T, r *events, want string) []cache.Event {
	t.Helper()
	evs := r.take()
	if got := describe(evs); got != want {
		t.Fatalf("reported %q, want %q", got, want)
	}
	return evs
}

// Every tier consulted reports whether it answered, and the origin reports
// the load: an in-process hit and a shared-tier hit are different events.
func TestObserverReportsWhichTierAnswered(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "v")
	top, bottom := cache.NewMemory(), cache.NewMemory()
	var r events
	s := newStack(t, cache.WithTier(top, time.Minute), cache.WithTier(bottom, time.Hour), cache.WithOrigin(o), cache.WithObserver(r.observe))

	assertGet(t, s, "k", "v")
	cold := wantEvents(t, &r, "Get 0:Miss, Get 1:Miss, Get origin:Loaded")
	assertGet(t, s, "k", "v")
	wantEvents(t, &r, "Get 0:Hit")

	stored := cache.EntryKey(s, tenant, "k")
	if evs := r.take(); len(evs) != 0 {
		t.Fatalf("the generation bookkeeping was reported: %s", describe(evs))
	}
	for _, e := range cold {
		if e.Key != "k" || e.LayerKey != stored {
			t.Fatalf("event %+v: want Key %q and LayerKey %q", e, "k", stored)
		}
	}
	_ = top.Delete(ctx, stored)
	assertGet(t, s, "k", "v")
	wantEvents(t, &r, "Get 0:Miss, Get 1:Hit")

	if _, err := cache.New(ctx, cache.WithTier(top, time.Minute), cache.WithObserver(nil)); err == nil {
		t.Fatal("New accepted a nil observer")
	}
}

// With a fill lease, the process that loads the origin reports the load, and
// the one that waited for its fill reports the wait.
func TestObserverReportsLeaseWaitAndLoad(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	o.delay = 150 * time.Millisecond
	shared := cache.NewMemory()
	var ra, rb events
	a := newStack(t, cache.WithTier(shared.Share(), time.Minute), cache.WithOrigin(o), cache.WithMode(cache.FillLease()), cache.WithObserver(ra.observe))
	b := newStack(t, cache.WithTier(shared.Share(), time.Minute), cache.WithOrigin(o), cache.WithMode(cache.FillLease()), cache.WithObserver(rb.observe))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); assertGet(t, a, "k", "v") }()
	time.Sleep(30 * time.Millisecond)
	go func() { defer wg.Done(); assertGet(t, b, "k", "v") }()
	wg.Wait()
	if n := o.loads.Load(); n != 1 {
		t.Fatalf("origin loaded %d times, want 1", n)
	}
	wantEvents(t, &ra, "Get 0:Miss, Get origin:Loaded")
	evs := wantEvents(t, &rb, "Get 0:Miss, Get 0:Waited")
	if waited := evs[1].Elapsed; waited < 50*time.Millisecond {
		t.Fatalf("the wait for another process's fill took %s, want about the load's %s", waited, o.delay)
	}
}

// failing is a layer whose every call fails: a server that cannot be reached.
type failing struct{ err error }

func (f failing) Get(context.Context, string) (cache.Entry, error) { return cache.Entry{}, f.err }
func (f failing) Set(context.Context, string, cache.Entry, time.Duration) error {
	return f.err
}
func (f failing) Delete(context.Context, string) error { return f.err }

// A plain cache answers ErrMiss both when it holds nothing and when its layer
// cannot be reached; the events tell the two apart, and show the breaker
// skipping the layer afterwards.
func TestObserverTellsColdFromDown(t *testing.T) {
	ctx := context.Background()
	var r events
	cold := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithObserver(r.observe))
	if _, err := cold.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("cold Get = %v, want ErrMiss", err)
	}
	wantEvents(t, &r, "Get 0:Miss")

	refused := errors.New("connection refused")
	down := newStack(t, cache.WithTier(failing{refused}, time.Minute), cache.WithBreaker(1, time.Minute), cache.WithObserver(r.observe))
	if _, err := down.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get on a failing layer = %v, want ErrMiss under Degrade", err)
	}
	evs := wantEvents(t, &r, "Get 0:Failed")
	if !errors.Is(evs[0].Err, refused) {
		t.Fatalf("the failure reported %v, want the layer's error", evs[0].Err)
	}
	if _, err := down.Get(ctx, tenant, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get behind an open breaker = %v, want ErrMiss", err)
	}
	wantEvents(t, &r, "Get 0:SkippedBreaker")
}

// A write reports the origin taking it, then each tier taking the key's new
// generation; an invalidation reports each tier; a plain cache reports its
// own layer writes.
func TestObserverReportsWrites(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	var r events
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o), cache.WithObserver(r.observe))
	if err := s.Set(ctx, tenant, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	evs := wantEvents(t, &r, "Set origin:Written, Set 0:Written")
	if evs[0].Key != "k" || evs[0].LayerKey != "" {
		t.Fatalf("the origin write reported %+v: want Key %q and no layer key", evs[0], "k")
	}
	if evs[1].Key != "k" || evs[1].LayerKey != cache.GenerationKey("k") {
		t.Fatalf("the generation write reported %+v: want Key %q under the generation key", evs[1], "k")
	}
	if err := s.Invalidate(ctx, tenant, "k"); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, &r, "Invalidate 0:Written")
	if err := s.Delete(ctx, tenant, "k"); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, &r, "Delete origin:Written, Delete 0:Written")

	plain := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithObserver(r.observe))
	if err := plain.Set(ctx, tenant, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	evs = wantEvents(t, &r, "Set 0:Written")
	if evs[0].LayerKey != cache.EntryKey(plain, tenant, "k") {
		t.Fatalf("the layer write reported %+v: want the layer key of the stored value", evs[0])
	}
	if err := plain.Delete(ctx, tenant, "k"); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, &r, "Delete 0:Written")
}

// A StaleWhileRevalidate refresh is its own operation, so a consumer can tell
// a background revalidation from a read that waited on the origin.
func TestObserverReportsRefreshes(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	var r events
	s := newStack(t,
		cache.WithTier(cache.NewMemory(), 50*time.Millisecond),
		cache.WithOrigin(o),
		cache.WithMode(cache.StaleWhileRevalidate(time.Minute)),
		cache.WithExecutor(func(task func()) { task() }),
		cache.WithObserver(r.observe))
	assertGet(t, s, "k", "v")
	wantEvents(t, &r, "Get 0:Miss, Get origin:Loaded")
	time.Sleep(80 * time.Millisecond)
	assertGet(t, s, "k", "v")
	wantEvents(t, &r, "Get 0:Hit, Refresh origin:NotModified")
}

// An observer that panics must not take the read with it. Every event is
// reported on the goroutine doing the operation, so a nil map written to or a
// type assertion on an unexpected Err would otherwise unwind out through the
// caller's Get and fail a read the cache had answered correctly.
func TestAPanickingObserverDoesNotFailTheRead(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	var seen int
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o),
		cache.WithObserver(func(cache.Event) { seen++; panic("observer blew up") }))

	// The value is served, every time, however often the observer panics.
	assertGet(t, s, "k", "v")
	assertGet(t, s, "k", "v")

	// And the observer is still being called: one panic does not disable it,
	// which would cost every later event for one transient fault.
	if seen < 3 {
		t.Fatalf("the observer was called %d times; it was disabled after panicking", seen)
	}
}

// The panic reaches the consumer through WithObserverErrors, with the event the
// observer was given — so a broken observer is discoverable rather than silent.
func TestAPanickingObserverIsReportedToItsHandler(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	type failure struct {
		ev       cache.Event
		panicked any
	}
	var mu sync.Mutex
	var failures []failure
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o),
		cache.WithObserver(func(cache.Event) { panic("observer blew up") }),
		cache.WithObserverErrors(func(ev cache.Event, panicked any) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, failure{ev, panicked})
		}))

	assertGet(t, s, "k", "v")

	mu.Lock()
	defer mu.Unlock()
	if len(failures) == 0 {
		t.Fatal("a panicking observer was not reported to the handler")
	}
	if failures[0].panicked != "observer blew up" {
		t.Fatalf("the handler got %v, want what the observer panicked with", failures[0].panicked)
	}
	if failures[0].ev.Key != "k" {
		t.Fatalf("the handler got event %+v, want the one the observer was given", failures[0].ev)
	}
	if _, err := cache.New(context.Background(),
		cache.WithTier(cache.NewMemory(), time.Minute), cache.WithObserverErrors(nil)); err == nil {
		t.Fatal("New accepted a nil observer-error handler")
	}
}

// A handler that panics in turn is contained the same way: one broken callback
// cannot reach the caller through the other.
func TestAPanickingObserverErrorHandlerIsContainedToo(t *testing.T) {
	o := newOrigin()
	o.put("k", "v")
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o),
		cache.WithObserver(func(cache.Event) { panic("observer blew up") }),
		cache.WithObserverErrors(func(cache.Event, any) { panic("handler blew up too") }))
	assertGet(t, s, "k", "v")
}
