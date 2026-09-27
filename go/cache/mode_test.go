package cache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// plainLayer is a Layer with no capability: it declares nothing.
type plainLayer struct{ m *cache.Memory }

func (p plainLayer) Get(ctx context.Context, k string) (cache.Entry, error) { return p.m.Get(ctx, k) }
func (p plainLayer) Set(ctx context.Context, k string, e cache.Entry, ttl time.Duration) error {
	return p.m.Set(ctx, k, e, ttl)
}
func (p plainLayer) Delete(ctx context.Context, k string) error { return p.m.Delete(ctx, k) }

// unboundedNotifier notifies but declares no NoticeBound.
type unboundedNotifier struct{ *cache.Memory }

func (u unboundedNotifier) Capabilities() cache.Capabilities {
	c := u.Memory.Capabilities()
	c.NoticeBound = 0
	return c
}

// liar declares leases it does not grant.
type liar struct{ plainLayer }

func (liar) Capabilities() cache.Capabilities { return cache.Capabilities{Leases: true} }

// declaredOrigin is origin with capabilities declared.
type declaredOrigin struct {
	*origin
	caps cache.Capabilities
}

func (d declaredOrigin) Capabilities() cache.Capabilities { return d.caps }

// Construction refuses every mode its layers and origin cannot keep, and the
// refusal names the capability that is missing.
func TestConstructionRefusesUnsupportedModes(t *testing.T) {
	readOnly := cache.SourceFunc(func(context.Context, string, string) (cache.Entry, error) { return cache.Entry{}, cache.ErrNotFound })
	conditional := declaredOrigin{newOrigin(), cache.Capabilities{ConditionalLoads: true}}
	sequenced := declaredOrigin{newOrigin(), cache.Capabilities{ConditionalLoads: true, Sequenced: true}}
	memory := func() cache.Option { return cache.WithTier(cache.NewMemory(), time.Minute) }
	plain := func() cache.Option { return cache.WithTier(plainLayer{cache.NewMemory()}, time.Minute) }
	for _, c := range []struct {
		name  string
		opts  []cache.Option
		names []string // every one must appear in the error
	}{
		{"InvalidateOnWrite without a notifier below", []cache.Option{memory(), plain(), cache.WithOrigin(newOrigin()), cache.WithMode(cache.InvalidateOnWrite())},
			[]string{"InvalidateOnWrite", "tier 0", "Notifies"}},
		{"InvalidateOnWrite over a notifier with no bound", []cache.Option{memory(), cache.WithTier(unboundedNotifier{cache.NewMemory()}, time.Minute), cache.WithOrigin(newOrigin()), cache.WithMode(cache.InvalidateOnWrite())},
			[]string{"InvalidateOnWrite", "NoticeBound"}},
		{"InvalidateOnWrite with WriteOriginOnly", []cache.Option{memory(), cache.WithOrigin(newOrigin()), cache.WithMode(cache.InvalidateOnWrite(), cache.WriteOriginOnly())},
			[]string{"WriteOriginOnly", "invalidate nothing"}},
		{"FillLease without a leasing tier", []cache.Option{plain(), cache.WithOrigin(newOrigin()), cache.WithMode(cache.FillLease())},
			[]string{"FillLease", "Leases"}},
		{"FillVersionFenced over an unsequenced origin", []cache.Option{memory(), cache.WithOrigin(conditional), cache.WithMode(cache.FillVersionFenced())},
			[]string{"FillVersionFenced", "Sequenced"}},
		{"FillVersionFenced over a tier that cannot fence", []cache.Option{memory(), plain(), cache.WithOrigin(sequenced), cache.WithMode(cache.FillVersionFenced())},
			[]string{"FillVersionFenced", "tier 1", "Fences"}},
		{"FillVersionFenced without an origin", []cache.Option{memory(), cache.WithMode(cache.FillVersionFenced())},
			[]string{"FillVersionFenced", "origin"}},
		{"Validated over an origin without conditional loads", []cache.Option{memory(), cache.WithOrigin(newOrigin()), cache.WithMode(cache.Validated())},
			[]string{"Validated", "ConditionalLoads"}},
		{"Validated without an origin", []cache.Option{memory(), cache.WithMode(cache.Validated())}, []string{"Validated", "origin"}},
		{"Bypass without an origin", []cache.Option{memory(), cache.WithMode(cache.Bypass())}, []string{"Bypass", "origin"}},
		{"StaleWhileRevalidate without an origin", []cache.Option{memory(), cache.WithMode(cache.StaleWhileRevalidate(time.Second))}, []string{"StaleWhileRevalidate", "origin"}},
		{"StaleWhileRevalidate with no window", []cache.Option{memory(), cache.WithOrigin(newOrigin()), cache.WithMode(cache.StaleWhileRevalidate(0))}, []string{"StaleWhileRevalidate", "positive"}},
		{"TTLBounded with no bound", []cache.Option{memory(), cache.WithMode(cache.TTLBounded(0))}, []string{"TTLBounded", "positive"}},
		{"ReadYourWrites over a read-only origin", []cache.Option{memory(), cache.WithOrigin(readOnly), cache.WithMode(cache.ReadYourWrites())}, []string{"ReadYourWrites", "Store"}},
		{"WriteBehind over a read-only origin", []cache.Option{memory(), cache.WithOrigin(readOnly), cache.WithMode(cache.WriteBehind(cache.AcceptWriteLoss))}, []string{"WriteBehind", "read-only"}},
		{"WriteBehind without an origin", []cache.Option{memory(), cache.WithMode(cache.WriteBehind(cache.AcceptWriteLoss))}, []string{"WriteBehind", "none"}},
		{"WriteOriginOnly without an origin", []cache.Option{memory(), cache.WithMode(cache.WriteOriginOnly())}, []string{"WriteOriginOnly", "origin"}},
		{"a key space's mode", []cache.Option{memory(), cache.WithOrigin(newOrigin()), cache.WithKeySpace("org-settings/*", cache.Validated())},
			[]string{`key space "org-settings/*"`, "Validated", "ConditionalLoads"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, err := cache.New(context.Background(), c.opts...)
			if err == nil {
				s.Close()
				t.Fatal("New accepted a mode its layers and origin cannot keep")
			}
			if !errors.Is(err, cache.ErrUnsupportedMode) {
				t.Fatalf("New = %v, want ErrUnsupportedMode", err)
			}
			for _, name := range c.names {
				if !strings.Contains(err.Error(), name) {
					t.Fatalf("the refusal %q does not name %q", err, name)
				}
			}
		})
	}

	// A declaration the implementation does not back is refused.
	if _, err := cache.New(context.Background(), cache.WithTier(liar{plainLayer{cache.NewMemory()}}, time.Minute)); err == nil ||
		!strings.Contains(err.Error(), "declares Leases but does not implement cache.Leaser") {
		t.Fatalf("New over a layer declaring what it does not implement = %v", err)
	}
	// An implementation the layer does not declare is not used: no leases.
	undeclared := plainLeaser{cache.NewMemory()}
	if _, err := cache.New(context.Background(), cache.WithTier(undeclared, time.Minute), cache.WithOrigin(newOrigin()), cache.WithMode(cache.FillLease())); !errors.Is(err, cache.ErrUnsupportedMode) {
		t.Fatalf("FillLease over a Leaser that does not declare Leases = %v, want a refusal", err)
	}
}

// plainLeaser implements Leaser but declares nothing.
type plainLeaser struct{ *cache.Memory }

func (plainLeaser) Capabilities() cache.Capabilities { return cache.Capabilities{} }

// Each mode needs what its refusal names, and nothing more: the reference
// backend and a sequenced, conditional origin keep every one.
func TestMemoryKeepsEveryMode(t *testing.T) {
	origin := declaredOrigin{newOrigin(), cache.Capabilities{ConditionalLoads: true, Sequenced: true}}
	for _, m := range []cache.ModeOption{
		cache.TTLBounded(time.Second), cache.StaleWhileRevalidate(time.Second), cache.InvalidateOnWrite(), cache.ReadYourWrites(),
		cache.Validated(), cache.Bypass(), cache.WriteInvalidate(), cache.WriteThrough(), cache.WriteOriginOnly(),
		cache.WriteBehind(cache.AcceptWriteLoss), cache.FillUncoordinated(), cache.FillSingleflight(), cache.FillLease(),
		cache.FillVersionFenced(), cache.Degrade(), cache.FailClosed(),
	} {
		t.Run(fmt.Sprint(m), func(t *testing.T) {
			shared := cache.NewMemory()
			newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithTier(shared, time.Minute),
				cache.WithOrigin(origin), cache.WithMode(m))
		})
	}
}

// The mode of an operation is the call's options over the longest matching
// key space over the stack's default; an unsupported per-call mode fails that
// call only.
func TestModeLevels(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	s := newStack(t, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithOrigin(o),
		cache.WithMode(cache.TTLBounded(time.Minute), cache.WriteThrough()),
		cache.WithKeySpace("avatars/*", cache.TTLBounded(time.Hour)),
		cache.WithKeySpace("avatars/admin/*", cache.Bypass()),
		cache.WithKeySpace("exact", cache.FailClosed()),
	)
	for key, want := range map[string]string{
		"other":            "TTLBounded(1m0s), WriteThrough, FillSingleflight, Degrade",
		"avatars/u1":       "TTLBounded(1h0m0s), WriteThrough, FillSingleflight, Degrade",
		"avatars/admin/u2": "Bypass, WriteThrough, FillSingleflight, Degrade",
		"exact":            "TTLBounded(1m0s), WriteThrough, FillSingleflight, FailClosed",
		"exactly":          "TTLBounded(1m0s), WriteThrough, FillSingleflight, Degrade",
	} {
		m, err := s.Mode(key)
		if err != nil || m.String() != want {
			t.Fatalf("Mode(%q) = %s, %v; want %s", key, m, err, want)
		}
	}
	if m, _ := s.Mode("avatars/u1", cache.FillUncoordinated()); m.String() != "TTLBounded(1h0m0s), WriteThrough, FillUncoordinated, Degrade" {
		t.Fatalf("a call's option over a key space = %s", m)
	}

	// Only the overriding read pays for its mode.
	o.put("avatars/u1", "one")
	assertGet(t, s, "avatars/u1", "one")
	o.put("avatars/u1", "two")
	assertGet(t, s, "avatars/u1", "one")
	if v, err := s.Get(ctx, tenant, "avatars/u1", cache.Bypass()); err != nil || string(v) != "two" {
		t.Fatalf("a per-call Bypass read = %q, %v", v, err)
	}
	assertGet(t, s, "avatars/u1", "one")

	// A per-call mode the stack cannot keep fails that call, naming why.
	if _, err := s.Get(ctx, tenant, "k", cache.Validated()); !errors.Is(err, cache.ErrUnsupportedMode) || !strings.Contains(err.Error(), "ConditionalLoads") {
		t.Fatalf("a per-call Validated over an origin without conditional loads = %v", err)
	}
	if err := s.Set(ctx, tenant, "k", []byte("v"), cache.FillVersionFenced()); !errors.Is(err, cache.ErrUnsupportedMode) {
		t.Fatalf("a per-call unsupported write = %v", err)
	}
	assertGet(t, s, "avatars/u1", "one") // and the stack is unaffected

	if _, err := cache.New(ctx, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithKeySpace("x"), cache.WithKeySpace("x")); err == nil {
		t.Fatal("a key space configured twice was accepted")
	}
	if _, err := cache.New(ctx, cache.WithTier(cache.NewMemory(), time.Minute), cache.WithKeySpace("*")); err == nil {
		t.Fatal("a key space matching every key was accepted; that is WithMode")
	}
}

// Memory's byte budget: it counts keys, values and versions plus a fixed
// overhead per entry, evicts the least recently used to stay under it, and
// refuses an entry larger than the whole budget.
func TestMemoryByteBudget(t *testing.T) {
	ctx := context.Background()
	const overhead = 128
	entry := func(n int) cache.Entry { return cache.Entry{Value: make([]byte, n), Version: "v1"} }
	size := func(key string, n int) int64 { return int64(len(key)+n+2) + overhead }

	m := cache.NewMemory(cache.MaxBytes(3 * size("k0", 100)))
	if got := m.Capabilities().ByteBudget; got != 3*size("k0", 100) {
		t.Fatalf("declared budget %d", got)
	}
	for i := range 3 {
		if err := m.Set(ctx, fmt.Sprintf("k%d", i), entry(100), time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if m.Bytes() != 3*size("k0", 100) {
		t.Fatalf("Bytes = %d, want %d", m.Bytes(), 3*size("k0", 100))
	}
	if _, err := m.Get(ctx, "k0"); err != nil { // k0 is now the most recent: k1 goes first
		t.Fatal(err)
	}
	if err := m.Set(ctx, "k3", entry(100), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get(ctx, "k1"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("the least recently used entry survived: %v", err)
	}
	for _, k := range []string{"k0", "k2", "k3"} {
		if _, err := m.Get(ctx, k); err != nil {
			t.Fatalf("%s was evicted: %v", k, err)
		}
	}
	// A larger value evicts as many as it needs; overwriting accounts the
	// difference.
	if err := m.Set(ctx, "k0", entry(250), time.Minute); err != nil {
		t.Fatal(err)
	}
	if m.Bytes() > m.Capabilities().ByteBudget {
		t.Fatalf("Bytes %d over the budget %d", m.Bytes(), m.Capabilities().ByteBudget)
	}
	if m.Len() != 2 {
		t.Fatalf("Len = %d after growing k0, want 2", m.Len())
	}
	// An entry larger than the whole budget is refused, and stores nothing.
	if err := m.Set(ctx, "huge", entry(1000), time.Minute); !errors.Is(err, cache.ErrTooLarge) {
		t.Fatalf("an entry over the budget = %v, want ErrTooLarge", err)
	}
	if err := m.SetFenced(ctx, "huge", entry(1000), time.Minute); !errors.Is(err, cache.ErrTooLarge) {
		t.Fatalf("a fenced entry over the budget = %v, want ErrTooLarge", err)
	}
	if err := m.Flush(ctx); err != nil || m.Bytes() != 0 || m.Len() != 0 {
		t.Fatalf("after Flush: %v, %d bytes, %d entries", err, m.Bytes(), m.Len())
	}

	// Expiry: an entry past its TTL is dropped when next touched, and its
	// bytes with it.
	now := time.Now()
	clock := func() time.Time { return now }
	e := cache.NewMemory(cache.MemoryClock(clock))
	if err := e.Set(ctx, "k", entry(10), time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := e.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) || e.Bytes() != 0 {
		t.Fatalf("an expired entry: %v, %d bytes held", err, e.Bytes())
	}
}

// A notice the tier above fails to act on quarantines the key there: the stack
// does not serve that tier's copy until the eviction lands.
func TestFailedEvictionQuarantines(t *testing.T) {
	ctx := context.Background()
	o := newOrigin()
	o.put("k", "one")
	shared := cache.NewMemory()
	top := &flakyMemory{Memory: cache.NewMemory()}
	s := newStack(t, cache.WithTier(top, time.Hour), cache.WithTier(shared, time.Hour), cache.WithOrigin(o))
	writer := newStack(t, cache.WithTier(shared.Share(), time.Hour), cache.WithOrigin(o))
	assertGet(t, s, "k", "one")
	top.failDeletes = true
	if err := writer.Set(ctx, tenant, "k", []byte("two")); err != nil {
		t.Fatal(err)
	}
	assertGet(t, s, "k", "two") // the top tier still holds the old generation, and is not read
	top.failDeletes = false
	assertGet(t, s, "k", "two")
}

type flakyMemory struct {
	*cache.Memory
	failDeletes bool
}

func (f *flakyMemory) Delete(ctx context.Context, key string) error {
	if f.failDeletes {
		return errUnreachable
	}
	return f.Memory.Delete(ctx, key)
}
