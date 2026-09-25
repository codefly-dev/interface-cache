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

// RunStack checks fill-once across processes: two stacks sharing the layer
// under test, each with its own Memory tier, hammered with concurrent reads of
// one missing key, reach the origin once when the layer is a Leaser, and at
// most once per stack otherwise.
func RunStack(t *testing.T, h Harness) {
	ctx := context.Background()
	ns := Namespace(t)
	var loads atomic.Int32
	origin := cache.SourceFunc(func(ctx context.Context, key, ifNot string) (cache.Entry, error) {
		loads.Add(1)
		time.Sleep(100 * time.Millisecond) // long enough for every reader to pile up
		return cache.Entry{Value: []byte("from origin"), Version: "1"}, nil
	})
	newStack := func() *cache.Stack {
		s, err := cache.New(ctx,
			cache.WithTier(cache.NewMemory(), time.Minute),
			cache.WithTier(h.New(t, ns), time.Minute),
			cache.WithOrigin(origin),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	stacks := []*cache.Stack{newStack(), newStack()}

	var wg sync.WaitGroup
	for i := range 64 {
		s := stacks[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.Get(ctx, "k")
			if err != nil {
				t.Error(err)
				return
			}
			if string(v) != "from origin" {
				t.Errorf("Get = %q", v)
			}
		}()
	}
	wg.Wait()

	want := int32(2)
	if _, ok := h.New(t, ns).(cache.Leaser); ok {
		want = 1
	}
	if n := loads.Load(); n > want || n == 0 {
		t.Fatalf("origin loaded %d times, want %d at most", n, want)
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
