package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A write's bookkeeping must not get more expensive as the process writes more
// keys. It used to scan every live entry on every write once 1024 had
// accumulated, so one write cost O(keys written within the window): measured at
// 5.5us for 1,000 live keys and 301us for 50,000, all under one mutex.
//
// Every record here shares one instant, so they all fall in one window and
// nothing expires — the worst case, and exactly the shape that was quadratic.
func TestOwnWritesCostDoesNotGrowWithLiveKeys(t *testing.T) {
	const window = time.Hour
	at := time.Unix(1_700_000_000, 0)
	cost := func(n int) time.Duration {
		o := &ownWrites{}
		for i := range n {
			o.record(fmt.Sprintf("key-%d", i), "v", false, at, window)
		}
		const probes = 2000
		start := time.Now()
		for i := range probes {
			o.record(fmt.Sprintf("probe-%d", i), "v", false, at, window)
		}
		return time.Since(start) / probes
	}
	small, large := cost(2_000), cost(32_000)
	t.Logf("per write: 2,000 live keys = %v; 32,000 live keys = %v", small, large)
	// 16x the live keys. A per-write scan costs ~16x more; O(1) costs the same.
	// The bound is deliberately loose so only a return to scanning trips it.
	if large > 6*small {
		t.Errorf("one write costs %v at 32,000 live keys against %v at 2,000: the cost is growing with the number of live keys", large, small)
	}
}

// The ring must not forget a write before its window is up, and must forget it
// after — that is what ReadYourWrites rests on.
func TestOwnWritesKeepEveryWriteForItsWindow(t *testing.T) {
	const window = time.Minute
	base := time.Unix(1_700_000_000, 0)
	o := &ownWrites{}
	// One write per ring step, spread over three windows.
	step := window / (ownWriteBuckets - 1)
	var keys []string
	for i := range 3 * ownWriteBuckets {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		at := base.Add(time.Duration(i) * step)
		o.record(k, "v"+k, false, at, window)
		// Every write made within the window before `at` is still known.
		for j := range i + 1 {
			was := base.Add(time.Duration(j) * step)
			w, ok := o.lookup(keys[j], at)
			want := !at.After(was.Add(window))
			if ok != want {
				t.Fatalf("at +%s, write of %s (made at +%s, window %s): found=%v, want %v", at.Sub(base), keys[j], was.Sub(base), window, ok, want)
			}
			if ok && w.version != "v"+keys[j] {
				t.Fatalf("lookup(%s) = %q, want %q", keys[j], w.version, "v"+keys[j])
			}
		}
	}
	// The live set is bounded by what the window holds, not by all writes.
	if len(o.writes) > 2*ownWriteBuckets {
		t.Errorf("after %d writes over three windows the live set holds %d entries; expiry is not dropping them", 3*ownWriteBuckets, len(o.writes))
	}
}

// A key rewritten in a later cycle survives the earlier cycle's bucket being
// dropped: the newest write is the one ReadYourWrites must see.
func TestOwnWritesRewriteSurvivesTheOldBucket(t *testing.T) {
	const window = time.Minute
	base := time.Unix(1_700_000_000, 0)
	step := window / (ownWriteBuckets - 1)
	o := &ownWrites{}
	o.record("k", "old", false, base, window)
	o.record("k", "new", false, base.Add(step), window)
	// Run the ring up to the cycle that reuses — and so empties — the bucket
	// the first write went into, but not as far as the rewrite's own.
	for i := 1; i <= ownWriteBuckets; i++ {
		o.record("filler", "v", false, base.Add(time.Duration(i)*step), window)
	}
	if w, ok := o.lookup("k", base.Add(time.Duration(ownWriteBuckets-1)*step)); !ok || w.version != "new" {
		t.Fatalf("lookup after the first write's bucket was dropped = %q, %v; want the rewrite, which is still inside its window", w.version, ok)
	}
}

// A fill lease must outlive the load it guards. When it does not, Leaser.Fill
// returns ErrLeaseLost and the fill is stored in no tier at all, so while every
// load takes longer than the lease the key is never cached by anybody — the
// stampede FillLease exists to prevent. The default lease TTL therefore follows
// the bound on the load rather than a constant that was shorter than it.
func TestDefaultLeaseTTLOutlivesTheLoad(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{"defaults", nil, 30 * time.Second},
		{"WithLoadTimeout", []Option{WithLoadTimeout(2 * time.Minute)}, 2 * time.Minute},
		{"WithLoadTimeout after", []Option{WithLoadTimeout(90 * time.Second)}, 90 * time.Second},
		{"explicit lease wins", []Option{WithLeaseTTL(3 * time.Second), WithLoadTimeout(time.Minute)}, 3 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, err := New(ctx, append([]Option{WithTier(NewMemory(), time.Hour)}, c.opts...)...)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.leaseTTL != c.want {
				t.Fatalf("leaseTTL = %s, want %s", s.leaseTTL, c.want)
			}
			if c.name != "explicit lease wins" && s.leaseTTL < s.loadTimeout {
				t.Fatalf("the default lease TTL (%s) is shorter than the load it guards (%s): fills would be stored nowhere", s.leaseTTL, s.loadTimeout)
			}
		})
	}
}

// The cache of checked per-call modes must stay bounded: TTLBounded carries a
// caller-supplied duration, so keying it on the whole Mode let a per-request
// bound grow the map for the life of the stack.
func TestModeCacheStaysBounded(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, WithTier(NewMemory(), time.Hour), WithOrigin(nopStore{}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := range 20_000 {
		if _, err := s.Mode("k", TTLBounded(time.Duration(i+1)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	s.modesMu.Lock()
	n := len(s.modes)
	s.modesMu.Unlock()
	if n != 1 {
		t.Errorf("20,000 distinct TTLBounded bounds left %d entries in the mode cache, want 1: every positive bound is the same decision", n)
	}
	// A refusal is still a refusal, and names its own bound.
	if _, err := s.Mode("k", TTLBounded(0)); !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("TTLBounded(0) = %v, want ErrUnsupportedMode", err)
	}
	if _, err := s.Mode("k", TTLBounded(-7*time.Second)); err == nil || !strings.Contains(err.Error(), "-7s") {
		t.Fatalf("TTLBounded(-7s) = %v, want a refusal naming -7s", err)
	}
}

type nopStore struct{}

func (nopStore) Load(context.Context, string, string) (Entry, error) { return Entry{}, ErrNotFound }
func (nopStore) Put(context.Context, string, []byte) (string, error) { return "v", nil }
func (nopStore) Remove(context.Context, string) error                { return nil }
