package cachetest

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// recorder counts the calls a stack makes on the layer it wraps, and keeps
// the key of the last Set.
type recorder struct {
	mu    sync.Mutex
	n     int
	set   string
	heard map[string]bool // keys the layer's notices delivered to the stack
}

// notified wraps a stack's notice callback so the recorder sees each key.
func (r *recorder) notified(fn func(string)) func(string) {
	return func(key string) {
		r.mu.Lock()
		if r.heard == nil {
			r.heard = map[string]bool{}
		}
		r.heard[key] = true
		r.mu.Unlock()
		fn(key)
	}
}

func (r *recorder) heardAll(keys []string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		if !r.heard[key] {
			return false
		}
	}
	return true
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

func (r *recorder) lastSet() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.set
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n = 0
}

// record wraps l so every call on it is counted. The wrapper implements every
// role and declares exactly what l declares, so the stack treats it as l.
// Subscriptions are not counted; the stack makes them once, when built.
func record(l cache.Layer) (cache.Layer, *recorder) {
	r := &recorder{}
	return &recordedLayer{forward: forward{l}, r: r}, r
}

// forward passes every role of a layer through to it, and declares what it
// declares. A role the layer does not have is never called: the stack only
// calls what the declaration names.
type forward struct{ inner cache.Layer }

func (f forward) Capabilities() cache.Capabilities { return cache.CapabilitiesOf(f.inner) }

func (f forward) Get(ctx context.Context, key string) (cache.Entry, error) {
	return f.inner.Get(ctx, key)
}

func (f forward) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	return f.inner.Set(ctx, key, e, ttl)
}

func (f forward) Delete(ctx context.Context, key string) error { return f.inner.Delete(ctx, key) }

func (f forward) Acquire(ctx context.Context, key string, ttl time.Duration) (cache.Lease, bool, error) {
	return f.inner.(cache.Leaser).Acquire(ctx, key, ttl)
}

func (f forward) Fill(ctx context.Context, lease cache.Lease, e cache.Entry, ttl time.Duration) error {
	return f.inner.(cache.Leaser).Fill(ctx, lease, e, ttl)
}

func (f forward) Release(ctx context.Context, lease cache.Lease) error {
	return f.inner.(cache.Leaser).Release(ctx, lease)
}

func (f forward) Subscribe(ctx context.Context, fn func(key string)) (func(), error) {
	return f.inner.(cache.Notifier).Subscribe(ctx, fn)
}

func (f forward) SubscribeGaps(ctx context.Context, lost, resynced func()) (func(), error) {
	return f.inner.(cache.Resyncer).SubscribeGaps(ctx, lost, resynced)
}

func (f forward) Flush(ctx context.Context) error { return f.inner.(cache.Flusher).Flush(ctx) }

func (f forward) SetFenced(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	return f.inner.(cache.Fencer).SetFenced(ctx, key, e, ttl)
}

type recordedLayer struct {
	forward
	r *recorder
}

func (x *recordedLayer) Get(ctx context.Context, key string) (cache.Entry, error) {
	x.r.called()
	return x.forward.Get(ctx, key)
}

func (x *recordedLayer) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	x.r.called()
	x.r.mu.Lock()
	x.r.set = key
	x.r.mu.Unlock()
	return x.forward.Set(ctx, key, e, ttl)
}

func (x *recordedLayer) SetFenced(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	x.r.called()
	x.r.mu.Lock()
	x.r.set = key
	x.r.mu.Unlock()
	return x.forward.SetFenced(ctx, key, e, ttl)
}

func (x *recordedLayer) Delete(ctx context.Context, key string) error {
	x.r.called()
	return x.forward.Delete(ctx, key)
}

func (x *recordedLayer) Acquire(ctx context.Context, key string, ttl time.Duration) (cache.Lease, bool, error) {
	x.r.called()
	return x.forward.Acquire(ctx, key, ttl)
}

func (x *recordedLayer) Fill(ctx context.Context, lease cache.Lease, e cache.Entry, ttl time.Duration) error {
	x.r.called()
	return x.forward.Fill(ctx, lease, e, ttl)
}

func (x *recordedLayer) Release(ctx context.Context, lease cache.Lease) error {
	x.r.called()
	return x.forward.Release(ctx, lease)
}

func (x *recordedLayer) Flush(ctx context.Context) error {
	x.r.called()
	return x.forward.Flush(ctx)
}

func (x *recordedLayer) Subscribe(ctx context.Context, fn func(key string)) (func(), error) {
	return x.forward.Subscribe(ctx, x.r.notified(fn))
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
	if !bytes.Equal(got.Value, want.Value) || got.Version != want.Version || got.Sequence != want.Sequence ||
		got.Missing != want.Missing || !got.Confirmed.Equal(want.Confirmed) {
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
