package cachetest

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// store is a real in-process origin: every write gets the next sequence, its
// version is that sequence, loads are conditional, deletes load as a Missing
// entry carrying their sequence, and with feed set it reports every write as a
// ChangeFeed. It counts loads and lets a test hold them.
type store struct {
	feed bool

	mu          sync.Mutex
	seq         uint64
	values      map[string]storedValue
	loads       int
	notModified int
	onLoad      func(ctx context.Context, key string) // runs after the value is read, before it returns
	subs        map[int]func(string)
	nextSub     int
	onCommit    func(key string, v storedValue, ctx context.Context)
}

type storedValue struct {
	value   string
	seq     uint64
	deleted bool
}

var (
	_ cache.Store      = (*store)(nil)
	_ cache.ChangeFeed = (*store)(nil)
)

func newStore() *store { return &store{values: map[string]storedValue{}, subs: map[int]func(string){}} }

func (o *store) Capabilities() cache.Capabilities {
	return cache.Capabilities{ConditionalLoads: true, Sequenced: true, ChangeFeed: o.feed}
}

func (o *store) Load(ctx context.Context, key, ifNot string) (cache.Entry, error) {
	o.mu.Lock()
	o.loads++
	v, ok := o.values[key]
	hook := o.onLoad
	o.mu.Unlock()
	if hook != nil {
		hook(ctx, key)
	}
	if !ok {
		return cache.Entry{}, cache.ErrNotFound
	}
	if v.deleted {
		return cache.Entry{Missing: true, Sequence: v.seq}, nil
	}
	version := strconv.FormatUint(v.seq, 10)
	if ifNot == version {
		o.mu.Lock()
		o.notModified++
		o.mu.Unlock()
		return cache.Entry{}, cache.ErrNotModified
	}
	return cache.Entry{Value: []byte(v.value), Version: version, Sequence: v.seq}, nil
}

func (o *store) Put(ctx context.Context, key string, value []byte) (string, error) {
	v := o.commit(ctx, key, string(value), false)
	return strconv.FormatUint(v.seq, 10), nil
}

func (o *store) Remove(ctx context.Context, key string) error {
	o.commit(ctx, key, "", true)
	return nil
}

// put writes around every stack.
func (o *store) put(key, value string) { o.commit(context.Background(), key, value, false) }

func (o *store) remove(key string) { o.commit(context.Background(), key, "", true) }

func (o *store) commit(ctx context.Context, key, value string, deleted bool) storedValue {
	o.mu.Lock()
	o.seq++
	v := storedValue{value: value, seq: o.seq, deleted: deleted}
	o.values[key] = v
	var fns []func(string)
	if o.feed {
		for i := range o.nextSub {
			if fn, ok := o.subs[i]; ok {
				fns = append(fns, fn)
			}
		}
	}
	onCommit := o.onCommit
	o.mu.Unlock()
	if onCommit != nil {
		onCommit(key, v, ctx)
	}
	for _, fn := range fns {
		fn(key)
	}
	return v
}

func (o *store) SubscribeChanges(_ context.Context, fn func(string)) (func(), error) {
	o.mu.Lock()
	id := o.nextSub
	o.nextSub++
	o.subs[id] = fn
	o.mu.Unlock()
	return func() { o.mu.Lock(); delete(o.subs, id); o.mu.Unlock() }, nil
}

func (o *store) current(key string) storedValue {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.values[key]
}

func (o *store) loadCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.loads
}

func (o *store) notModifiedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.notModified
}

func (o *store) setOnLoad(fn func(ctx context.Context, key string)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onLoad = fn
}

// plainOrigin is store without any capability declared: unversioned for the
// stack's purposes, the way an origin that declares nothing is.
type plainOrigin struct{ s *store }

func (p plainOrigin) Load(ctx context.Context, key, _ string) (cache.Entry, error) {
	e, err := p.s.Load(ctx, key, "")
	e.Sequence = 0
	return e, err
}

func (p plainOrigin) Put(ctx context.Context, key string, value []byte) (string, error) {
	return p.s.Put(ctx, key, value)
}

func (p plainOrigin) Remove(ctx context.Context, key string) error { return p.s.Remove(ctx, key) }

// fakeClock is a clock the test moves. Sleep moves it too, so a stack waiting
// on a lease waits in virtual time.
type fakeClock struct {
	// real makes Sleep also wait d, so a stack waiting on another goroutine's
	// lease gives that goroutine time to fill it.
	real bool

	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if c.real {
		time.Sleep(d)
	}
	c.Advance(d)
	return ctx.Err()
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// skewed reads base's time off by offset: another process's clock.
type skewed struct {
	base   *fakeClock
	offset time.Duration
}

func (s skewed) Now() time.Time { return s.base.Now().Add(s.offset) }

func (s skewed) Sleep(ctx context.Context, d time.Duration) error { return s.base.Sleep(ctx, d) }

// tasks holds a stack's background work until the test runs it.
type tasks struct {
	mu    sync.Mutex
	queue []func()
}

func (q *tasks) run(task func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, task)
}

// runAll runs every task, including those the tasks queue.
func (q *tasks) runAll() {
	for q.runOne() {
	}
}

func (q *tasks) runOne() bool {
	q.mu.Lock()
	if len(q.queue) == 0 {
		q.mu.Unlock()
		return false
	}
	task := q.queue[0]
	q.queue = q.queue[1:]
	q.mu.Unlock()
	task()
	return true
}

func (q *tasks) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

var errInjected = errors.New("cachetest: injected layer failure")

// faulty wraps a layer with the faults the suite injects: every call fails
// while failing is set; afterGet runs once a Get has its answer and before it
// returns it; notices can be held, then delivered in any order, twice, or
// dropped inside a reported gap. With gaps set it declares Resyncs, whatever
// the layer declares, and reports the gaps it makes.
type faulty struct {
	forward
	failing  atomic.Bool
	// failWrites fails every write while reads still answer: a replica that
	// lost its primary, a server out of memory.
	failWrites atomic.Bool
	gaps     bool
	afterGet func(ctx context.Context, key string)

	mu       sync.Mutex
	hold     bool
	dropping bool
	held     []heldNotice
	fns      map[int]func(string)
	nextFn   int
	lost     []func()
	resynced []func()
}

type heldNotice struct {
	fn  func(string)
	key string
}

func newFaulty(l cache.Layer) *faulty {
	return &faulty{forward: forward{l}, fns: map[int]func(string){}}
}

func (f *faulty) Capabilities() cache.Capabilities {
	c := cache.CapabilitiesOf(f.inner)
	if f.gaps && c.Notifies {
		c.Resyncs = true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hold && c.NoticeBound == cache.NoticesBeforeReturn {
		c.NoticeBound = time.Minute // held notices arrive later than the write
	}
	return c
}

func (f *faulty) fail() error {
	if f.failing.Load() {
		return errInjected
	}
	return nil
}

func (f *faulty) Get(ctx context.Context, key string) (cache.Entry, error) {
	if err := f.fail(); err != nil {
		return cache.Entry{}, err
	}
	e, err := f.forward.Get(ctx, key)
	if f.afterGet != nil {
		f.afterGet(ctx, key)
	}
	return e, err
}

func (f *faulty) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	if err := f.fail(); err != nil {
		return err
	}
	if f.failWrites.Load() {
		return errInjected
	}
	return f.forward.Set(ctx, key, e, ttl)
}

func (f *faulty) SetFenced(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	if err := f.fail(); err != nil {
		return err
	}
	if f.failWrites.Load() {
		return errInjected
	}
	return f.forward.SetFenced(ctx, key, e, ttl)
}

func (f *faulty) Delete(ctx context.Context, key string) error {
	if err := f.fail(); err != nil {
		return err
	}
	if f.failWrites.Load() {
		return errInjected
	}
	return f.forward.Delete(ctx, key)
}

func (f *faulty) Acquire(ctx context.Context, key string, ttl time.Duration) (cache.Lease, bool, error) {
	if err := f.fail(); err != nil {
		return cache.Lease{}, false, err
	}
	return f.forward.Acquire(ctx, key, ttl)
}

func (f *faulty) Fill(ctx context.Context, lease cache.Lease, e cache.Entry, ttl time.Duration) error {
	if err := f.fail(); err != nil {
		return err
	}
	return f.forward.Fill(ctx, lease, e, ttl)
}

func (f *faulty) Release(ctx context.Context, lease cache.Lease) error {
	if err := f.fail(); err != nil {
		return err
	}
	return f.forward.Release(ctx, lease)
}

func (f *faulty) Flush(ctx context.Context) error {
	if err := f.fail(); err != nil {
		return err
	}
	return f.forward.Flush(ctx)
}

func (f *faulty) Subscribe(ctx context.Context, fn func(string)) (func(), error) {
	f.mu.Lock()
	id := f.nextFn
	f.nextFn++
	f.fns[id] = fn
	f.mu.Unlock()
	stop, err := f.forward.Subscribe(ctx, func(key string) { f.notice(id, key) })
	if err != nil {
		return nil, err
	}
	return func() { stop(); f.mu.Lock(); delete(f.fns, id); f.mu.Unlock() }, nil
}

func (f *faulty) notice(id int, key string) {
	f.mu.Lock()
	fn, ok := f.fns[id]
	switch {
	case !ok || f.dropping:
		f.mu.Unlock()
		return
	case f.hold:
		f.held = append(f.held, heldNotice{fn, key})
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()
	fn(key)
}

func (f *faulty) SubscribeGaps(ctx context.Context, lost, resynced func()) (func(), error) {
	f.mu.Lock()
	f.lost = append(f.lost, lost)
	f.resynced = append(f.resynced, resynced)
	f.mu.Unlock()
	if r, ok := f.inner.(cache.Resyncer); ok && cache.CapabilitiesOf(f.inner).Resyncs {
		return r.SubscribeGaps(ctx, lost, resynced)
	}
	return func() {}, nil
}

// holdNotices keeps notices until release, reverse or duplicate delivers them.
func (f *faulty) holdNotices() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hold = true
}

func (f *faulty) heldKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, len(f.held))
	for i, n := range f.held {
		keys[i] = n.key
	}
	return keys
}

// deliver hands over the held notices, in order or reversed, each once or
// twice, and stops holding when stop is set.
func (f *faulty) deliver(reversed, twice, stop bool) {
	f.mu.Lock()
	held := f.held
	f.held = nil
	if stop {
		f.hold = false
	}
	f.mu.Unlock()
	if reversed {
		for i, j := 0, len(held)-1; i < j; i, j = i+1, j-1 {
			held[i], held[j] = held[j], held[i]
		}
	}
	for _, n := range held {
		n.fn(n.key)
		if twice {
			n.fn(n.key)
		}
	}
}

// loseNotices opens a gap: it reports it, and drops every notice until
// resync.
func (f *faulty) loseNotices() {
	f.mu.Lock()
	f.dropping = true
	f.held = nil
	lost := append([]func(){}, f.lost...)
	f.mu.Unlock()
	for _, fn := range lost {
		fn()
	}
}

func (f *faulty) resync() {
	f.mu.Lock()
	f.dropping = false
	resynced := append([]func(){}, f.resynced...)
	f.mu.Unlock()
	for _, fn := range resynced {
		fn()
	}
}

// stackOf builds one process's stack for the mode and pitfall cases: a Memory
// tier on clock over shared, over origin, with jitter off and the clock and
// executor given. It fails the test when New refuses.
func stackOf(t *testing.T, top *cache.Memory, shared cache.Layer, origin cache.Source, clock cache.Clock, run func(func()), opts ...cache.Option) *cache.Stack {
	t.Helper()
	s, err := buildStack(top, shared, origin, clock, run, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func buildStack(top *cache.Memory, shared cache.Layer, origin cache.Source, clock cache.Clock, run func(func()), opts ...cache.Option) (*cache.Stack, error) {
	base := []cache.Option{cache.WithTTLJitter(0), cache.WithClock(clock)}
	if top != nil {
		base = append(base, cache.WithTier(top, time.Minute))
	}
	if shared != nil {
		base = append(base, cache.WithTier(shared, time.Minute))
	}
	if origin != nil {
		base = append(base, cache.WithOrigin(origin))
	}
	if run != nil {
		base = append(base, cache.WithExecutor(run))
	}
	return cache.New(context.Background(), append(base, opts...)...)
}

// refuses asserts that New refuses opts, naming every one of names.
func refuses(t *testing.T, names []string, opts ...cache.Option) {
	t.Helper()
	_, err := cache.New(context.Background(), opts...)
	if err == nil {
		t.Fatalf("New accepted a mode it cannot keep; want a refusal naming %s", strings.Join(names, ", "))
	}
	if !errors.Is(err, cache.ErrUnsupportedMode) {
		t.Fatalf("New = %v, want ErrUnsupportedMode", err)
	}
	for _, name := range names {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("New's refusal %q does not name %s", err, name)
		}
	}
}

func getString(t *testing.T, s *cache.Stack, p cache.Partition, key string, opts ...cache.ModeOption) string {
	t.Helper()
	v, err := s.Get(context.Background(), p, key, opts...)
	if errors.Is(err, cache.ErrNotFound) {
		return "<not found>"
	}
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v)
}

func wantString(t *testing.T, s *cache.Stack, p cache.Partition, key, want string, opts ...cache.ModeOption) {
	t.Helper()
	if got := getString(t, s, p, key, opts...); got != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}
