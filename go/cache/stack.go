package cache

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Stack reads through its layers, top first, down to the origin, and fills
// the layers on the way back. Build one with New; it is safe for concurrent
// use.
//
// Every operation takes a Partition, and fails closed without one: the stack
// never serves one partition's data, fill or negative entry to another. A
// stack built WithGlobal takes Global instead. Over an origin, a write reaches
// every partition's copies: each key has a generation that all copies are
// stored under, and a write replaces it.
//
// Every operation runs in a Mode: the stack's default (WithMode), the key
// space's (WithKeySpace), or the call's own options.
type Stack struct {
	tiers      []*tier
	origin     Source
	originCaps Capabilities
	global     bool
	schema     string
	mode       Mode
	spaceOpts  []spaceOption
	spaces     []keySpace // longest pattern first
	// bound is the default freshness bound: the longest tier TTL.
	bound           time.Duration
	negativeTTL     time.Duration
	leaseTTL        time.Duration
	loadTimeout     time.Duration
	staleWindow     time.Duration
	swrExtension    time.Duration // the longest StaleWhileRevalidate window configured
	jitter          float64
	breakerFailures int
	breakerCooldown time.Duration
	clock           Clock
	skew            time.Duration
	run             func(task func())
	behindErrors    func(key string, err error)
	// mint is how a missing generation is minted: with a lease when a tier
	// grants them, fenced when every tier fences, else unguarded.
	mint FillCoordination

	flight singleflight.Group
	stops  []func()

	// resyncMu orders resyncs against fills of the tiers they flush: a fill
	// that read before a resync must not store into a tier it flushed.
	resyncMu sync.RWMutex
	resyncs  uint64

	// notices and writes count, per bucket of cache keys, the notices and
	// the local writes that touched any of their layer keys. A fill that read
	// below while its key's count moved undoes what it stored: the notice or
	// write may have landed first. A notice only concerns the tiers above
	// its notifier; a local write, every tier.
	notices [epochBuckets]atomic.Uint64
	writes  [epochBuckets]atomic.Uint64

	modesMu sync.Mutex
	modes   map[Mode]error // per-call modes checked so far

	own    ownWrites
	behind writeBehind
}

const epochBuckets = 4096

type tier struct {
	layer   Layer
	caps    Capabilities
	ttl     time.Duration
	breaker *breaker
	// flushed marks a tier above a Resyncer, which a resync empties.
	flushed bool
	// notifier is the nearest tier below that notifies, or -1.
	notifier int
	// inGap is set on a Resyncer between lost and resynced; guarded by
	// Stack.resyncMu.
	inGap bool
	// gaps counts the Resyncers below this tier in a gap: while positive,
	// the tier is neither read nor filled.
	gaps       atomic.Int32
	quarantine quarantine
}

// Clock is where a stack reads time and waits. Freshness, leases and breakers
// all use it; the simulation in cachetest drives it.
type Clock interface {
	Now() time.Time
	// Sleep waits d, or until ctx ends.
	Sleep(ctx context.Context, d time.Duration) error
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// SystemClock is the wall clock, the default.
func SystemClock() Clock { return systemClock{} }

// Option configures a Stack.
type Option func(*Stack) error

type spaceOption struct {
	pattern string
	opts    []ModeOption
}

// WithTier appends a layer below the ones already added. It keeps an entry for
// ttl (plus WithStaleWindow and the longest StaleWhileRevalidate window); how
// long it serves one is the mode's freshness. The longest tier TTL is the
// default freshness bound.
func WithTier(layer Layer, ttl time.Duration) Option {
	return func(s *Stack) error {
		if layer == nil {
			return errors.New("cache: WithTier: nil layer")
		}
		if ttl <= 0 {
			return fmt.Errorf("cache: WithTier: ttl must be positive, got %s", ttl)
		}
		s.tiers = append(s.tiers, &tier{layer: layer, ttl: ttl, notifier: -1})
		return nil
	}
}

// WithOrigin sets the authoritative source under the layers. Without one the
// stack is a plain cache: a miss is ErrMiss, and Set and Delete write the
// layers only.
func WithOrigin(source Source) Option {
	return func(s *Stack) error {
		if source == nil {
			return errors.New("cache: WithOrigin: nil source")
		}
		s.origin = source
		return nil
	}
}

// WithMode sets the stack's default mode, one dimension per option; the
// others keep their defaults: TTLBounded(the longest tier TTL),
// WriteInvalidate, FillSingleflight, Degrade.
func WithMode(opts ...ModeOption) Option {
	return func(s *Stack) error { s.mode = s.mode.with(opts); return nil }
}

// WithKeySpace sets the mode of the keys pattern matches: a pattern ending in
// "*" matches every key with that prefix, any other pattern one key. The
// longest matching pattern wins; dimensions it does not set come from
// WithMode. New refuses the stack if the key space's mode is one the layers
// and origin cannot keep.
func WithKeySpace(pattern string, opts ...ModeOption) Option {
	return func(s *Stack) error {
		if pattern == "" || pattern == "*" {
			return errors.New("cache: WithKeySpace: the pattern must name keys; use WithMode for every key")
		}
		for _, o := range s.spaceOpts {
			if o.pattern == pattern {
				return fmt.Errorf("cache: WithKeySpace: %q is configured twice", pattern)
			}
		}
		s.spaceOpts = append(s.spaceOpts, spaceOption{pattern: pattern, opts: opts})
		return nil
	}
}

// WithNegativeTTL sets how long an origin's ErrNotFound is served. Zero
// disables negative caching. Default 5s.
func WithNegativeTTL(d time.Duration) Option {
	return func(s *Stack) error { s.negativeTTL = d; return nil }
}

// WithLeaseTTL sets how long a fill lease is held on a shared layer. It should
// exceed the origin's load time: a fill that outlives its lease is not cached.
// Default 5s.
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Stack) error {
		if d <= 0 {
			return fmt.Errorf("cache: WithLeaseTTL: must be positive, got %s", d)
		}
		s.leaseTTL = d
		return nil
	}
}

// WithLoadTimeout bounds one origin load. The load is detached from the
// caller that started it, because other callers wait on the same load.
// Default 30s.
func WithLoadTimeout(d time.Duration) Option {
	return func(s *Stack) error {
		if d <= 0 {
			return fmt.Errorf("cache: WithLoadTimeout: must be positive, got %s", d)
		}
		s.loadTimeout = d
		return nil
	}
}

// WithStaleWindow keeps entries this long past their tier's TTL, so a copy
// too old to serve that carries a version is revalidated with a conditional
// load instead of reloaded. Default 0.
func WithStaleWindow(d time.Duration) Option {
	return func(s *Stack) error { s.staleWindow = d; return nil }
}

// WithTTLJitter shortens each stored TTL by a random fraction up to f, so
// entries filled together do not expire together. Default 0.1.
func WithTTLJitter(f float64) Option {
	return func(s *Stack) error {
		if f < 0 || f >= 1 {
			return fmt.Errorf("cache: WithTTLJitter: must be in [0, 1), got %v", f)
		}
		s.jitter = f
		return nil
	}
}

// WithBreaker skips a layer for cooldown after failures consecutive errors, so
// an unreachable layer costs one timeout per cooldown rather than one per
// read. Default 5 failures, 10s.
func WithBreaker(failures int, cooldown time.Duration) Option {
	return func(s *Stack) error {
		if failures < 1 || cooldown <= 0 {
			return errors.New("cache: WithBreaker: failures must be >= 1 and cooldown positive")
		}
		s.breakerFailures, s.breakerCooldown = failures, cooldown
		return nil
	}
}

// WithGlobal builds a stack for data that does not vary by viewer: every
// operation takes Global(), and a caller's partition is refused with
// ErrWrongPartition. Use it only when every caller may read every entry; it is
// where a reviewer looks for a cross-viewer leak.
func WithGlobal() Option {
	return func(s *Stack) error { s.global = true; return nil }
}

// WithSchema names the shape of the values the stack stores, the codec's
// version. Copies are keyed by it, so a deploy that changes the shape never
// decodes bytes its predecessor cached; generations are not, so a write by
// either version invalidates both versions' copies. Default "".
func WithSchema(version string) Option {
	return func(s *Stack) error { s.schema = version; return nil }
}

// WithClock sets where the stack reads time and waits. Default SystemClock.
func WithClock(c Clock) Option {
	return func(s *Stack) error {
		if c == nil {
			return errors.New("cache: WithClock: nil clock")
		}
		s.clock = c
		return nil
	}
}

// WithClockSkew bounds how far any other process's clock may be from this
// one's. Confirmed is stamped by whichever process loaded a value, so every
// age the stack measures is taken as d older than it looks: TTLBounded(b) then
// holds in real time for skews up to d. Default 0: clocks agree.
func WithClockSkew(d time.Duration) Option {
	return func(s *Stack) error {
		if d < 0 {
			return fmt.Errorf("cache: WithClockSkew: must not be negative, got %s", d)
		}
		s.skew = d
		return nil
	}
}

// WithExecutor runs the stack's background work — StaleWhileRevalidate
// refreshes and WriteBehind origin writes — through run. Default: a new
// goroutine per task.
func WithExecutor(run func(task func())) Option {
	return func(s *Stack) error {
		if run == nil {
			return errors.New("cache: WithExecutor: nil executor")
		}
		s.run = run
		return nil
	}
}

// WithWriteBehindErrors reports each WriteBehind write the origin refused five
// times in a row and that was dropped. Stack.Drain also returns them.
func WithWriteBehindErrors(fn func(key string, err error)) Option {
	return func(s *Stack) error { s.behindErrors = fn; return nil }
}

// New builds a stack. It needs at least one layer or an origin. It refuses
// every mode — the default, and each key space's — that the layers' and the
// origin's declared capabilities cannot keep, naming the capability missing.
// Layers that are Notifiers are subscribed here, so New fails when one cannot
// be reached. Every layer above a Resyncer must be a Flusher.
func New(ctx context.Context, opts ...Option) (*Stack, error) {
	s := &Stack{
		negativeTTL:     5 * time.Second,
		leaseTTL:        5 * time.Second,
		loadTimeout:     30 * time.Second,
		jitter:          0.1,
		breakerFailures: 5,
		breakerCooldown: 10 * time.Second,
		clock:           systemClock{},
		run:             func(task func()) { go task() },
		modes:           map[Mode]error{},
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	if len(s.tiers) == 0 && s.origin == nil {
		return nil, errors.New("cache: New: a stack needs at least one tier or an origin")
	}
	for i, t := range s.tiers {
		t.caps = CapabilitiesOf(t.layer)
		if err := checkDeclared(fmt.Sprintf("tier %d", i), t.layer, t.caps); err != nil {
			return nil, err
		}
		t.breaker = &breaker{threshold: s.breakerFailures, cooldown: s.breakerCooldown, now: s.clock.Now}
		s.bound = max(s.bound, t.ttl)
	}
	if s.origin != nil {
		s.originCaps = CapabilitiesOf(s.origin)
		if err := checkDeclared("the origin", s.origin, s.originCaps); err != nil {
			return nil, err
		}
	}
	for i := len(s.tiers) - 2; i >= 0; i-- {
		below := s.tiers[i+1]
		s.tiers[i].notifier = below.notifier
		if below.caps.Notifies {
			s.tiers[i].notifier = i + 1
		}
	}
	s.mint = s.mintCoordination()
	if err := s.resolveModes(); err != nil {
		return nil, err
	}
	if err := s.subscribe(ctx); err != nil {
		s.Close()
		return nil, err
	}
	s.behind.init()
	return s, nil
}

// resolveModes checks the default mode and builds each key space's.
func (s *Stack) resolveModes() error {
	if err := s.supports(s.mode); err != nil {
		return fmt.Errorf("cache: New: default mode: %w", err)
	}
	s.noteSWR(s.mode)
	for _, o := range s.spaceOpts {
		k := keySpace{pattern: o.pattern, mode: s.mode.with(o.opts)}
		if p, ok := strings.CutSuffix(o.pattern, "*"); ok {
			k.pattern, k.prefix = p, true
		}
		if err := s.supports(k.mode); err != nil {
			return fmt.Errorf("cache: New: key space %q: %w", o.pattern, err)
		}
		s.noteSWR(k.mode)
		s.spaces = append(s.spaces, k)
	}
	slices.SortStableFunc(s.spaces, func(a, b keySpace) int { return len(b.pattern) - len(a.pattern) })
	return nil
}

func (s *Stack) noteSWR(m Mode) {
	if m.Freshness.kind == freshSWR {
		s.swrExtension = max(s.swrExtension, m.Freshness.bound)
	}
}

// mintCoordination is how a missing generation is minted, whatever the
// mode's fill coordination: minting over a generation a write just stored
// would bring back the copies it orphaned, so a mint is leased when a tier
// grants leases, and fenced below any write's generation when every tier
// fences.
func (s *Stack) mintCoordination() FillCoordination {
	if s.declaresLeases() {
		return CoordinateLease
	}
	fences := len(s.tiers) > 0
	for _, t := range s.tiers {
		fences = fences && t.caps.Fences
	}
	if fences {
		return CoordinateVersionFenced
	}
	return CoordinateSingleflight
}

// subscribe listens to every notifying tier for the tiers above it, and to
// the origin's change feed.
func (s *Stack) subscribe(ctx context.Context) error {
	for i, t := range s.tiers {
		if !t.caps.Notifies || i == 0 {
			continue
		}
		above := s.tiers[:i]
		if t.caps.Resyncs {
			for j, a := range above {
				if !a.caps.Flushes {
					return fmt.Errorf("cache: tier %d (%T) is above tier %d, which can lose invalidations, but cannot be flushed on a resync (it does not declare Flushes)", j, a.layer, i)
				}
				a.flushed = true
			}
		}
		stop, err := t.layer.(Notifier).Subscribe(ctx, func(key string) { s.noticed(i, key) })
		if err != nil {
			return fmt.Errorf("cache: subscribe to invalidations of tier %d: %w", i, err)
		}
		s.stops = append(s.stops, stop)
		if t.caps.Resyncs {
			stop, err := t.layer.(Resyncer).SubscribeGaps(ctx, func() { s.gap(i, true) }, func() { s.gap(i, false) })
			if err != nil {
				return fmt.Errorf("cache: subscribe to gaps of tier %d: %w", i, err)
			}
			s.stops = append(s.stops, stop)
		}
	}
	if s.originCaps.ChangeFeed {
		stop, err := s.origin.(ChangeFeed).SubscribeChanges(ctx, func(key string) {
			_, _ = s.bump(context.Background(), key, FailDegrade)
		})
		if err != nil {
			return fmt.Errorf("cache: subscribe to the origin's changes: %w", err)
		}
		s.stops = append(s.stops, stop)
	}
	return nil
}

// noticed evicts key, reported changed by tier i, from the tiers above it. A
// tier that fails the eviction is quarantined for key: it may still hold the
// old copy.
func (s *Stack) noticed(i int, key string) {
	s.noticedEpoch(key)
	for _, a := range s.tiers[:i] {
		if err := a.layer.Delete(context.Background(), key); err != nil {
			a.breaker.failure()
			a.quarantine.add(key, s.clock.Now())
		}
	}
	s.noticedEpoch(key)
}

// gap handles tier i losing notices (lost) or getting them back: the tiers
// above it are flushed either way, and neither read nor filled in between.
// Fills that read before either store nothing in them.
func (s *Stack) gap(i int, lost bool) {
	s.resyncMu.Lock()
	defer s.resyncMu.Unlock()
	s.resyncs++
	t := s.tiers[i]
	above := s.tiers[:i]
	if lost && !t.inGap {
		t.inGap = true
		for _, a := range above {
			a.gaps.Add(1)
		}
	}
	for _, a := range above {
		if err := a.layer.(Flusher).Flush(context.Background()); err != nil {
			a.quarantine.overflowed(s.clock.Now())
		}
	}
	if !lost && t.inGap {
		t.inGap = false
		for _, a := range above {
			a.gaps.Add(-1)
		}
	}
}

// since returns the resync count a read starts from.
func (s *Stack) since() uint64 {
	s.resyncMu.RLock()
	defer s.resyncMu.RUnlock()
	return s.resyncs
}

// bucket is the epoch bucket of the cache key layerKey belongs to. It is keyed
// by the cache key, not the layer key, so every partition and generation of a
// key share it, and so that which fills a collision undoes does not depend on
// the random generation tokens: a run replays exactly.
func bucket(layerKey string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(keyOf(layerKey)))
	return h.Sum64() % epochBuckets
}

// noticed records that a notice named layerKey, for fills racing it.
func (s *Stack) noticedEpoch(layerKey string) { s.notices[bucket(layerKey)].Add(1) }

// touch records that a local write changed layerKey, for fills racing it.
func (s *Stack) touch(layerKey string) { s.writes[bucket(layerKey)].Add(1) }

// epoch is layerKey's notice and write counts.
type epoch struct{ notices, writes uint64 }

func (s *Stack) epochOf(layerKey string) epoch {
	b := bucket(layerKey)
	return epoch{s.notices[b].Load(), s.writes[b].Load()}
}

// Close stops the stack's invalidation subscriptions. It does not close the
// layers or the origin, and it does not wait for WriteBehind writes: those not
// yet written are lost. Call Drain first to keep them.
func (s *Stack) Close() {
	for _, stop := range s.stops {
		stop()
	}
	s.stops = nil
	s.behind.close()
}

// Mode returns the mode an operation on key runs in, with opts applied, or
// why the stack cannot keep it.
func (s *Stack) Mode(key string, opts ...ModeOption) (Mode, error) {
	m := s.mode
	for _, k := range s.spaces {
		if k.matches(key) {
			m = k.mode
			break
		}
	}
	if len(opts) == 0 {
		return m, nil
	}
	m = m.with(opts)
	s.modesMu.Lock()
	err, checked := s.modes[m]
	s.modesMu.Unlock()
	if !checked {
		err = s.supports(m)
		s.modesMu.Lock()
		s.modes[m] = err
		s.modesMu.Unlock()
	}
	return m, err
}

// Get returns the value for key in partition p. It is ErrNotFound when the
// origin holds none, and ErrMiss when the stack has no origin and no layer
// holds the key. opts override the mode for this read.
func (s *Stack) Get(ctx context.Context, p Partition, key string, opts ...ModeOption) ([]byte, error) {
	e, err := s.GetEntry(ctx, p, key, opts...)
	if err != nil {
		return nil, err
	}
	if e.Missing {
		return nil, ErrNotFound
	}
	return e.Value, nil
}

// GetEntry is Get returning the whole entry, version included. A negative
// entry is returned as an Entry with Missing set, not as an error.
//
// With an origin, the entry is p's copy under the key's current generation,
// so a write through any partition — which replaces the generation — is never
// followed by a read of a copy filled before it.
//
// In a write-around partition, or with Bypass, the read goes to the origin
// alone: it consults no layer, takes no lease, shares its load with no other
// caller and stores nothing. Without an origin it is ErrMiss.
func (s *Stack) GetEntry(ctx context.Context, p Partition, key string, opts ...ModeOption) (Entry, error) {
	if err := s.check(p); err != nil {
		return Entry{}, err
	}
	m, err := s.Mode(key, opts...)
	if err != nil {
		return Entry{}, err
	}
	s.heal(ctx)
	if p.writeAround || m.Freshness.kind == freshBypass {
		return s.loadAround(ctx, key)
	}
	if e, ok := s.behind.pending(p, key); ok {
		return e, nil
	}
	// One resync snapshot for the whole read: a generation read before a
	// resync may be one the resync exists to drop.
	since := s.since()
	if s.origin == nil {
		return s.read(ctx, m, s.newFill(p.layerKey(s.schema, key), since, m.Fill))
	}
	generation, err := s.generation(ctx, m, since, key)
	if err != nil {
		return Entry{}, err
	}
	f := s.newFill(p.entryKey(s.schema, generation, key), since, m.Fill)
	f.load = func(ctx context.Context, ifNot string) (Entry, error) { return s.origin.Load(ctx, key, ifNot) }
	if m.Freshness.kind == freshValidated {
		return s.validated(ctx, m, f)
	}
	e, err := s.read(ctx, m, f)
	if err != nil || m.Freshness.kind != freshRYW {
		return e, err
	}
	if w, ok := s.own.lookup(key, s.clock.Now()); ok && !w.satisfiedBy(e) {
		// A copy older than this process's own write: a lagging tier, or one
		// refilled from one. The origin has the write, or something newer.
		return s.revalidate(ctx, m, f, &e)
	}
	return e, nil
}

// check refuses a partition that does not match the stack. Every operation
// calls it before touching a layer or the origin.
func (s *Stack) check(p Partition) error {
	switch {
	case !p.global && p.key == "":
		return ErrNoPartition
	case s.global && !p.global:
		return fmt.Errorf("%w: the stack is built WithGlobal, so it takes cache.Global(), not a caller's partition", ErrWrongPartition)
	case !s.global && p.global:
		return fmt.Errorf("%w: cache.Global() is only for a stack built WithGlobal", ErrWrongPartition)
	}
	return nil
}

// fill is one layer key, and how to load it on a miss. Everything below read
// — tiers, leases, in-process loads — is keyed by layer, which already names
// the schema, the partition and the generation.
type fill struct {
	layer string
	// since is the resync count when the operation began; what it found must
	// not be stored into a flushed tier after a later resync.
	since uint64
	// epoch is layer's notice and write counts when the read began; a store
	// made after they moved is undone.
	epoch epoch
	// coordinate is how concurrent loads of layer are shared.
	coordinate FillCoordination
	// generation marks a generation's layer key: any copy a layer still keeps
	// is current, whatever its age.
	generation bool
	// load returns the value for layer; nil means the layers are all there
	// is, and a miss is ErrMiss.
	load func(ctx context.Context, ifNot string) (Entry, error)
}

func (s *Stack) newFill(layer string, since uint64, coordinate FillCoordination) fill {
	return fill{layer: layer, since: since, epoch: s.epochOf(layer), coordinate: coordinate}
}

// verdict is what a read may do with a copy it found.
type verdict int

const (
	stale verdict = iota
	serve
	serveAndRefresh // StaleWhileRevalidate: serve it, and revalidate in the background
)

// judge decides whether m may serve e at now. A generation is served for as
// long as a layer keeps it.
func (s *Stack) judge(m Mode, f fill, e Entry, now time.Time) verdict {
	if f.generation {
		return serve
	}
	age := e.Age(now) + s.skew
	if e.Confirmed.IsZero() {
		age = time.Duration(1<<63 - 1)
	}
	if e.Missing && age >= s.negativeTTL {
		return stale
	}
	bound := s.bound
	switch m.Freshness.kind {
	case freshTTL:
		bound = m.Freshness.bound
	case freshInvalidate:
		return serve
	case freshSWR:
		switch {
		case age < s.bound:
			return serve
		case age < s.bound+m.Freshness.bound:
			return serveAndRefresh
		}
		return stale
	}
	if age < bound {
		return serve
	}
	return stale
}

// usable reports whether a read may consult t now. A tier in a notifier's gap
// is skipped silently: skipping it cannot make a read staler. A failing or
// quarantined one is skipped under Degrade and refused under FailClosed.
func (s *Stack) usable(ctx context.Context, i int, m Mode) (bool, error) {
	t := s.tiers[i]
	if t.gaps.Load() > 0 {
		return false, nil
	}
	ok := t.breaker.allow() && s.recover(ctx, t)
	if !ok && m.Failure == FailClosedPolicy {
		return false, fmt.Errorf("%w: tier %d (%T) is failing or missed an invalidation", ErrUnavailable, i, t.layer)
	}
	return ok, nil
}

// read walks the tiers top-down for f.layer, and loads it on a miss.
func (s *Stack) read(ctx context.Context, m Mode, f fill) (Entry, error) {
	now := s.clock.Now()
	var old *Entry
	for i, t := range s.tiers {
		ok, err := s.usable(ctx, i, m)
		if err != nil {
			return Entry{}, err
		}
		if !ok {
			continue
		}
		e, err := t.layer.Get(ctx, f.layer)
		if errors.Is(err, ErrMiss) {
			t.breaker.success()
			continue
		}
		if err != nil {
			t.breaker.failure()
			if m.Failure == FailClosedPolicy {
				return Entry{}, fmt.Errorf("%w: tier %d (%T): %w", ErrUnavailable, i, t.layer, err)
			}
			continue
		}
		t.breaker.success()
		switch s.judge(m, f, e, now) {
		case serve:
			s.fillAbove(ctx, m, f, i, e)
			return e, nil
		case serveAndRefresh:
			s.fillAbove(ctx, m, f, i, e)
			s.refresh(m, f, e)
			return e, nil
		}
		if old == nil {
			old = &e
		}
	}
	if f.load == nil {
		return Entry{}, ErrMiss
	}
	return s.load(ctx, m, f, old)
}

// refresh revalidates e in the background, once per layer key at a time.
func (s *Stack) refresh(m Mode, f fill, e Entry) {
	s.run(func() {
		_, _, _ = s.flight.Do("refresh\x00"+f.layer, func() (any, error) {
			ctx, cancel := context.WithTimeout(context.Background(), s.loadTimeout)
			defer cancel()
			now := s.clock.Now()
			for _, t := range s.tiers {
				if cur, err := t.layer.Get(ctx, f.layer); err == nil && s.judge(m, f, cur, now) == serve {
					return cur, nil // an earlier refresh, or another process, got there first
				}
			}
			return s.loadShared(ctx, m, f, &e)
		})
	})
}

// validated serves a Validated read: whatever copy the layers hold, at any
// age, is checked against the origin before it is served.
func (s *Stack) validated(ctx context.Context, m Mode, f fill) (Entry, error) {
	var found *Entry
	for i, t := range s.tiers {
		ok, err := s.usable(ctx, i, m)
		if err != nil {
			return Entry{}, err
		}
		if !ok {
			continue
		}
		e, err := t.layer.Get(ctx, f.layer)
		if errors.Is(err, ErrMiss) {
			t.breaker.success()
			continue
		}
		if err != nil {
			t.breaker.failure()
			if m.Failure == FailClosedPolicy {
				return Entry{}, fmt.Errorf("%w: tier %d (%T): %w", ErrUnavailable, i, t.layer, err)
			}
			continue
		}
		t.breaker.success()
		found = &e
		break
	}
	return s.revalidate(ctx, m, f, found)
}

// revalidate loads f on its own — sharing no other caller's load, since one
// that began before this read may predate a write — conditionally on found's
// version, and stores the result.
func (s *Stack) revalidate(ctx context.Context, m Mode, f fill, found *Entry) (Entry, error) {
	lctx, cancel := context.WithTimeout(ctx, s.loadTimeout)
	defer cancel()
	skip := -1
	if m.Fill == CoordinateLease {
		skip, _ = s.leaser() // stored there under a lease only
	}
	return s.loadOrigin(lctx, m, f, found, skip, nil)
}

// generation returns key's current generation, minting one — once across
// processes, like any fill — when no layer holds it. It must be read before
// the origin is loaded: a fill is stored under the generation current when
// its load started, so a write that lands during the load orphans it.
func (s *Stack) generation(ctx context.Context, m Mode, since uint64, key string) (string, error) {
	if len(s.tiers) == 0 {
		return "", nil // nothing is stored, so nothing needs invalidating
	}
	f := s.newFill(generationKey(key), since, s.mint)
	f.generation, f.load = true, mintGeneration
	gm := m
	gm.Fill = s.mint
	e, err := s.read(ctx, gm, f)
	if err != nil {
		return "", err
	}
	return string(e.Value), nil
}

// mintGeneration is the "origin" of a generation. It is only asked when no
// usable layer holds one.
func mintGeneration(_ context.Context, _ string) (Entry, error) {
	g := newGeneration()
	return Entry{Value: []byte(g), Version: g}, nil
}

// mintedSequence and bumpedSequence order a generation a reader minted below
// one a write stored, for tiers that fence: a mint never replaces a write's.
const (
	mintedSequence uint64 = 0
	bumpedSequence uint64 = 1
)

// loadAround serves a write-around or Bypass read from the origin, stored
// nowhere and shared with no other caller.
func (s *Stack) loadAround(ctx context.Context, key string) (Entry, error) {
	if s.origin == nil {
		return Entry{}, ErrMiss
	}
	lctx, cancel := context.WithTimeout(ctx, s.loadTimeout)
	defer cancel()
	start := s.clock.Now()
	e, err := s.origin.Load(lctx, key, "")
	if errors.Is(err, ErrNotFound) {
		return Entry{Missing: true, Confirmed: start}, nil
	}
	e.Confirmed = start
	return e, err
}

// load shares a miss per the fill's coordination: singleflight collapses
// concurrent misses for one layer key — one key in one partition under one
// generation — in this process, and lets each caller stop waiting when its
// own context ends.
func (s *Stack) load(ctx context.Context, m Mode, f fill, old *Entry) (Entry, error) {
	if f.coordinate == CoordinateNone {
		lctx, cancel := context.WithTimeout(ctx, s.loadTimeout)
		defer cancel()
		return s.loadShared(lctx, m, f, old)
	}
	ch := s.flight.DoChan(f.layer, func() (any, error) {
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.loadTimeout)
		defer cancel()
		return s.loadShared(lctx, m, f, old)
	})
	select {
	case <-ctx.Done():
		return Entry{}, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return Entry{}, res.Err
		}
		return res.Val.(Entry), nil
	}
}

// loadShared collapses misses across processes when the fill is leased: the
// deepest layer that grants leases decides which process loads the origin;
// the others wait for its fill.
func (s *Stack) loadShared(ctx context.Context, m Mode, f fill, old *Entry) (Entry, error) {
	idx, leaser := s.leaser()
	if f.coordinate != CoordinateLease || leaser == nil {
		return s.loadOrigin(ctx, m, f, old, -1, nil)
	}
	t := s.tiers[idx]
	giveUp := s.clock.Now().Add(2 * s.leaseTTL)
	for {
		lease, ok, err := leaser.Acquire(ctx, f.layer, s.leaseTTL)
		if err != nil {
			t.breaker.failure()
			return s.loadOrigin(ctx, m, f, old, idx, nil)
		}
		if ok {
			// Another process may have filled the key between this one's miss
			// and its lease. Use that fill: loading again would replace a
			// generation, orphaning every copy made under it.
			if e, err := leaser.Get(ctx, f.layer); err == nil {
				if s.judge(m, f, e, s.clock.Now()) != stale {
					_ = leaser.Release(ctx, lease)
					s.fillAbove(ctx, m, f, idx, e)
					return e, nil
				}
				if old == nil {
					old = &e
				}
			}
			return s.loadOrigin(ctx, m, f, old, idx, &lease)
		}
		e, filled, err := s.waitForFill(ctx, m, f, leaser)
		if err != nil {
			return Entry{}, err
		}
		if filled {
			s.fillAbove(ctx, m, f, idx, e)
			return e, nil
		}
		if !s.clock.Now().Before(giveUp) {
			// The holder never filled, twice over: load without coordinating
			// rather than wait forever, and leave the shared layer alone.
			return s.loadOrigin(ctx, m, f, old, idx, nil)
		}
	}
}

// waitForFill polls the leasing layer until another process fills the key or
// the lease it holds must have expired.
func (s *Stack) waitForFill(ctx context.Context, m Mode, f fill, leaser Leaser) (Entry, bool, error) {
	deadline := s.clock.Now().Add(s.leaseTTL)
	delay := 5 * time.Millisecond
	for s.clock.Now().Before(deadline) {
		if err := s.clock.Sleep(ctx, delay); err != nil {
			return Entry{}, false, err
		}
		e, err := leaser.Get(ctx, f.layer)
		if err == nil && s.judge(m, f, e, s.clock.Now()) != stale {
			return e, true, nil
		}
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
	return Entry{}, false, nil
}

// loadOrigin loads f and stores the result. With a lease, the leasing layer is
// filled under it, and a lost lease means nothing is stored anywhere: the key
// changed while loading. The tier at skip, if any, is left alone otherwise,
// since storing there unleased could overwrite a newer value. The entry is
// confirmed as of the load's start.
func (s *Stack) loadOrigin(ctx context.Context, m Mode, f fill, old *Entry, skip int, lease *Lease) (Entry, error) {
	ifNot := ""
	if old != nil && !old.Missing {
		ifNot = old.Version
	}
	start := s.clock.Now()
	e, err := f.load(ctx, ifNot)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotModified) && ifNot != "":
		e = *old
	case errors.Is(err, ErrNotFound):
		e = Entry{Missing: true}
	default:
		if lease != nil {
			_ = s.tiers[skip].layer.(Leaser).Release(ctx, *lease)
		}
		return Entry{}, err
	}
	if e.Missing {
		e.Value, e.Version = nil, ""
	}
	e.Confirmed = start
	if f.generation {
		e.Sequence = mintedSequence
	}
	if e.Missing && s.negativeTTL <= 0 {
		if lease != nil {
			_ = s.tiers[skip].layer.(Leaser).Release(ctx, *lease)
		}
		return e, nil
	}

	if lease != nil {
		t := s.tiers[skip]
		err := s.fillLeased(ctx, f.since, t, *lease, e, s.retention(t, e))
		switch {
		case errors.Is(err, ErrLeaseLost):
			return e, nil
		case errors.Is(err, ErrTooLarge):
		case err != nil:
			t.breaker.failure()
		}
	}
	for i, t := range s.tiers {
		if i != skip {
			s.store(ctx, m, f, t, e)
		}
	}
	return e, nil
}

// fillLeased stores e under lease, unless the leasing tier is flushed and a
// resync came after the fill's read began: the lease is then given up.
func (s *Stack) fillLeased(ctx context.Context, since uint64, t *tier, lease Lease, e Entry, ttl time.Duration) error {
	leaser := t.layer.(Leaser)
	if t.flushed {
		s.resyncMu.RLock()
		defer s.resyncMu.RUnlock()
		if s.resyncs != since {
			_ = leaser.Release(ctx, lease)
			return ErrLeaseLost
		}
	}
	return leaser.Fill(ctx, lease, e, ttl)
}

// fillAbove copies an entry found in tier i into the tiers above it. The copy
// keeps the entry's confirmation, so it is never fresher than it was below.
func (s *Stack) fillAbove(ctx context.Context, m Mode, f fill, i int, e Entry) {
	for _, t := range s.tiers[:i] {
		s.store(ctx, m, f, t, e)
	}
}

// retention is how long t keeps e: its TTL, jittered, plus the windows in
// which an expired copy is still useful to revalidate.
func (s *Stack) retention(t *tier, e Entry) time.Duration {
	ttl := t.ttl
	if e.Missing {
		ttl = min(ttl, s.negativeTTL)
	}
	if s.jitter > 0 {
		ttl -= time.Duration(rand.Float64() * s.jitter * float64(ttl))
	}
	if !e.Missing {
		ttl += s.staleWindow + s.swrExtension
	}
	return ttl
}

// store is a best-effort fill of one tier. A tier that is failing, in a gap,
// or quarantined is left alone, and so is a flushed tier after a resync the
// fill's read began before. A fenced fill never replaces a newer version. A
// store the key's epoch moved under — a notice or a local write landed while
// the fill was reading below — is undone, since it may be older than that
// change.
func (s *Stack) store(ctx context.Context, m Mode, f fill, t *tier, e Entry) {
	if t.gaps.Load() > 0 || !t.breaker.allow() || t.quarantine.active() {
		return
	}
	if t.flushed {
		s.resyncMu.RLock()
		defer s.resyncMu.RUnlock()
		if s.resyncs != f.since {
			return
		}
	}
	ttl := s.retention(t, e)
	var err error
	if (f.coordinate == CoordinateVersionFenced || m.Fill == CoordinateVersionFenced) && t.caps.Fences {
		err = t.layer.(Fencer).SetFenced(ctx, f.layer, e, ttl)
	} else {
		err = t.layer.Set(ctx, f.layer, e, ttl)
	}
	switch {
	case err == nil:
		now := s.epochOf(f.layer)
		if now.writes != f.epoch.writes || (t.notifier >= 0 && now.notices != f.epoch.notices) {
			_ = t.layer.Delete(ctx, f.layer)
		}
	case errors.Is(err, ErrTooLarge), errors.Is(err, ErrFenced):
	default:
		t.breaker.failure()
	}
}

// leaser picks the layer that coordinates fills: the deepest one that declares
// leases, because the layer closest to the origin is the one most widely
// shared. A private Memory above a shared Redis also grants leases, but only
// within its own process, which singleflight already covers. A tier the read
// path would skip — failing, in a notice gap, or quarantined — is skipped here
// too: a lease holder reads and fills the tier it leases on.
func (s *Stack) leaser() (int, Leaser) {
	for i := len(s.tiers) - 1; i >= 0; i-- {
		t := s.tiers[i]
		if !t.caps.Leases {
			continue
		}
		if t.breaker != nil && (!t.breaker.allow() || t.gaps.Load() > 0 || t.quarantine.active()) {
			continue
		}
		return i, t.layer.(Leaser)
	}
	return -1, nil
}

// Set writes value for key in partition p. With a Store origin it writes the
// origin, then gives key a new generation, so no partition reads a copy made
// before the write; the mode's write propagation decides the rest (see
// WritePropagation). With a read-only origin it is ErrReadOnlyOrigin. With no
// origin it stores value in every layer, as p's value alone.
//
// In a write-around partition nothing is stored: over an origin the write
// only replaces the generation, and with none it drops p's value.
//
// A write the origin took but some layer did not see returns ErrPartialWrite;
// the layer is then not read by this stack until the invalidation reaches it
// or it is flushed. Under FailClosed the generation is replaced in every layer
// before the origin is written, and the write fails with ErrUnavailable,
// leaving the origin unchanged, when a layer refuses.
func (s *Stack) Set(ctx context.Context, p Partition, key string, value []byte, opts ...ModeOption) error {
	return s.write(ctx, p, key, value, false, opts)
}

// Delete removes key: from a Store origin, then from every partition's
// copies by giving key a new generation, as Set does. With a read-only origin
// it is ErrReadOnlyOrigin. With no origin it removes p's value from every
// layer.
func (s *Stack) Delete(ctx context.Context, p Partition, key string, opts ...ModeOption) error {
	return s.write(ctx, p, key, nil, true, opts)
}

// ErrPartialWrite is returned by a write the origin took while some layer
// missed the invalidation.
var ErrPartialWrite = errors.New("cache: written to the origin, but a layer missed the invalidation")

func (s *Stack) write(ctx context.Context, p Partition, key string, value []byte, del bool, opts []ModeOption) error {
	if err := s.check(p); err != nil {
		return err
	}
	m, err := s.Mode(key, opts...)
	if err != nil {
		return err
	}
	s.heal(ctx)
	if s.origin == nil {
		layerKey := p.layerKey(s.schema, key)
		if del || p.writeAround {
			return s.invalidate(ctx, layerKey)
		}
		return s.setAll(ctx, layerKey, Entry{Value: value, Confirmed: s.clock.Now()})
	}
	store, ok := s.origin.(Store)
	if !ok {
		return ErrReadOnlyOrigin
	}
	if m.Write == PropagateBehind && !p.writeAround {
		s.behind.enqueue(s, p, key, value, del)
		return nil
	}
	if m.Failure == FailClosedPolicy && m.Write != PropagateOriginOnly {
		if _, err := s.bump(ctx, key, FailClosedPolicy); err != nil {
			return fmt.Errorf("%w: the origin was not written: %w", ErrUnavailable, err)
		}
	}
	start := s.clock.Now()
	var version string
	if del {
		err = store.Remove(ctx, key)
	} else {
		version, err = store.Put(ctx, key, value)
	}
	if err != nil {
		return err
	}
	s.own.record(key, version, del, start, s.ownWindow())
	if m.Write == PropagateOriginOnly {
		return nil
	}
	generation, err := s.bump(ctx, key, FailDegrade)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPartialWrite, err)
	}
	if del || m.Write != PropagateThrough || p.writeAround || generation == "" {
		return nil
	}
	// The written value is the writer's copy only if no other write landed
	// between the origin taking it and the new generation: check, now that
	// the generation is out, with a conditional load of the version it got.
	// Unchanged, that transfers nothing; changed, the copy is the newer value.
	f := s.newFill(p.entryKey(s.schema, generation, key), s.since(), m.Fill)
	f.load = func(ctx context.Context, ifNot string) (Entry, error) { return store.Load(ctx, key, ifNot) }
	written := Entry{Value: value, Version: version}
	if _, err := s.revalidate(ctx, m, f, &written); err != nil {
		return nil // the write stands; the copy is simply not made
	}
	return nil
}

// ownWindow is how long a copy older than a write can outlive it in some
// layer: the longest retention, plus a load that began before the write.
func (s *Stack) ownWindow() time.Duration {
	return s.bound + s.staleWindow + s.swrExtension + s.loadTimeout
}

// bump gives key a new generation in every layer, so no partition's copy made
// before it is read again. Every layer is attempted, even one the breaker is
// skipping for reads, and failures are returned: a layer left on the old
// generation would keep serving the old copies, so it is quarantined until the
// new generation reaches it.
func (s *Stack) bump(ctx context.Context, key string, _ FailurePolicy) (string, error) {
	if len(s.tiers) == 0 {
		return "", nil
	}
	g := newGeneration()
	return g, s.setAll(ctx, generationKey(key), Entry{Value: []byte(g), Version: g, Sequence: bumpedSequence, Confirmed: s.clock.Now()})
}

// setAll stores e under layerKey in every layer, bottom-up, so a layer above
// is never set before the one it would refill from. A layer that refuses is
// quarantined for layerKey. A layer above one that another write, or a
// notice, touched while this one was under way is emptied of layerKey rather
// than left holding this write's version over that one's.
func (s *Stack) setAll(ctx context.Context, layerKey string, e Entry) error {
	s.touch(layerKey)
	start := s.epochOf(layerKey)
	var errs []error
	for i := len(s.tiers) - 1; i >= 0; i-- {
		t := s.tiers[i]
		err := t.layer.Set(ctx, layerKey, e, s.retention(t, e))
		switch {
		case err == nil:
			t.quarantine.clear(layerKey)
			now := s.epochOf(layerKey)
			if i < len(s.tiers)-1 && (now.writes != start.writes || (t.notifier >= 0 && now.notices != start.notices)) {
				if err := t.layer.Delete(ctx, layerKey); err != nil {
					t.quarantine.add(layerKey, s.clock.Now())
				}
			}
		case errors.Is(err, ErrTooLarge):
			// Too large to copy there: drop what the layer holds instead.
			if err := t.layer.Delete(ctx, layerKey); err != nil {
				t.quarantine.add(layerKey, s.clock.Now())
				errs = append(errs, fmt.Errorf("tier %d: %w", i, err))
			}
		default:
			t.breaker.failure()
			t.quarantine.add(layerKey, s.clock.Now())
			errs = append(errs, fmt.Errorf("tier %d: %w", i, err))
		}
	}
	s.touch(layerKey)
	return errors.Join(errs...)
}

// Invalidate drops every partition's copy of key without touching the origin
// — for a write the origin received some other way — by giving key a new
// generation; p must still match the stack. Every layer is attempted, even one
// the breaker is skipping for reads: a missed invalidation serves a stale value
// until its TTL, so failures are returned. With no origin it drops p's value.
func (s *Stack) Invalidate(ctx context.Context, p Partition, key string) error {
	if err := s.check(p); err != nil {
		return err
	}
	if s.origin == nil {
		return s.invalidate(ctx, p.layerKey(s.schema, key))
	}
	_, err := s.bump(ctx, key, FailDegrade)
	return err
}

func (s *Stack) invalidate(ctx context.Context, layerKey string) error {
	s.touch(layerKey)
	var errs []error
	for i := len(s.tiers) - 1; i >= 0; i-- {
		t := s.tiers[i]
		if err := t.layer.Delete(ctx, layerKey); err != nil {
			t.breaker.failure()
			t.quarantine.add(layerKey, s.clock.Now())
			errs = append(errs, fmt.Errorf("tier %d: %w", i, err))
			continue
		}
		t.quarantine.clear(layerKey)
	}
	s.touch(layerKey)
	return errors.Join(errs...)
}

// quarantine is the layer keys a tier missed a write of. While it holds any,
// the stack neither reads nor fills the tier; each read retries the missed
// writes as deletes, and the tier is back once they all land. Past
// quarantineLimit keys it stops counting and waits for a flush, or for every
// entry the tier held then to have expired.
type quarantine struct {
	mu       sync.Mutex
	keys     map[string]struct{}
	overflow bool
	since    time.Time
}

const quarantineLimit = 4096

func (q *quarantine) add(layerKey string, now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.keys == nil {
		q.keys = map[string]struct{}{}
	}
	if len(q.keys) >= quarantineLimit {
		q.overflow = true
	} else {
		q.keys[layerKey] = struct{}{}
	}
	q.since = now
}

// overflowed quarantines the whole tier: it missed a flush.
func (q *quarantine) overflowed(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.overflow, q.since = true, now
}

func (q *quarantine) clear(layerKey string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.keys, layerKey)
}

func (q *quarantine) active() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.overflow || len(q.keys) > 0
}

// heal retries, before any operation, the writes each quarantined tier
// missed, whether or not the operation would consult it: other processes read
// that tier, and they only see the invalidation once it lands there.
func (s *Stack) heal(ctx context.Context) {
	for _, t := range s.tiers {
		if t.quarantine.active() && t.breaker.allow() {
			s.recover(ctx, t)
		}
	}
}

// recover retries what t missed, and reports whether it may be read.
func (s *Stack) recover(ctx context.Context, t *tier) bool {
	q := &t.quarantine
	q.mu.Lock()
	if !q.overflow && len(q.keys) == 0 {
		q.mu.Unlock()
		return true
	}
	if q.overflow {
		expired := !s.clock.Now().Before(q.since.Add(t.ttl + s.staleWindow + s.swrExtension))
		q.mu.Unlock()
		if expired {
			q.reset()
			return true
		}
		if t.caps.Flushes && t.layer.(Flusher).Flush(ctx) == nil {
			q.reset()
			return true
		}
		return false
	}
	keys := make([]string, 0, len(q.keys))
	for k := range q.keys {
		keys = append(keys, k)
	}
	q.mu.Unlock()
	slices.Sort(keys)
	for _, k := range keys {
		if err := t.layer.Delete(ctx, k); err != nil {
			t.breaker.failure()
			return false
		}
		q.clear(k)
	}
	t.breaker.success()
	return !q.active()
}

func (q *quarantine) reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.keys, q.overflow = nil, false
}

// ownWrites remembers the version each key's last write through this stack
// produced, for ReadYourWrites, for as long as a copy older than it can
// outlive it in a layer.
type ownWrites struct {
	mu     sync.Mutex
	writes map[string]ownWrite
}

type ownWrite struct {
	version string
	deleted bool
	until   time.Time
}

func (o *ownWrites) record(key, version string, deleted bool, at time.Time, window time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.writes == nil {
		o.writes = map[string]ownWrite{}
	}
	if len(o.writes) >= 1024 {
		for k, w := range o.writes {
			if at.After(w.until) {
				delete(o.writes, k)
			}
		}
	}
	o.writes[key] = ownWrite{version: version, deleted: deleted, until: at.Add(window)}
}

func (o *ownWrites) lookup(key string, now time.Time) (ownWrite, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	w, ok := o.writes[key]
	if !ok || now.After(w.until) {
		return ownWrite{}, false
	}
	return w, true
}

// satisfiedBy reports whether e is known to be no older than the write. A
// version the write did not produce may be older or newer; only the origin
// can tell.
func (w ownWrite) satisfiedBy(e Entry) bool {
	if w.deleted {
		return e.Missing
	}
	return !e.Missing && w.version != "" && e.Version == w.version
}

// writeBehind queues WriteBehind writes for the origin, in order, and serves
// the writing partition's pending writes to its reads.
type writeBehind struct {
	mu       sync.Mutex
	queue    []behindWrite
	pendings map[pendingKey]behindWrite
	running  bool
	closed   bool
	next     uint64
	errs     []error
	idle     chan struct{} // closed when the queue empties
}

type pendingKey struct {
	p   Partition
	key string
}

type behindWrite struct {
	id    uint64
	p     Partition
	key   string
	value []byte
	del   bool
	at    time.Time
}

func (w *writeBehind) init() {
	w.pendings = map[pendingKey]behindWrite{}
	w.idle = make(chan struct{})
	close(w.idle)
}

func (w *writeBehind) enqueue(s *Stack, p Partition, key string, value []byte, del bool) {
	w.mu.Lock()
	w.next++
	bw := behindWrite{id: w.next, p: p, key: key, value: append([]byte(nil), value...), del: del, at: s.clock.Now()}
	w.queue = append(w.queue, bw)
	w.pendings[pendingKey{p, key}] = bw
	start := !w.running
	if start {
		w.running = true
		w.idle = make(chan struct{})
	}
	w.mu.Unlock()
	if start {
		s.run(func() { w.drain(s) })
	}
}

func (w *writeBehind) pending(p Partition, key string) (Entry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	bw, ok := w.pendings[pendingKey{p, key}]
	if !ok {
		return Entry{}, false
	}
	if bw.del {
		return Entry{Missing: true, Confirmed: bw.at}, true
	}
	return Entry{Value: append([]byte(nil), bw.value...), Confirmed: bw.at}, true
}

// drain writes the queue to the origin, oldest first, each write followed by a
// new generation for its key: copies loaded while it was pending are dropped.
func (w *writeBehind) drain(s *Stack) {
	store := s.origin.(Store)
	for {
		w.mu.Lock()
		if len(w.queue) == 0 || w.closed {
			w.running = false
			close(w.idle)
			w.mu.Unlock()
			return
		}
		bw := w.queue[0]
		w.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), s.loadTimeout)
		var err error
		var version string
		for attempt := range 5 {
			if attempt > 0 {
				_ = s.clock.Sleep(ctx, time.Duration(attempt)*50*time.Millisecond)
			}
			if bw.del {
				err = store.Remove(ctx, bw.key)
			} else {
				version, err = store.Put(ctx, bw.key, bw.value)
			}
			if err == nil {
				break
			}
		}
		if err == nil {
			s.own.record(bw.key, version, bw.del, bw.at, s.ownWindow())
			_, _ = s.bump(ctx, bw.key, FailDegrade)
		}
		cancel()

		w.mu.Lock()
		w.queue = w.queue[1:]
		if cur, ok := w.pendings[pendingKey{bw.p, bw.key}]; ok && cur.id == bw.id {
			delete(w.pendings, pendingKey{bw.p, bw.key})
		}
		if err != nil {
			err = fmt.Errorf("cache: write-behind of %q dropped: %w", bw.key, err)
			w.errs = append(w.errs, err)
		}
		w.mu.Unlock()
		if err != nil && s.behindErrors != nil {
			s.behindErrors(bw.key, err)
		}
	}
}

func (w *writeBehind) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
}

// Drain waits until every WriteBehind write acknowledged so far has reached
// the origin or been dropped, and returns the dropped ones' errors since the
// last Drain.
func (s *Stack) Drain(ctx context.Context) error {
	w := &s.behind
	w.mu.Lock()
	idle := w.idle
	w.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-idle:
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	errs := w.errs
	w.errs = nil
	return errors.Join(errs...)
}

// breaker opens after threshold consecutive failures and lets one attempt
// through per cooldown until a success closes it.
type breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu        sync.Mutex
	failures  int
	openUntil time.Time
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.now().Before(b.openUntil)
}

func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= b.threshold {
		b.openUntil = b.now().Add(b.cooldown)
		b.failures = b.threshold - 1 // one more failure after cooldown reopens it
	}
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.openUntil = time.Time{}
}
