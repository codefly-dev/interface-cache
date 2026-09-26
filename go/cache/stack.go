package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// WritePolicy decides what a write through the stack does to the layers.
type WritePolicy int

const (
	// WriteInvalidate writes the origin, then deletes the key from every
	// layer; the next read loads the new value. The default.
	WriteInvalidate WritePolicy = iota
	// WriteThrough writes the origin, then stores the new value and version in
	// every layer.
	WriteThrough
)

// Stack reads through its layers, top first, down to the origin, and fills
// the layers on the way back. Build one with New; it is safe for concurrent
// use.
//
// Every operation takes a Partition, and fails closed without one: the stack
// never serves one partition's data, fill or negative entry to another. A
// stack built WithGlobal takes Global instead.
type Stack struct {
	tiers           []*tier
	origin          Source
	global          bool
	policy          WritePolicy
	negativeTTL     time.Duration
	leaseTTL        time.Duration
	loadTimeout     time.Duration
	staleWindow     time.Duration
	jitter          float64
	breakerFailures int
	breakerCooldown time.Duration
	now             func() time.Time

	flight singleflight.Group
	stops  []func()
}

type tier struct {
	layer   Layer
	ttl     time.Duration
	breaker *breaker
}

// Option configures a Stack.
type Option func(*Stack) error

// WithTier appends a layer below the ones already added. Entries it stores are
// fresh for ttl.
func WithTier(layer Layer, ttl time.Duration) Option {
	return func(s *Stack) error {
		if layer == nil {
			return errors.New("cache: WithTier: nil layer")
		}
		if ttl <= 0 {
			return fmt.Errorf("cache: WithTier: ttl must be positive, got %s", ttl)
		}
		s.tiers = append(s.tiers, &tier{layer: layer, ttl: ttl})
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

// WithWritePolicy selects what Set does to the layers after writing a Store
// origin. Default WriteInvalidate.
func WithWritePolicy(policy WritePolicy) Option {
	return func(s *Stack) error { s.policy = policy; return nil }
}

// WithNegativeTTL sets how long an origin's ErrNotFound is cached. Zero
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

// WithStaleWindow keeps entries this long past their freshness so an expired
// entry that carries a version is revalidated with a conditional load instead
// of reloaded. Default 0.
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

// New builds a stack. It needs at least one layer or an origin. Layers that are
// Notifiers are subscribed here, so New fails when one cannot be reached.
func New(ctx context.Context, opts ...Option) (*Stack, error) {
	s := &Stack{
		negativeTTL:     5 * time.Second,
		leaseTTL:        5 * time.Second,
		loadTimeout:     30 * time.Second,
		jitter:          0.1,
		breakerFailures: 5,
		breakerCooldown: 10 * time.Second,
		now:             time.Now,
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	if len(s.tiers) == 0 && s.origin == nil {
		return nil, errors.New("cache: New: a stack needs at least one tier or an origin")
	}
	for _, t := range s.tiers {
		t.breaker = &breaker{threshold: s.breakerFailures, cooldown: s.breakerCooldown, now: s.now}
	}
	for i, t := range s.tiers {
		notifier, ok := t.layer.(Notifier)
		if !ok || i == 0 {
			continue
		}
		above := s.tiers[:i]
		stop, err := notifier.Subscribe(ctx, func(key string) {
			for _, a := range above {
				_ = a.layer.Delete(context.Background(), key)
			}
		})
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("cache: subscribe to invalidations of tier %d: %w", i, err)
		}
		s.stops = append(s.stops, stop)
	}
	return s, nil
}

// Close stops the stack's invalidation subscriptions. It does not close the
// layers or the origin; their owner does.
func (s *Stack) Close() {
	for _, stop := range s.stops {
		stop()
	}
	s.stops = nil
}

// Get returns the value for key in partition p. It is ErrNotFound when the
// origin holds none, and ErrMiss when the stack has no origin and no layer
// holds the key.
func (s *Stack) Get(ctx context.Context, p Partition, key string) ([]byte, error) {
	e, err := s.GetEntry(ctx, p, key)
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
// In a write-around partition the read goes to the origin alone: it consults
// no layer, takes no lease, shares its load with no other caller and stores
// nothing. Without an origin it is ErrMiss.
func (s *Stack) GetEntry(ctx context.Context, p Partition, key string) (Entry, error) {
	sk, err := s.scope(p, key)
	if err != nil {
		return Entry{}, err
	}
	if sk.writeAround {
		return s.loadAround(ctx, sk)
	}
	now := s.now()
	var stale *Entry
	for i, t := range s.tiers {
		if !t.breaker.allow() {
			continue
		}
		e, err := t.layer.Get(ctx, sk.layer)
		if errors.Is(err, ErrMiss) {
			t.breaker.success()
			continue
		}
		if err != nil {
			t.breaker.failure()
			continue
		}
		t.breaker.success()
		if e.Fresh(now) {
			s.fillAbove(ctx, i, sk.layer, e)
			return e, nil
		}
		if stale == nil {
			stale = &e
		}
	}
	if s.origin == nil {
		return Entry{}, ErrMiss
	}
	return s.load(ctx, sk, stale)
}

// scoped is one operation's key: key is what the origin knows it by, layer
// what the layers store it under in the operation's partition.
type scoped struct {
	key         string
	layer       string
	writeAround bool
}

// scope checks p against the stack and derives the layer key. It is the one
// place a partition becomes a key, so every layer, lease, negative entry and
// in-process load below it is scoped by construction.
func (s *Stack) scope(p Partition, key string) (scoped, error) {
	switch {
	case !p.global && p.key == "":
		return scoped{}, ErrNoPartition
	case s.global && !p.global:
		return scoped{}, fmt.Errorf("%w: the stack is built WithGlobal, so it takes cache.Global(), not a caller's partition", ErrWrongPartition)
	case !s.global && p.global:
		return scoped{}, fmt.Errorf("%w: cache.Global() is only for a stack built WithGlobal", ErrWrongPartition)
	}
	return scoped{key: key, layer: p.LayerKey(key), writeAround: p.writeAround}, nil
}

// loadAround serves a write-around read from the origin, stored nowhere and
// shared with no other caller: the load ran under the caller's own grant.
func (s *Stack) loadAround(ctx context.Context, sk scoped) (Entry, error) {
	if s.origin == nil {
		return Entry{}, ErrMiss
	}
	lctx, cancel := context.WithTimeout(ctx, s.loadTimeout)
	defer cancel()
	e, err := s.origin.Load(lctx, sk.key, "")
	if errors.Is(err, ErrNotFound) {
		return Entry{Missing: true}, nil
	}
	return e, err
}

// load collapses concurrent misses for one layer key — one key in one
// partition — in this process into one call to loadShared, and lets each
// caller stop waiting when its own context ends.
func (s *Stack) load(ctx context.Context, sk scoped, stale *Entry) (Entry, error) {
	ch := s.flight.DoChan(sk.layer, func() (any, error) {
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.loadTimeout)
		defer cancel()
		return s.loadShared(lctx, sk, stale)
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

// loadShared collapses misses across processes: the deepest layer that grants
// leases decides which process loads the origin; the others wait for its fill.
func (s *Stack) loadShared(ctx context.Context, sk scoped, stale *Entry) (Entry, error) {
	idx, leaser := s.leaser()
	if leaser == nil {
		return s.loadOrigin(ctx, sk, stale, -1, nil)
	}
	t := s.tiers[idx]
	giveUp := s.now().Add(2 * s.leaseTTL)
	for {
		lease, ok, err := leaser.Acquire(ctx, sk.layer, s.leaseTTL)
		if err != nil {
			t.breaker.failure()
			return s.loadOrigin(ctx, sk, stale, idx, nil)
		}
		if ok {
			return s.loadOrigin(ctx, sk, stale, idx, &lease)
		}
		e, filled, err := s.waitForFill(ctx, leaser, sk.layer)
		if err != nil {
			return Entry{}, err
		}
		if filled {
			s.fillAbove(ctx, idx, sk.layer, e)
			return e, nil
		}
		if !s.now().Before(giveUp) {
			// The holder never filled, twice over: load without coordinating
			// rather than wait forever, and leave the shared layer alone.
			return s.loadOrigin(ctx, sk, stale, idx, nil)
		}
	}
}

// waitForFill polls the leasing layer until another process fills key or the
// lease it holds must have expired.
func (s *Stack) waitForFill(ctx context.Context, leaser Leaser, key string) (Entry, bool, error) {
	deadline := s.now().Add(s.leaseTTL)
	delay := 5 * time.Millisecond
	for s.now().Before(deadline) {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Entry{}, false, ctx.Err()
		case <-timer.C:
		}
		e, err := leaser.Get(ctx, key)
		if err == nil && e.Fresh(s.now()) {
			return e, true, nil
		}
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
	return Entry{}, false, nil
}

// loadOrigin loads sk from the origin and stores the result. With a lease,
// the leasing layer is filled under it, and a lost lease means nothing is
// stored anywhere: the key changed while loading. Without a lease, the leasing
// layer at skip (if any) is left alone, since storing there unleased could
// overwrite a newer value.
func (s *Stack) loadOrigin(ctx context.Context, sk scoped, stale *Entry, skip int, lease *Lease) (Entry, error) {
	ifNot := ""
	if stale != nil && !stale.Missing {
		ifNot = stale.Version
	}
	e, err := s.origin.Load(ctx, sk.key, ifNot)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotModified) && ifNot != "":
		e = *stale
	case errors.Is(err, ErrNotFound):
		e = Entry{Missing: true}
	default:
		if lease != nil {
			_ = s.tiers[skip].layer.(Leaser).Release(ctx, *lease)
		}
		return Entry{}, err
	}
	if e.Missing && s.negativeTTL <= 0 {
		if lease != nil {
			_ = s.tiers[skip].layer.(Leaser).Release(ctx, *lease)
		}
		return e, nil
	}

	now := s.now()
	if lease != nil {
		t := s.tiers[skip]
		stored, ttl := s.stamp(e, t.ttl, now)
		err := t.layer.(Leaser).Fill(ctx, *lease, stored, ttl)
		switch {
		case errors.Is(err, ErrLeaseLost):
			return e, nil
		case errors.Is(err, ErrTooLarge):
		case err != nil:
			t.breaker.failure()
		}
	}
	for i, t := range s.tiers {
		if i == skip {
			continue
		}
		stored, ttl := s.stamp(e, t.ttl, now)
		s.store(ctx, t, sk.layer, stored, ttl)
	}
	stored, _ := s.stamp(e, s.shortestTTL(), now)
	return stored, nil
}

// fillAbove copies an entry found in tier i into the tiers above it, never
// fresher than it was below.
func (s *Stack) fillAbove(ctx context.Context, i int, key string, e Entry) {
	now := s.now()
	for _, t := range s.tiers[:i] {
		stored, ttl := s.stamp(e, t.ttl, now)
		if !e.FreshUntil.IsZero() && e.FreshUntil.Before(stored.FreshUntil) {
			stored.FreshUntil = e.FreshUntil
			ttl = e.FreshUntil.Sub(now)
			if !e.Missing {
				ttl += s.staleWindow
			}
		}
		if ttl > 0 {
			s.store(ctx, t, key, stored, ttl)
		}
	}
}

// stamp sets e's freshness for a tier with the given TTL and returns how long
// the tier should keep it.
func (s *Stack) stamp(e Entry, ttl time.Duration, now time.Time) (Entry, time.Duration) {
	if e.Missing {
		ttl = min(ttl, s.negativeTTL)
	}
	if s.jitter > 0 {
		ttl -= time.Duration(rand.Float64() * s.jitter * float64(ttl))
	}
	e.FreshUntil = now.Add(ttl)
	if !e.Missing {
		ttl += s.staleWindow
	}
	return e, ttl
}

// store is a best-effort fill: a layer the breaker is skipping is left alone.
func (s *Stack) store(ctx context.Context, t *tier, key string, e Entry, ttl time.Duration) {
	if !t.breaker.allow() {
		return
	}
	err := t.layer.Set(ctx, key, e, ttl)
	if err != nil && !errors.Is(err, ErrTooLarge) {
		t.breaker.failure()
	}
}

// leaser picks the layer that coordinates fills: the deepest one that grants
// leases, because the layer closest to the origin is the one most widely
// shared. A private Memory above a shared Redis also grants leases, but only
// within its own process, which singleflight already covers.
func (s *Stack) leaser() (int, Leaser) {
	for i := len(s.tiers) - 1; i >= 0; i-- {
		t := s.tiers[i]
		if l, ok := t.layer.(Leaser); ok && t.breaker.allow() {
			return i, l
		}
	}
	return -1, nil
}

func (s *Stack) shortestTTL() time.Duration {
	if len(s.tiers) == 0 {
		return s.negativeTTL
	}
	shortest := s.tiers[0].ttl
	for _, t := range s.tiers[1:] {
		shortest = min(shortest, t.ttl)
	}
	return shortest
}

// Set writes value for key in partition p. With a Store origin it writes the
// origin first, then applies the write policy; with a read-only origin it is
// ErrReadOnlyOrigin; with no origin it stores value in every layer.
//
// The layers are written or invalidated in p only: copies of the same origin
// key in other partitions keep being served until their TTL. In a
// write-around partition nothing is stored; the write drops p's copies.
func (s *Stack) Set(ctx context.Context, p Partition, key string, value []byte) error {
	sk, err := s.scope(p, key)
	if err != nil {
		return err
	}
	e := Entry{Value: value}
	if s.origin != nil {
		store, ok := s.origin.(Store)
		if !ok {
			return ErrReadOnlyOrigin
		}
		version, err := store.Put(ctx, sk.key, value)
		if err != nil {
			return err
		}
		if s.policy == WriteInvalidate || sk.writeAround {
			return s.invalidate(ctx, sk.layer)
		}
		e.Version = version
	} else if sk.writeAround {
		return s.invalidate(ctx, sk.layer)
	}
	// Bottom-up, so a layer above is never set before the one it would refill
	// from.
	now := s.now()
	var errs []error
	for i := len(s.tiers) - 1; i >= 0; i-- {
		t := s.tiers[i]
		stored, ttl := s.stamp(e, t.ttl, now)
		if err := t.layer.Set(ctx, sk.layer, stored, ttl); err != nil && !errors.Is(err, ErrTooLarge) {
			errs = append(errs, fmt.Errorf("tier %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Delete removes key: from a Store origin first, then from every layer in
// partition p. With a read-only origin it is ErrReadOnlyOrigin. Other
// partitions' copies expire with their TTL.
func (s *Stack) Delete(ctx context.Context, p Partition, key string) error {
	sk, err := s.scope(p, key)
	if err != nil {
		return err
	}
	if s.origin != nil {
		store, ok := s.origin.(Store)
		if !ok {
			return ErrReadOnlyOrigin
		}
		if err := store.Remove(ctx, sk.key); err != nil {
			return err
		}
	}
	return s.invalidate(ctx, sk.layer)
}

// Invalidate drops key from every layer in partition p without touching the
// origin — for a write the origin received some other way. Every layer is
// attempted, even one the breaker is skipping for reads: a missed invalidation
// serves a stale value until its TTL, so failures are returned. Other
// partitions' copies of key are not reached; the stack cannot enumerate
// partitions.
func (s *Stack) Invalidate(ctx context.Context, p Partition, key string) error {
	sk, err := s.scope(p, key)
	if err != nil {
		return err
	}
	return s.invalidate(ctx, sk.layer)
}

func (s *Stack) invalidate(ctx context.Context, layerKey string) error {
	var errs []error
	for i := len(s.tiers) - 1; i >= 0; i-- {
		if err := s.tiers[i].layer.Delete(ctx, layerKey); err != nil {
			errs = append(errs, fmt.Errorf("tier %d: %w", i, err))
		}
	}
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
