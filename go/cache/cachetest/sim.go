package cachetest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// SimConfig configures one run of the model-based simulation.
type SimConfig struct {
	// Seed picks every operation, fault and interleaving. A failure prints
	// it; CACHETEST_SIM_SEED=<seed> replays it through RunSimulation.
	Seed uint64
	// Steps is how many top-level operations run. Default 300.
	Steps int
	// Processes is how many stacks share the layer under test. Default 3.
	Processes int
	// Partitions is how many partitions the operations spread over. Default 2.
	Partitions int
	// Modes are the key spaces: key i runs in Modes[i]. Default SimModes,
	// less those the layer cannot keep.
	Modes []SimMode
	// Faults are the faults injected. Default AllFaults.
	Faults *SimFaults
}

// SimMode is one key space's mode in the simulation. The oracle checks each
// read against its freshness.
type SimMode struct {
	Name      string
	Freshness SimFreshness
	Opts      []cache.ModeOption
}

// SimFreshness is the freshness the oracle holds a key space's reads to.
type SimFreshness struct {
	// Kind is "ttl", "swr", "invalidate", "ryw", "validated" or "bypass".
	Kind string
	// Bound is the TTL bound ("ttl", "ryw"), or the bound plus the window
	// ("swr").
	Bound time.Duration
}

// SimFaults selects the injected faults.
type SimFaults struct {
	// Notices holds notices and delivers them later, reversed, twice.
	Notices bool
	// Gaps drops notices inside gaps the layer reports.
	Gaps bool
	// LayerFailures makes the layer under test fail every call for a while.
	LayerFailures bool
	// ClockSkew is the most two processes' clocks differ; each stack is
	// built WithClockSkew of it.
	ClockSkew time.Duration
	// ReplicationLag makes a process read the layer as a replica that
	// applies another process's write only when its notice arrives.
	ReplicationLag bool
	// OutOfBand writes the origin around every stack; the origin reports
	// them as a ChangeFeed.
	OutOfBand bool
	// Crashes closes a process without draining, and starts a new one.
	Crashes bool
	// Nesting runs other processes' operations inside a layer or origin
	// call: a slow load raced by a write, a read raced by an invalidation.
	Nesting bool
}

// AllFaults is every fault, with 5s of clock skew.
func AllFaults() *SimFaults {
	return &SimFaults{Notices: true, Gaps: true, LayerFailures: true, ClockSkew: 5 * time.Second,
		ReplicationLag: true, OutOfBand: true, Crashes: true, Nesting: true}
}

// The stacks' tier TTLs in the simulation, and so the default TTL bound.
const (
	simTopTTL    = 30 * time.Second
	simSharedTTL = time.Minute
	simNegative  = 10 * time.Second
)

// SimModes is the default mode set: every freshness, each write propagation,
// fill coordination and failure policy at least once.
func SimModes() []SimMode {
	ttl := func(d time.Duration) SimFreshness { return SimFreshness{Kind: "ttl", Bound: d} }
	return []SimMode{
		{"default", ttl(simSharedTTL), nil},
		{"TTLBounded", ttl(20 * time.Second), []cache.ModeOption{cache.TTLBounded(20 * time.Second), cache.FillUncoordinated()}},
		{"StaleWhileRevalidate", SimFreshness{Kind: "swr", Bound: simSharedTTL + 20*time.Second}, []cache.ModeOption{cache.StaleWhileRevalidate(20 * time.Second)}},
		{"InvalidateOnWrite+Lease", SimFreshness{Kind: "invalidate"}, []cache.ModeOption{cache.InvalidateOnWrite(), cache.FillLease()}},
		{"InvalidateOnWrite+Through+FailClosed", SimFreshness{Kind: "invalidate"}, []cache.ModeOption{cache.InvalidateOnWrite(), cache.WriteThrough(), cache.FailClosed()}},
		{"ReadYourWrites+OriginOnly", SimFreshness{Kind: "ryw", Bound: simSharedTTL}, []cache.ModeOption{cache.ReadYourWrites(), cache.WriteOriginOnly()}},
		{"ReadYourWrites+Behind", SimFreshness{Kind: "ryw", Bound: simSharedTTL}, []cache.ModeOption{cache.ReadYourWrites(), cache.WriteBehind(cache.AcceptWriteLoss)}},
		{"Validated+VersionFenced", SimFreshness{Kind: "validated"}, []cache.ModeOption{cache.Validated(), cache.FillVersionFenced()}},
		{"Bypass", SimFreshness{Kind: "bypass"}, []cache.ModeOption{cache.Bypass()}},
		{"TTLBounded+Through+FailClosed", ttl(20 * time.Second), []cache.ModeOption{cache.TTLBounded(20 * time.Second), cache.WriteThrough(), cache.FailClosed()}},
	}
}

// regressionSeeds are seeds that found a bug in the stack, each failing on
// Memory without its fix. They replay exactly only on the backend they were
// found on; elsewhere they are more seeds.
var regressionSeeds = []struct {
	seed  uint64
	steps int
	bug   string
}{
	{10182964830851443243, 1500, "a fill stored a generation a notice had replaced while it read (module-saas-starter#942, F2)"},
	{9542286670127949007, 1500, "a write-through stored its value after a newer write had landed"},
	{7184643888874414290, 1500, "a fill leased on, read from and filled a tier a notice gap bypassed"},
	{3922929514010261480, 1500, "a read consulted a tier a notice gap bypassed"},
}

// RunSimulation runs the simulation over a fixed seed set, the one CI runs,
// and the regression seeds.
// CACHETEST_SIM_SEED=<seed> replays one seed instead; CACHETEST_SIM_RUNS=<n>
// adds n random seeds of CACHETEST_SIM_STEPS steps (default 2000), the longer
// run on demand. A failure prints the last 60 operations, or all of them with
// CACHETEST_SIM_LOG=1.
func RunSimulation(t *testing.T, h Harness) {
	steps := 300
	if v, err := strconv.Atoi(os.Getenv("CACHETEST_SIM_STEPS")); err == nil && v > 0 {
		steps = v
	}
	if v := os.Getenv("CACHETEST_SIM_SEED"); v != "" {
		seed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("CACHETEST_SIM_SEED=%q: %v", v, err)
		}
		Simulate(t, h, SimConfig{Seed: seed, Steps: steps})
		return
	}
	for seed := uint64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { Simulate(t, h, SimConfig{Seed: seed, Steps: steps}) })
	}
	for _, r := range regressionSeeds {
		t.Run(fmt.Sprintf("seed=%d", r.seed), func(t *testing.T) { Simulate(t, h, SimConfig{Seed: r.seed, Steps: r.steps}) })
	}
	runs, _ := strconv.Atoi(os.Getenv("CACHETEST_SIM_RUNS"))
	if runs > 0 {
		long := 2000
		if v, err := strconv.Atoi(os.Getenv("CACHETEST_SIM_STEPS")); err == nil && v > 0 {
			long = v
		}
		for range runs {
			seed := rand.Uint64()
			t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { Simulate(t, h, SimConfig{Seed: seed, Steps: long}) })
		}
	}
}

// Simulate runs one simulation: several stacks, each a process with its own
// Memory tier, share the layer under test over one origin, the source of
// truth. Seeded random reads, writes, deletes, invalidations and writes around
// the stacks run across them, with the faults injected, and every read is held
// to its key space's freshness guarantee, checked against the origin's
// history:
//
//   - ttl(d): the value was current at some instant within d before the read
//     began, or during it;
//   - swr: the same, with d the TTL bound plus the window;
//   - validated, bypass: the value was current at some point during the read;
//   - invalidate: no write that replaced the key's generation in the shared
//     layer before the reader heard of it (a notice, its own write, a gap)
//     superseded the value;
//   - ryw: ttl of the TTL bound, and never older than the process's own last
//     acknowledged write (its pending write-behind in that partition).
//
// Every value must also have existed, reads under Degrade never fail, a
// FailClosed write that fails leaves the origin unchanged, and at the end
// every write-behind write a live process acknowledged is in the origin.
//
// On Memory a run is deterministic: a seed replays exactly. On a server whose
// notices arrive on their own schedule, the operations and faults replay; when
// each notice lands does not, and the oracle only credits notices the stacks
// have received.
func Simulate(t *testing.T, h Harness, cfg SimConfig) {
	s := newSim(t, h, cfg)
	defer s.close()
	for i := 0; i < s.cfg.Steps && !s.failed; i++ {
		s.step()
	}
	if !s.failed {
		s.finish()
	}
	if s.failed {
		tail := s.log
		if len(tail) > 60 && os.Getenv("CACHETEST_SIM_LOG") == "" {
			tail = tail[len(tail)-60:]
		}
		t.Fatalf("simulation seed %d: %s\nreplay: CACHETEST_SIM_SEED=%d go test -run '<this test>'\nlast operations:\n  %s",
			cfg.Seed, s.violation, cfg.Seed, strings.Join(tail, "\n  "))
	}
}

type sim struct {
	t      *testing.T
	h      Harness
	cfg    SimConfig
	faults SimFaults
	rng    *rand.Rand
	clock  *fakeClock
	origin *store
	ns     string
	procs  []*simProc
	modes  []SimMode

	tick      int
	log       []string
	failed    bool
	violation string
	nextValue int
	nextOp    int
	depth     int
	draining  *simProc // whose background task is running
	history   map[string][]simVersion
	pending   map[string]bool // write-behind values acknowledged, not yet committed
	committed map[int]int     // op id -> tick of the commit it made
	bumps     map[string][]int
}

type simVersion struct {
	value   string
	deleted bool
	tick    int
	at      time.Time
}

type simProc struct {
	id       int
	stack    *cache.Stack
	shared   *simLayer
	clock    skewed
	tasks    *tasks
	active   bool
	syncs    []simSync
	inGap    bool
	own      map[string]int          // key -> tick of the commit of its last acknowledged write
	behind   map[pendingSim]string   // (partition, key) -> pending value
	behindID map[pendingSim]int      // (partition, key) -> the op id of the latest pending write
	queue    []queuedWrite           // write-behind writes in the order the stack drains them
	acked    []string                // write-behind values acknowledged
	lagged   map[string]*cache.Entry // replica view: key -> what it still shows (nil: a miss)
	lagging  bool
}

type pendingSim struct{ partition, key string }

type queuedWrite struct {
	id  int
	pk  pendingSim
	del bool
}

// simSync: reads of key by the process that begin after tick x reflect every
// write committed before tick c. With shared set, c is instead whatever the
// shared layer's generation of the key reflected at x — the bumps that reached
// it — and key "" means every key.
type simSync struct {
	x, c   int
	key    string
	shared bool
}

type opKey struct{}

func newSim(t *testing.T, h Harness, cfg SimConfig) *sim {
	if cfg.Steps <= 0 {
		cfg.Steps = 300
	}
	if cfg.Processes <= 0 {
		cfg.Processes = 3
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 2
	}
	s := &sim{
		t: t, h: h, cfg: cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		clock:     newFakeClock(),
		origin:    newStore(),
		ns:        Namespace(t),
		history:   map[string][]simVersion{},
		pending:   map[string]bool{},
		committed: map[int]int{},
		bumps:     map[string][]int{},
	}
	s.faults = *AllFaults()
	if cfg.Faults != nil {
		s.faults = *cfg.Faults
	}
	s.origin.feed = s.faults.OutOfBand
	s.origin.onCommit = s.onCommit
	if s.faults.Nesting {
		s.origin.onLoad = func(ctx context.Context, key string) { s.nest(nil, "") }
	}
	modes := cfg.Modes
	if modes == nil {
		modes = SimModes()
	}
	for _, m := range modes {
		// Keep the modes the layer can keep; a refusal is RunModes' business.
		probe, err := buildStack(cache.NewMemory(), s.newShared(), s.origin, s.clock, nil, cache.WithMode(m.Opts...))
		if err != nil {
			t.Logf("simulation: mode %s is not run: %v", m.Name, err)
			continue
		}
		probe.Close()
		s.modes = append(s.modes, m)
	}
	for i := range cfg.Processes {
		s.procs = append(s.procs, s.newProc(i))
	}
	return s
}

func (s *sim) keys() int { return len(s.modes) }

func key(i int) string { return fmt.Sprintf("k%d", i) }

func (s *sim) newShared() *simLayer {
	f := newFaulty(s.h.New(s.t, s.ns))
	f.gaps = s.faults.Gaps
	return &simLayer{faulty: f, sim: s}
}

func (s *sim) newProc(id int) *simProc {
	p := &simProc{id: id, tasks: &tasks{}, own: map[string]int{}, behind: map[pendingSim]string{},
		behindID: map[pendingSim]int{}, lagged: map[string]*cache.Entry{}}
	offset := time.Duration(0)
	if s.faults.ClockSkew > 0 {
		offset = time.Duration(s.rng.Int64N(int64(s.faults.ClockSkew))) - s.faults.ClockSkew/2
	}
	p.clock = skewed{s.clock, offset}
	p.shared = s.newShared()
	p.shared.proc = p
	// A layer whose notices arrive on their own schedule is always held, so
	// they are delivered between operations.
	if bound := cache.CapabilitiesOf(p.shared.inner).NoticeBound; bound != cache.NoticesBeforeReturn || s.faults.Notices {
		p.shared.holdNotices()
	}
	if s.faults.Nesting {
		p.shared.afterGet = func(_ context.Context, layerKey string) { s.nest(p, layerKey) }
	}
	opts := []cache.Option{
		cache.WithNegativeTTL(simNegative),
		cache.WithLeaseTTL(2 * time.Second),
		cache.WithClockSkew(s.faults.ClockSkew),
		cache.WithBreaker(2, 5*time.Second),
	}
	for i, m := range s.modes {
		opts = append(opts, cache.WithKeySpace(key(i), m.Opts...))
	}
	top := cache.NewMemory(cache.MemoryClock(p.clock.Now))
	stack, err := buildStack(nil, nil, s.origin, p.clock, p.tasks.run, append([]cache.Option{
		cache.WithTier(top, simTopTTL), cache.WithTier(p.shared, simSharedTTL),
	}, opts...)...)
	if err != nil {
		s.t.Fatalf("simulation: process %d: %v", id, err)
	}
	p.stack = stack
	s.tick++
	p.syncs = append(p.syncs, simSync{x: s.tick, shared: true}) // its tiers start empty
	return p
}

func (s *sim) close() {
	for _, p := range s.procs {
		p.stack.Close()
	}
}

func (s *sim) logf(format string, args ...any) {
	s.log = append(s.log, fmt.Sprintf("[%d t=%s] ", s.tick, s.clock.Now().Format("15:04:05.000"))+fmt.Sprintf(format, args...))
}

func (s *sim) fail(format string, args ...any) {
	if s.failed {
		return
	}
	s.failed = true
	s.violation = fmt.Sprintf(format, args...)
}

func (s *sim) partition(i int) cache.Partition {
	return cache.NewPartition(fmt.Sprintf("tenant-%d", i))
}

// step runs one top-level operation, chosen by weight.
func (s *sim) step() {
	type choice struct {
		weight int
		run    func()
	}
	p := s.procs[s.rng.IntN(len(s.procs))]
	choices := []choice{
		{40, func() { s.read(p) }},
		{14, func() { s.write(p, false) }},
		{5, func() { s.write(p, true) }},
		{3, func() { s.invalidate(p) }},
		{10, func() { s.deliver(p) }},
		{8, func() {
			d := time.Duration(s.rng.IntN(15_000)) * time.Millisecond
			s.clock.Advance(d)
			s.logf("clock +%s", d)
		}},
		{6, func() { s.runTask(p) }},
	}
	if s.faults.OutOfBand {
		choices = append(choices, choice{4, func() { s.outOfBand() }})
	}
	if s.faults.Gaps {
		choices = append(choices, choice{2, func() { s.gap(p) }})
	}
	if s.faults.LayerFailures {
		choices = append(choices, choice{3, func() { s.toggleFailure(p) }})
	}
	if s.faults.ReplicationLag {
		choices = append(choices, choice{2, func() { s.toggleLag(p) }})
	}
	if s.faults.Crashes {
		choices = append(choices, choice{1, func() { s.crash(p) }})
	}
	if s.faults.Notices {
		choices = append(choices, choice{2, func() { s.toggleHold(p) }})
	}
	total := 0
	for _, c := range choices {
		total += c.weight
	}
	n := s.rng.IntN(total)
	for _, c := range choices {
		if n < c.weight {
			c.run()
			return
		}
		n -= c.weight
	}
}

// nest runs another process's operation inside the current one, at a layer or
// origin call: the interleavings the pitfalls are made of. Inside a read of
// the shared layer by reader, it may then hand reader the notices it holds,
// before reader stores what it read: a change announced between a fill's
// read and its store (module-saas-starter#942, F2).
func (s *sim) nest(reader *simProc, layerKey string) {
	if s.failed || s.depth >= 2 || s.rng.IntN(100) >= 12 {
		return
	}
	var idle []*simProc
	for _, p := range s.procs {
		if !p.active {
			idle = append(idle, p)
		}
	}
	if len(idle) == 0 {
		return
	}
	p := idle[s.rng.IntN(len(idle))]
	s.depth++
	defer func() { s.depth-- }()
	s.logf("  nested in the call above:")
	// Half the time, change the very key the outer call is reading.
	same := -1
	if i := strings.LastIndex(layerKey, ":k"); i >= 0 && s.rng.IntN(2) == 0 {
		if n, err := strconv.Atoi(layerKey[i+2:]); err == nil && n < s.keys() {
			same = n
		}
	}
	if same >= 0 {
		if s.rng.IntN(2) == 0 {
			s.writeKey(p, same, false)
		} else {
			s.invalidateKey(p, same)
		}
	} else {
		switch s.rng.IntN(5) {
		case 0, 1:
			s.read(p)
		case 2:
			s.write(p, false)
		case 3:
			s.invalidate(p)
		default:
			if s.faults.OutOfBand {
				s.outOfBand()
			} else {
				s.write(p, true)
			}
		}
	}
	if reader != nil && s.rng.IntN(2) == 0 {
		s.logf("  p%d gets its %d held notices before storing what it read", reader.id, len(reader.shared.heldKeys()))
		reader.shared.deliver(false, false, false)
	}
}

func (s *sim) begin(p *simProc) (context.Context, int) {
	p.active = true
	s.nextOp++
	s.tick++
	return context.WithValue(context.Background(), opKey{}, s.nextOp), s.nextOp
}

func (s *sim) end(p *simProc) int {
	p.active = false
	s.tick++
	return s.tick
}

func (s *sim) read(p *simProc) {
	k := s.rng.IntN(s.keys())
	part := s.rng.IntN(s.cfg.Partitions)
	fresh := s.modes[k].Freshness
	var opts []cache.ModeOption
	switch s.rng.IntN(10) {
	case 0:
		if _, err := p.stack.Mode(key(k), cache.Validated()); err == nil {
			opts, fresh = []cache.ModeOption{cache.Validated()}, SimFreshness{Kind: "validated"}
		}
	case 1:
		opts, fresh = []cache.ModeOption{cache.Bypass()}, SimFreshness{Kind: "bypass"}
	}
	mode, _ := p.stack.Mode(key(k), opts...)
	ctx, _ := s.begin(p)
	startTick, startAt, inGap := s.tick, s.clock.Now(), p.inGap
	coverage := s.coverage(p, key(k), startTick)
	own, hasOwn := p.own[key(k)]
	pendingValue, hasPending := p.behind[pendingSim{fmt.Sprint(part), key(k)}]
	v, err := p.stack.Get(ctx, s.partition(part), key(k), opts...)
	endTick := s.end(p)
	result := string(v)
	if errors.Is(err, cache.ErrNotFound) {
		result, err = "<not found>", nil
	}
	s.logf("p%d read %s/%s (%s%s) = %q %v", p.id, key(k), fmt.Sprintf("tenant-%d", part), s.modes[k].Name, optName(opts), result, err)
	if err != nil {
		if errors.Is(err, cache.ErrUnavailable) && mode.Failure == cache.FailClosedPolicy && s.faults.LayerFailures {
			return
		}
		s.fail("p%d's read of %s failed: %v", p.id, key(k), err)
		return
	}
	if hasPending && fresh.Kind != "bypass" { // Bypass reads the origin, pending or not
		if pendingValue == "<deleted>" {
			pendingValue = "<not found>"
		}
		if result != pendingValue {
			s.fail("p%d read %q in the partition of its pending write-behind %q", p.id, result, pendingValue)
		}
		return
	}
	if fresh.Kind == "invalidate" && inGap {
		coverage = max(coverage, s.lastBump(key(k), startTick)) // the tiers above are bypassed
	}
	s.check(p, key(k), result, fresh, startTick, endTick, startAt, s.clock.Now(), coverage, own, hasOwn)
}

func optName(opts []cache.ModeOption) string {
	if len(opts) == 0 {
		return ""
	}
	return ", per call " + fmt.Sprint(opts[0])
}

// coverage is the tick before which every commit of k is reflected by p's
// reads beginning at tick at.
func (s *sim) coverage(p *simProc, k string, at int) int {
	c := -1
	for _, sync := range p.syncs {
		if sync.x >= at || (sync.key != "" && sync.key != k) {
			continue
		}
		if sync.shared {
			c = max(c, s.lastBump(k, sync.x))
		} else {
			c = max(c, sync.c)
		}
	}
	return c
}

// lastBump is the tick of the last write that replaced k's generation in the
// shared layer before tick at.
func (s *sim) lastBump(k string, at int) int {
	c := -1
	for _, b := range s.bumps[cache.GenerationKey(k)] {
		if b < at {
			c = max(c, b)
		}
	}
	return c
}

// check holds a read's result to its freshness.
func (s *sim) check(p *simProc, k, result string, fresh SimFreshness, startTick, endTick int, startAt, endAt time.Time, coverage, own int, hasOwn bool) {
	history := s.history[k]
	versions := append([]simVersion{{deleted: true, tick: -1}}, history...)
	matched := false
	for i, v := range versions {
		if (result == "<not found>") != v.deleted || (!v.deleted && v.value != result) {
			continue
		}
		matched = true
		endT, endA := int(^uint(0)>>1), time.Time{}
		if i+1 < len(versions) {
			endT, endA = versions[i+1].tick, versions[i+1].at
		}
		after := func(at time.Time) bool { return endA.IsZero() || !endA.Before(at) }
		ok := false
		switch fresh.Kind {
		case "validated", "bypass":
			ok = v.tick <= endTick && endT >= startTick
		case "ttl", "swr":
			ok = after(startAt.Add(-fresh.Bound))
		case "invalidate":
			ok = endT >= coverage
		case "ryw":
			ok = after(startAt.Add(-fresh.Bound)) && (!hasOwn || v.tick >= own)
		}
		if ok {
			return
		}
	}
	if !matched {
		if s.pending[result] {
			s.fail("p%d read %q for %s, a write-behind pending in another process or partition", p.id, result, k)
			return
		}
		s.fail("p%d read %q for %s, which the origin never held", p.id, result, k)
		return
	}
	s.fail("p%d read %q for %s under %s(%s): stale (read ticks %d-%d, coverage %d, own write %d/%v; history %s)",
		p.id, result, k, fresh.Kind, fresh.Bound, startTick, endTick, coverage, own, hasOwn, s.describe(k))
}

func (s *sim) describe(k string) string {
	var parts []string
	for _, v := range s.history[k] {
		value := v.value
		if v.deleted {
			value = "<deleted>"
		}
		parts = append(parts, fmt.Sprintf("%s@%d/%s", value, v.tick, v.at.Format("15:04:05.000")))
	}
	return strings.Join(parts, " ")
}

func (s *sim) write(p *simProc, del bool) { s.writeKey(p, s.rng.IntN(s.keys()), del) }

func (s *sim) writeKey(p *simProc, k int, del bool) {
	part := s.rng.IntN(s.cfg.Partitions)
	s.nextValue++
	value := fmt.Sprintf("v%d", s.nextValue)
	mode, _ := p.stack.Mode(key(k))
	ctx, op := s.begin(p)
	var err error
	if del {
		err = p.stack.Delete(ctx, s.partition(part), key(k))
		value = "<deleted>"
	} else {
		err = p.stack.Set(ctx, s.partition(part), key(k), []byte(value))
	}
	endTick := s.end(p)
	s.logf("p%d %s %s/%s = %q: %v", p.id, map[bool]string{true: "delete", false: "set"}[del], key(k), fmt.Sprintf("tenant-%d", part), value, err)
	committedAt, committed := s.committed[op]
	switch {
	case mode.Write == cache.PropagateBehind:
		if err != nil {
			s.fail("a write-behind returned %v", err)
			return
		}
		pk := pendingSim{fmt.Sprint(part), key(k)}
		p.behind[pk] = value
		p.behindID[pk] = op
		p.queue = append(p.queue, queuedWrite{id: op, pk: pk, del: del})
		p.acked = append(p.acked, value)
		if !del {
			s.pending[value] = true
		}
		return
	case err == nil, errors.Is(err, cache.ErrPartialWrite):
		if !committed {
			s.fail("p%d's write of %s returned %v without reaching the origin", p.id, key(k), err)
			return
		}
		p.own[key(k)] = committedAt
		if mode.Write != cache.PropagateOriginOnly {
			p.syncs = append(p.syncs, simSync{x: endTick, c: endTick, key: key(k)})
		}
	case errors.Is(err, cache.ErrUnavailable) && mode.Failure == cache.FailClosedPolicy:
		if committed {
			s.fail("a FailClosed write failed (%v) after changing the origin", err)
		}
	default:
		s.fail("p%d's write of %s failed: %v", p.id, key(k), err)
	}
}

func (s *sim) invalidate(p *simProc) { s.invalidateKey(p, s.rng.IntN(s.keys())) }

func (s *sim) invalidateKey(p *simProc, k int) {
	ctx, _ := s.begin(p)
	err := p.stack.Invalidate(ctx, s.partition(0), key(k))
	endTick := s.end(p)
	s.logf("p%d invalidate %s: %v", p.id, key(k), err)
	if err == nil {
		p.syncs = append(p.syncs, simSync{x: endTick, c: endTick, key: key(k)})
	}
}

// outOfBand writes the origin around every stack.
func (s *sim) outOfBand() {
	k := s.rng.IntN(s.keys())
	s.nextValue++
	value := fmt.Sprintf("v%d", s.nextValue)
	ctx := context.WithValue(context.Background(), opKey{}, -1) // not a stack's, not a drain's
	if s.rng.IntN(4) == 0 {
		s.logf("origin delete %s, around the stacks", key(k))
		s.origin.commit(ctx, key(k), "", true)
		return
	}
	s.logf("origin set %s = %q, around the stacks", key(k), value)
	s.origin.commit(ctx, key(k), value, false)
}

func (s *sim) onCommit(k string, v storedValue, ctx context.Context) {
	s.tick++
	s.history[k] = append(s.history[k], simVersion{value: v.value, deleted: v.deleted, tick: s.tick, at: s.clock.Now()})
	delete(s.pending, v.value)
	if op, ok := ctx.Value(opKey{}).(int); ok {
		s.committed[op] = s.tick
		return
	}
	// An untagged commit is a write-behind write landing, the next in the
	// draining process's queue.
	p := s.draining
	if p == nil || len(p.queue) == 0 {
		return
	}
	w := p.queue[0]
	p.queue = p.queue[1:]
	if w.pk.key != k || w.del != v.deleted {
		s.fail("p%d's write-behind landed out of order: %s (deleted %v), expected %s", p.id, k, v.deleted, w.pk.key)
		return
	}
	if p.behindID[w.pk] == w.id {
		delete(p.behind, w.pk)
		delete(p.behindID, w.pk)
	}
	p.own[k] = s.tick
}

func (s *sim) deliver(p *simProc) {
	reversed, twice := s.rng.IntN(3) == 0, s.rng.IntN(4) == 0
	s.logf("p%d gets %d held notices (reversed %v, twice %v)", p.id, len(p.shared.heldKeys()), reversed, twice)
	p.shared.deliver(reversed, twice, false)
}

func (s *sim) toggleHold(p *simProc) {
	if cache.CapabilitiesOf(p.shared.inner).NoticeBound != cache.NoticesBeforeReturn {
		return // always held
	}
	p.shared.mu.Lock()
	hold := !p.shared.hold
	p.shared.mu.Unlock()
	if hold {
		s.logf("p%d holds its notices", p.id)
		p.shared.holdNotices()
		return
	}
	s.logf("p%d gets its notices as they come", p.id)
	p.shared.deliver(false, false, true)
}

func (s *sim) runTask(p *simProc) {
	if p.tasks.len() == 0 {
		return
	}
	s.logf("p%d runs a background task", p.id)
	p.active, s.draining = true, p
	p.tasks.runOne()
	p.active, s.draining = false, nil
}

func (s *sim) gap(p *simProc) {
	s.tick++
	if p.inGap {
		s.logf("p%d's notices flow again (resync)", p.id)
		p.inGap = false
		clear(p.lagged)
		p.shared.resync()
		s.tick++
		p.syncs = append(p.syncs, simSync{x: s.tick, shared: true})
		return
	}
	s.logf("p%d loses its notices (gap)", p.id)
	p.inGap = true
	clear(p.lagged)
	p.shared.loseNotices()
}

func (s *sim) toggleFailure(p *simProc) {
	failing := !p.shared.failing.Load()
	s.logf("p%d's shared layer failing: %v", p.id, failing)
	p.shared.failing.Store(failing)
}

func (s *sim) toggleLag(p *simProc) {
	p.lagging = !p.lagging
	clear(p.lagged)
	s.logf("p%d reads through a lagging replica: %v", p.id, p.lagging)
}

func (s *sim) crash(p *simProc) {
	s.logf("p%d crashes, losing %d pending write-behind writes; a new process takes its place", p.id, len(p.behind))
	p.stack.Close()
	for i, x := range s.procs {
		if x == p {
			s.procs[i] = s.newProc(p.id)
		}
	}
}

// finish delivers everything, runs every background task and drains every
// live process: each write-behind write a live process acknowledged must be in
// the origin.
func (s *sim) finish() {
	for _, p := range s.procs {
		p.shared.deliver(false, false, true)
		s.draining = p
		p.tasks.runAll()
		s.draining = nil
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.stack.Drain(ctx)
		cancel()
		if err != nil {
			s.fail("p%d's drain: %v", p.id, err)
			return
		}
		for _, value := range p.acked {
			if value != "<deleted>" && s.pending[value] {
				s.fail("p%d acknowledged write-behind %q, which never reached the origin", p.id, value)
				return
			}
		}
	}
}

// simLayer is one process's client of the layer under test, with the
// simulation's faults, and the observations its oracle needs: which
// generations the shared layer replaced when, which notices each process got,
// and a replica view that applies other processes' writes only when their
// notice arrives.
type simLayer struct {
	*faulty
	sim  *sim
	proc *simProc
}

func (l *simLayer) Get(ctx context.Context, key string) (cache.Entry, error) {
	if p := l.proc; p != nil && p.lagging && !p.inGap {
		if e, ok := p.lagged[key]; ok {
			if err := l.fail(); err != nil {
				return cache.Entry{}, err
			}
			if l.afterGet != nil {
				l.afterGet(ctx, key)
			}
			if e == nil {
				return cache.Entry{}, cache.ErrMiss
			}
			return *e, nil
		}
	}
	return l.faulty.Get(ctx, key)
}

// writing snapshots key for every other lagging process before the write, and
// clears this process's own view of it: its writes are waited for.
func (l *simLayer) writing(key string) {
	for _, p := range l.sim.procs {
		if p == l.proc || !p.lagging || p.inGap {
			continue
		}
		if _, ok := p.lagged[key]; ok {
			continue
		}
		if e, err := l.inner.Get(context.Background(), key); err == nil {
			p.lagged[key] = &e
		} else {
			p.lagged[key] = nil
		}
	}
	if l.proc != nil {
		delete(l.proc.lagged, key)
	}
}

func (l *simLayer) wrote(key string, e cache.Entry, deleted bool, err error) {
	if err != nil || !strings.HasPrefix(key, cache.GenerationKey("")) {
		return
	}
	if deleted || e.Sequence > 0 {
		l.sim.tick++
		l.sim.bumps[key] = append(l.sim.bumps[key], l.sim.tick)
	}
	if l.sim.faults.Nesting {
		l.sim.nest(nil, "")
	}
}

func (l *simLayer) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	l.writing(key)
	err := l.faulty.Set(ctx, key, e, ttl)
	l.wrote(key, e, false, err)
	return err
}

func (l *simLayer) SetFenced(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	l.writing(key)
	err := l.faulty.SetFenced(ctx, key, e, ttl)
	l.wrote(key, e, false, err)
	return err
}

func (l *simLayer) Fill(ctx context.Context, lease cache.Lease, e cache.Entry, ttl time.Duration) error {
	l.writing(lease.Key)
	err := l.faulty.Fill(ctx, lease, e, ttl)
	l.wrote(lease.Key, e, false, err)
	return err
}

func (l *simLayer) Delete(ctx context.Context, key string) error {
	l.writing(key)
	err := l.faulty.Delete(ctx, key)
	l.wrote(key, cache.Entry{}, true, err)
	return err
}

// Subscribe records each notice this process's stack receives: the replica
// view applies the write first, and once the stack has acted on a
// generation's notice, its reads reflect every generation the shared layer
// held before it.
func (l *simLayer) Subscribe(ctx context.Context, fn func(string)) (func(), error) {
	return l.faulty.Subscribe(ctx, func(key string) {
		if p := l.proc; p != nil {
			delete(p.lagged, key)
		}
		fn(key)
		if p := l.proc; p != nil && strings.HasPrefix(key, cache.GenerationKey("")) {
			l.sim.tick++
			k := strings.TrimPrefix(key, cache.GenerationKey(""))
			p.syncs = append(p.syncs, simSync{x: l.sim.tick, key: k, shared: true})
		}
	})
}
