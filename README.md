# interface-cache

The **`codefly.dev/cache`** interface: its published definition, a layered
cache library, and the conformance suite every implementation must pass.
Drivers live with the service that provides them: the Redis driver is
`github.com/codefly-dev/service-redis/cache`.

Caching is composed by the **consumer**. The services it caches never know about
it: object storage stores objects, Postgres stores rows. A provider service such
as `service-redis` runs the server and emits configuration that follows the
definition; the consumer opens the matching driver from that configuration at
runtime. Roadmap: [codefly-dev/.github#7](https://github.com/codefly-dev/.github/issues/7).

## Layout

| Path | What |
|---|---|
| `interface.codefly.yaml` | The published definition, `codefly.dev/cache@0.3.0`, in core's format: type `capability`, the `cache` configuration group (`driver`; `connection`, secret). Providers are held to it. See [The published definition](#the-published-definition). |
| `definition` | A module of its own that holds the definition to core's checks (core's loader, the Go constants, a provider's emitted configuration, evolution between published versions), so `go/cache` never depends on core. `definition/published/` records every published version. |
| `go/cache` | `Layer`, `Source`, `Store`, `Partition`, the `Stack` (any number of layers), [consistency modes](#consistency-modes) and `Capabilities`, `Open` + driver registry, `Typed[T]`, and `Memory`: a complete in-process backend that declares every layer capability (leases, notifications, flush, fenced fills, a byte budget; `Share()` gives several stacks one store). Depends on nothing but `golang.org/x/sync`. |
| `go/cache/cachetest` | Conformance suite: `Run` (the Layer contract, the layer's declared `Capabilities` against its behaviour: Leaser, Notifier, Resyncer, Fencer, byte budget) and `RunStack` (fill-once across processes, partition isolation, eviction on external writes and resyncs, a property test per mode, every [pitfall](#pitfalls) as a scenario, and the [simulation](#simulation)). A harness may provide `ExternalWrite` (change a key bypassing the driver) and must provide `Interrupt` for a Resyncer. Benchmarks per mode are in its tests. |
| `go/sources/objectstorage` | Origin adapter over the object-storage gateway client: loads conditional on ETag (it declares `ConditionalLoads`, so it keeps `Validated`). Safe only behind the consumer's own authorization check (see [Partitions](#partitions-and-authorization)). |

Each directory under `go/`, and `definition`, is its own module. This
repository holds no backend code: an implementation depends on the interface,
never the reverse. A provider service ships its driver as a module of its own
repository, proves it with `cachetest` against the server its agent runs, and
adds it to [Drivers](#drivers).

## The published definition

`interface.codefly.yaml` publishes **`codefly.dev/cache@0.3.0`** in the format
core defines (`docs/interfaces.md` in codefly-dev/core, v0.5.10). Core defines
no interface itself: it loads, binds and checks this one without knowing it.

```yaml
kind: interface
publisher: codefly.dev
name: cache
version: 0.3.0
type: capability
capability:
    configuration: cache
    keys:
        - name: driver
        - name: connection
          secret: true
```

| Key | Secret | What a consumer reads |
|---|---|---|
| `driver` | no | The driver to open, as registered with `cache.Register`, e.g. `redis`. |
| `connection` | yes | The driver-specific connection string. For `redis`: a `redis://` or `rediss://` URL. |

Both keys are required, and nothing else may appear in the group: core refuses
an undeclared key rather than let consumers start reading it.

### Drivers

| Driver | Go package | Provider |
|---|---|---|
| `redis` | `github.com/codefly-dev/service-redis/cache` | `service-redis` |

Core's format has no place for drivers, so they are listed here. A provider
adds its row when its driver passes `cachetest`.

### Providing it

A module declares which of its services provide the interface, under
`module.interface.capabilities`. Consumers reach the server over the network,
so its endpoint is exported too:

```yaml
kind: module
name: platform
services:
    - name: redis
interface:
    endpoints:
        - service: redis
          endpoint: tcp
          visibility: public
    capabilities:
        - service: redis
          implements:
              - codefly.dev/cache@0.3.0
```

The service must then emit the `cache` group exactly as the definition says.
The host checks what it emits with core's
`Module.ValidateProvidedConfiguration`: the group is there, each key once,
`connection` secret and `driver` not, no other key.

### Requiring it

A consumer depends on the interface by range instead of naming the service:

```yaml
service-dependencies:
    - interface: codefly.dev/cache@^0.3
```

Core binds the requirement to the one provider in scope that implements a
version in range, and fails when there is none or several with no
`interface-bindings` entry to choose. A provider still declaring `0.2.0` does
not satisfy `^0.3`.

### Where it is fetched

The host (the CLI) resolves an identity to its definition through an
`InterfaceResolver`; core never fetches one. `codefly.dev/cache@X.Y.Z` is the
file `interface.codefly.yaml` at the root of this repository at the tag
`vX.Y.Z`:

```
https://github.com/codefly-dev/interface-cache  ref: vX.Y.Z  path: interface.codefly.yaml
https://raw.githubusercontent.com/codefly-dev/interface-cache/v0.2.0/interface.codefly.yaml
```

A bare `vX.Y.Z` tag names an interface version and nothing else: the Go modules
are tagged under their paths (`go/cache/vX.Y.Z`), and the `definition` module is
never tagged. Core's `ResolveInterface` refuses a file whose identity is not
the one asked for, so the tag and the file's `version` must agree; CI checks
every `v*` tag for it. The resolver itself is codefly-dev/cli#833 §3, owned by
the session that owns core's release; it does not exist yet, and it should pin
the commit it fetched, since a tag can be moved.

## Use

```go
import (
    "github.com/codefly-dev/interface-cache/go/cache"
    _ "github.com/codefly-dev/service-redis/cache" // registers driver "redis"
    "github.com/codefly-dev/interface-cache/go/sources/objectstorage"
    codefly "github.com/codefly-dev/sdk-go"
)

shared, err := cache.Open(ctx, codefly.For(ctx).Service("redis"))
docs, err := cache.New(ctx,
    cache.WithTier(cache.NewMemory(), 30*time.Second),
    cache.WithTier(shared, 10*time.Minute),
    cache.WithOrigin(objectstorage.New(storageClient)), // or cache.SourceFunc for a query
    cache.WithStaleWindow(time.Hour),
)
// Only after the caller's own authorization check has passed. The partition
// key comes from the verified Work Context; codefly's SDK derives it.
p := cache.NewPartition(partitionKey)
doc, err := docs.Get(ctx, p, "reports/2026-09.json")
```

`cache.Open` takes anything with `Configuration(group, key)` and
`Secret(group, key)`; the SDK's per-service query has exactly those. It reads the
`cache` group, `driver` and `connection`, and calls the registered driver. With
a nil lookup (no provider in scope) it returns a `Memory` layer, so the same code
runs without a provider. The service declares its dependency by interface
([Requiring it](#requiring-it)) and core binds it to the provider in scope.

## Partitions and authorization

**The cache stores data, never decisions.** The authorization check runs on
every read, before the stack is consulted, exactly as it would without a cache.
A cache hit proves only that someone in the same partition loaded the value; it
says nothing about whether this caller may read it. Partitioning prevents
cross-viewer *data* leaks; it does not replace the check.

The `objectstorage` source is the clearest case. It loads with whatever
credential its gateway client is configured with. For a shared stack that must
be the consumer's service credential: the gateway then answers for the service,
and a stack over it returns any object to whoever asks with a valid partition,
so it is only safe behind the consumer's own check of the caller against the
object. A client whose per-call credentials come from the caller's context
would, through a shared load, authorize every caller in the partition as the
one whose request started the load (next paragraph).

Every `Stack` operation takes a `Partition`:

- `cache.NewPartition(key)` builds one from an opaque key the caller derives
  from who is asking. It always names the tenant, and adds a digest of the
  effective authorization view (scopes plus `authorization_revision`) when the
  data itself varies by viewer (redaction, row filtering). It never includes
  session or task ids: they carry no authorization meaning and would make every
  read a miss. codefly's SDK derives the key from a verified Work Context; this
  module never interprets identity and depends on neither core nor the SDK.
- **An origin that reads the caller's identity makes the data vary by viewer.**
  A load is shared by every caller in the partition and runs with the context
  of the one that started it. If the origin authorizes or filters with that
  context (per-call credentials, row-level security), every caller in the
  partition receives what that one caller was allowed to see, errors and
  "not found" included. Such an origin needs the authorization-view digest in
  the key, not the tenant alone.
- The stack prefixes every key with the partition in every layer. Values, fill
  leases, negative entries, in-process fill-once and invalidation notices are
  all scoped by it: two partitions never observe each other. Drivers are
  unchanged; they never see a partition.
- **A write reaches every partition.** Over an origin, each key has a
  *generation*, a random token held in the layers under a key shared by all
  partitions, and every partition's copy is stored under the current one. `Set`
  and `Delete` write the origin, then they and `Invalidate` replace the
  generation, so no partition reads a copy made before the write, and a
  load that was running during it is stored where no read looks. A stack with
  no origin holds each partition's own values, and its writes touch only the
  caller's partition.
- **Fail closed.** `Get`, `GetEntry`, `Set`, `Delete` and `Invalidate` with the
  zero `Partition` (or an empty key) return `ErrNoPartition`. A stack that holds
  data every caller may read opts out with `WithGlobal()` and passes
  `cache.Global()`; each refuses the other (`ErrWrongPartition`), so the choice
  is visible at construction and at every call site.
- **Write-around.** `cache.NewPartition(key, cache.WriteAround())` marks work done
  under an approval grant (codefly-dev/core#658). Its reads go to the origin
  alone: they consult no layer, take no fill lease, share their load with no
  other caller, and store nothing, found or not found. Its writes reach the
  origin and replace the key's generation like any write. Without an origin a
  write-around `Set` stores nothing and drops the partition's value.
- **Revocation.** Because `authorization_revision` is part of the view digest,
  a bump moves callers to a fresh partition, so no entry cached before it is
  served after it. The cost: a bump makes that view's cache cold, and the old
  partition's entries sit unused until their TTL.
- **Cost.** Each partition holds its own copy of a key, and each key over an
  origin one generation entry per layer, so a layer holds up to one copy per
  view in use. Size `MaxEntries` (and the server's memory) for that: a busy
  view evicts other tenants' entries sooner than when every viewer shared one
  copy. A read looks up the generation, then the copy: two lookups, both in
  the in-process tier when it holds them, two round trips when only the shared
  layer does.

## Consistency modes

Speed against correctness is a choice made per use, with a stated guarantee,
not an accident of TTLs. A `cache.Mode` has four independent dimensions. Each
value's guarantee is a property `cachetest` checks: in `RunStack`'s `Modes`
subtests on two processes with a clock the test moves, and in the
[simulation](#simulation) under faults. The modes are generic, not a lowest
common denominator: every one exists, each layer and origin declares what it
can do ([Capabilities](#capabilities)), and a stack refuses at construction a
mode they cannot keep. Nothing degrades silently to a weaker mode.

| Dimension | Values (default first) | Trades |
|---|---|---|
| Freshness | `TTLBounded(d)` · `StaleWhileRevalidate(d)` · `InvalidateOnWrite()` · `ReadYourWrites()` · `Validated()` · `Bypass()` | staleness against latency and origin load |
| Write propagation | `WriteInvalidate()` · `WriteThrough()` · `WriteOriginOnly()` · `WriteBehind(cache.AcceptWriteLoss)` | write latency against read-after-write |
| Fill coordination | `FillSingleflight()` · `FillUncoordinated()` · `FillLease()` · `FillVersionFenced()` | origin stampede against round trips |
| Failure | `Degrade()` · `FailClosed()` | availability against correctness |

The default mode is `TTLBounded(longest tier TTL), WriteInvalidate,
FillSingleflight, Degrade`. `Stack.Mode(key, opts...)` returns the mode an
operation on `key` runs in, or why the stack cannot keep it.

### Freshness

A copy's age is measured from `Entry.Confirmed`, the start of the load,
revalidation or write that last vouched for it, on the reader's clock plus
`WithClockSkew`.

| Mode | Guarantee (checked property) | Needs | Property test |
|---|---|---|---|
| `TTLBounded(d)` | Never serves a value last confirmed by the origin more than `d` ago, for clock skews up to `WithClockSkew`. An older copy is revalidated (conditionally when it carries a version) or reloaded. | nothing | `Modes/Freshness/TTLBounded`, `Pitfalls/ClockSkew` |
| `StaleWhileRevalidate(d)` | Never serves a value confirmed more than the TTL bound plus `d` ago. Between the two it serves the copy and runs one background revalidation per key, so a hot key never waits on the origin. | an origin | `Modes/Freshness/StaleWhileRevalidate` |
| `InvalidateOnWrite()` | Serves a copy while its layer keeps it, and never serves a value that a write through any stack sharing the deepest tier superseded more than the notifier's `NoticeBound` before the read began. While a notifier is in a gap it serves nothing from the layers above it, and after the resync nothing they held. Writes around every stack are covered only through `Stack.Invalidate` or an origin that declares `ChangeFeed`. | every tier but the deepest has a notifier below it that declares `Notifies` with a `NoticeBound`; refuses `WriteOriginOnly` | `Modes/Freshness/InvalidateOnWrite`, `Pitfalls/Invalidations` |
| `ReadYourWrites()` | The default's TTL bound, and a process never reads older than its own last acknowledged write of the key, whatever a lagging replica or a stale tier holds: a copy without the version its own write produced is checked against the origin. | a `Store` origin, or none | `Modes/Freshness/ReadYourWrites` |
| `Validated()` | Checks the origin's version with a conditional load on every read, so it never serves a version the origin had superseded when the read began. One conditional round trip per read, shared with no other caller. | an origin that declares `ConditionalLoads` | `Modes/Freshness/Validated` |
| `Bypass()` | Reads the origin alone: no layer consulted or filled, no load shared. Returns the origin's value as of the read. | an origin | `Modes/Freshness/Bypass` |

### Write propagation

| Mode | Guarantee | Needs | Property test |
|---|---|---|---|
| `WriteInvalidate()` | Writes the origin, then replaces the key's generation in every layer: no partition reads a copy made before the write. | — | `Modes/Write/Invalidate` |
| `WriteThrough()` | `WriteInvalidate`, then stores the new value as the writer's copy, so its next read is a hit. The copy is made after a conditional load of the version the origin returned, so a write that landed in between is never overwritten by this one. | — | `Modes/Write/Through` |
| `WriteOriginOnly()` | Writes the origin and touches no layer: copies catch up through their freshness mode. | an origin; refused with `InvalidateOnWrite` | `Modes/Write/OriginOnly` |
| `WriteBehind(cache.AcceptWriteLoss)` | Stores the value as the writer's copy and acknowledges, then writes the origin in the background. Until it lands, the writing process reads its own pending write in the writing partition and everyone else reads the origin; once it lands the generation is replaced again. **Loses the write if the process dies first**, hence the required acknowledgement. At most `WithWriteBehindQueue` writes (default 10,000) may be waiting; past that a write is refused with `ErrWriteBehindFull` rather than acknowledged, so an origin that stops accepting writes surfaces as an error the caller can act on instead of a queue that grows until the process dies with every write in it. `Stack.Drain` waits for the queue and returns every dropped write — refused five times, or still queued at `Stack.Close` — each also reported to `WithWriteBehindErrors`; `Stack.Close` does not wait. | a `Store` origin | `Modes/Write/Behind` |

A write the origin took while a layer missed the invalidation returns
`ErrPartialWrite`; the stack does not read that layer again until the
invalidation is replayed there or the layer is flushed.

### Fill coordination

| Mode | Guarantee | Needs | Property test |
|---|---|---|---|
| `FillSingleflight()` | Concurrent misses of one key in one partition load the origin once per process. | — | `Modes/Fill/Singleflight` |
| `FillUncoordinated()` | Every miss loads the origin. | — | `Modes/Fill/Uncoordinated` |
| `FillLease()` | A miss reaches the origin once across every process sharing the deepest tier that declares `Leases`; the others wait for that fill. A fill whose lease was revoked by a write stores nothing. | a tier that declares `Leases` | `Modes/Fill/Lease`, and `RunStack`'s fill-once check |
| `FillVersionFenced()` | Singleflight, and every fill is stored with `Fencer.SetFenced`: a fill never replaces a copy of a newer version (by `Entry.Sequence`), with or without leases, even for a write made around the stack. | an origin that declares `Sequenced`, and every tier declaring `Fences` | `Modes/Fill/VersionFenced`, `Pitfalls/StaleSet` |

Whatever the fill mode, a missing generation is minted under a lease when a
tier declares `Leases`, or fenced when every tier declares `Fences`, since
minting over a generation a write just stored would bring back the copies it
orphaned.

### Failure

| Mode | Guarantee | Property test |
|---|---|---|
| `Degrade()` | Reads never fail because a layer does: a failing layer is skipped behind a breaker (`WithBreaker`) and reads fall through to the next layer or the origin. A write reports a layer it could not invalidate as `ErrPartialWrite`. | `Modes/Failure/Degrade` |
| `FailClosed()` | Returns `ErrUnavailable` rather than skip a layer. A read that cannot consult every tier fails; a write replaces the key's generation in every tier before it writes the origin, and fails with the origin unchanged when one refuses. | `Modes/Failure/FailClosed` |

### Three levels of configuration

A later level overrides only the dimensions it sets.

```go
settings, err := cache.New(ctx,
    cache.WithTier(cache.NewMemory(), 30*time.Second),
    cache.WithTier(shared, 10*time.Minute),
    cache.WithOrigin(origin),
    // Per stack: the default for every key.
    cache.WithMode(cache.InvalidateOnWrite(), cache.FillLease()),
    // Per key space: a pattern ending in "*" is a prefix, any other one key;
    // the longest matching pattern wins, and an exact pattern beats a prefix
    // of the same length whichever was registered first.
    cache.WithKeySpace("org-settings/*", cache.Validated()),
    cache.WithKeySpace("avatars/*", cache.TTLBounded(time.Hour)),
)

// Per call: only this read pays the conditional round trip.
v, err := settings.Get(ctx, p, "billing/plan", cache.Validated())
// Per call on a write too.
err = settings.Set(ctx, p, "billing/plan", data, cache.WriteThrough())
```

`New` checks the default mode and every key space's mode. A per-call mode is
checked on first use (and remembered): a call with a mode the stack cannot
keep returns `ErrUnsupportedMode` naming the missing capability, and touches
nothing.

### Capabilities

A layer or origin declares what it can do by implementing `cache.Declarer`
(`Capabilities() cache.Capabilities`). The declaration is what counts: a layer
that implements `Leaser` but does not declare `Leases` grants the stack no
leases, and `New` refuses a layer that declares a capability without
implementing the interface behind it. `cachetest.Run` holds each declaration
to the layer's behaviour.

| Capability | Of | Backed by | Needed by | `Memory` | `objectstorage` |
|---|---|---|---|---|---|
| `Leases` | layer | `Leaser` | `FillLease` | yes | — |
| `Notifies` | layer | `Notifier`, reporting changes by any writer | `InvalidateOnWrite` | yes | — |
| `NoticeBound` | layer | every change reported within it, or the gap reported as lost within it; `NoticesBeforeReturn` when reported before the write returns | `InvalidateOnWrite` | `NoticesBeforeReturn` | — |
| `Resyncs` | layer | `Resyncer` (`SubscribeGaps(lost, resynced)`) | a notifier that can lose notices | no (never loses one) | — |
| `Flushes` | layer | `Flusher` | every layer above a `Resyncer` | yes | — |
| `Fences` | layer | `Fencer` (`SetFenced`, `ErrFenced`) | `FillVersionFenced`, every tier | yes | — |
| `ByteBudget` | layer | evicts to stay under it | — (declared, and checked by `Run`) | `MaxBytes`, default 64 MiB | — |
| `ConditionalLoads` | origin | `Load` honours `ifNotVersion` with `ErrNotModified` | `Validated` | — | yes (ETag) |
| `Sequenced` | origin | every entry carries an `Entry.Sequence` that grows with each write, deletes included | `FillVersionFenced` | — | no (ETags are unordered) |
| `ChangeFeed` | origin | `ChangeFeed` (`SubscribeChanges`) | covers writes around the stacks under `InvalidateOnWrite` | — | no |

A layer or origin that declares nothing has no capability. Drivers declare
theirs in their own repository and prove them with `cachetest`.

### Capability errors

Every refusal wraps `ErrUnsupportedMode`, names the mode, the missing
capability and where it is missing, and says what would go wrong. From `New`:

```
cache: New: default mode: cache: mode not supported by this stack: InvalidateOnWrite, WriteInvalidate, FillSingleflight, Degrade: InvalidateOnWrite needs a layer below tier 0 (*cache.Memory) that declares Notifies; there is none, so a write through another stack would never evict its copies
cache: New: key space "org-settings/*": cache: mode not supported by this stack: Validated, WriteInvalidate, FillSingleflight, Degrade: Validated needs the origin (main.query) to declare ConditionalLoads; it does not, so every read would reload the value
```

| Refused | When |
|---|---|
| `InvalidateOnWrite` | a tier above the deepest has no layer below it that declares `Notifies`; or its notifier declares no `NoticeBound`; or the write mode is `WriteOriginOnly` |
| `FillLease` | no tier declares `Leases` |
| `FillVersionFenced` | no origin, an origin that does not declare `Sequenced`, or any tier that does not declare `Fences` |
| `Validated` | no origin, or one that does not declare `ConditionalLoads` |
| `StaleWhileRevalidate`, `Bypass`, `WriteOriginOnly` | no origin |
| `ReadYourWrites` | an origin that is not a `Store` |
| `WriteBehind` | no origin, or one that is not a `Store`; a write past `WithWriteBehindQueue` is refused with `ErrWriteBehindFull` |
| `TTLBounded(d)`, `StaleWhileRevalidate(d)` | `d` not positive |
| a layer or origin | it declares a capability it does not implement, `Resyncs` or a `NoticeBound` without `Notifies` |

## Pitfalls

Every known way a multi-layer cache serves the wrong value is a named scenario
under `RunStack`'s `Pitfalls`, run on `Memory` here and by every driver's
suite. Each states the modes it applies to and the correct outcome under each.

| Scenario | Correct outcome |
|---|---|
| `StaleSet`: a slow load stores an old value after a newer write | A write through a stack, any mode: never, since the load is stored under the generation the write replaced. A write around every stack: `FillVersionFenced` refuses the older fill; other fills store it and serve it until the mode's freshness bound. |
| `StaleRefillAfterInvalidation` (module-saas-starter#942, F2): a process reads a key's old generation from the shared layer, a notice for it arrives while its own tier is still empty, then it stores what it read | Every mode: the store is undone. A notice or local write between a fill's read of a lower tier and its store into any tier voids the store, for generations and copies alike. A write or a notice voids the fills of that key alone: the stack tracks the operations in flight per cache key exactly, so writes to other keys never discard a fill. A voided store that the layer refuses to delete quarantines the tier, as a missed invalidation does. |
| `ReplicaRefill`: a reader refills its in-memory tier from a shared layer that has not caught up | Default and `InvalidateOnWrite`: once the write's notice reached the reader, it serves the write. Holds through a replica only when notices come from the server the reads do; `ReadYourWrites` holds it for the writer whatever the replica. |
| `Invalidations`: lost, delayed, reordered or duplicated notices | Lost inside a reported gap: nothing from the layers above is served during it, nor anything they held after it. Delayed: a copy is served until the notice lands (`InvalidateOnWrite`'s bound). Reordered or duplicated: harmless, a notice only evicts. A delete twice: no error, not found. |
| `Stampede`: hot key, mass expiry, missing-key penetration | `FillUncoordinated` loads once per miss, `FillSingleflight` once per process, `FillLease` once across processes. With TTL jitter `f`, copies filled together expire over `[(1-f)·TTL, TTL]`. A missing key loads the origin once per negative TTL. |
| `NegativeCaching`: a key created right after its "not found" was cached | Created through a stack, any write mode but `WriteOriginOnly`: read at once. Otherwise `TTLBounded` serves "not found" until the negative TTL, `Validated` reads it at once, `InvalidateOnWrite` after `Stack.Invalidate` or the origin's change feed. |
| `Pressure`: hot keys, memory pressure, per-view duplication | Under a byte budget far below the working set, every read is correct and the in-memory tier stays under its budget; each partition holds its own copy, so views multiply what a key costs. |
| `ClockSkew`: `Confirmed` is stamped on the loader's clock | Without `WithClockSkew`, `TTLBounded(d)` may serve a value up to `d` plus the skew old; with `WithClockSkew` at least the skew, it holds in real time. |
| `CodecSkew`: a deploy with a new value shape shares the layers | With `WithSchema` per shape, neither decodes the other's bytes, and a write by either invalidates both. |
| `PartialFailure`: an invalidation reaching some layers only, a layer down mid-write, breaker recovery | `ErrPartialWrite`, and the writer never reads the layer that missed it until the invalidation is replayed there. Down mid-write: `Degrade` writes the origin and reports `ErrPartialWrite`; `FailClosed` fails with `ErrUnavailable` and the origin unchanged. Once the layer is back, the stack replays what it missed before reading it. |
| `Identity`: partitions under every freshness | No partition reads another's view of a key (and [Partitions](#partitions-and-authorization) for the generation check). |
| `OutOfBandOriginWrites`: a write the origin receives around every stack | Measured and logged per mode: `TTLBounded(d)` stale for at most `d`; `Validated` and `Bypass` not at all; `InvalidateOnWrite` until `Stack.Invalidate`, or at once over an origin that declares `ChangeFeed`. Only as good as the origin's change feed. |

## Simulation

Hand-written cases do not find ordering bugs, so `cachetest.Simulate` runs a
deterministic, model-based simulation. Several stacks, each a process with its
own `Memory` tier, share the layer under test over one origin, the source of
truth. Seeded random reads, writes, deletes, invalidations and writes around
the stacks run across them, one key space per mode in `SimModes` (every
freshness, each write propagation, fill coordination and failure policy at
least once), with faults injected (`AllFaults`): held, reversed and duplicated
notices, notice gaps, a failing layer, clock skew, replication lag,
out-of-band writes with a change feed, crashes without draining, and other
processes' operations nested inside a layer or origin call.

Every read is held to its key space's freshness property against the origin's
history; every value must have existed, `Degrade` reads never fail, a failed
`FailClosed` write leaves the origin unchanged, and at the end every
write-behind write a live process acknowledged is in the origin.

`RunStack` runs seeds 1–6 for 300 steps, and every regression seed (each
failed on `Memory` before its fix) for 1500. On `Memory` a seed replays
exactly; on a server whose notices arrive on their own schedule the
operations and faults replay, and the oracle credits only notices the stacks
have received.

```bash
# replay one seed
CACHETEST_SIM_SEED=10182964830851443243 CACHETEST_SIM_STEPS=1500 go test -run 'TestMemoryStackFillOnce/Simulation' ./go/cache
# the longer randomized run, on demand
CACHETEST_SIM_RUNS=50 CACHETEST_SIM_STEPS=5000 go test -run 'TestMemoryStackFillOnce/Simulation' -timeout 60m ./go/cache
```

`CACHETEST_SIM_LOG=1` prints every operation of a failing run instead of the
last 60.

## Semantics

- **Reads** walk the layers top-down and fill the ones above a hit, never
  fresher than the entry was below.
- **Fill-once.** Concurrent misses for one key in one partition share one load
  in a process (`FillSingleflight`, the default). With `FillLease`, the
  deepest tier that declares `Leases` (the most widely shared) grants one fill
  lease across processes; the others wait for that fill. A load that was
  running when a write landed is stored under the generation the write
  replaced, so it cannot bring back a value older than the write; with
  `FillVersionFenced` a fill also never replaces a newer version.
- **Revalidation.** With `WithStaleWindow`, an expired entry that carries a
  version is reloaded conditionally; `ErrNotModified` renews it without moving
  the value.
- **Writes.** Over a `Store` origin, `Set`/`Delete` write the origin, then
  replace the key's generation; the [write propagation](#write-propagation)
  mode decides what else they do. Over a read-only origin they are
  `ErrReadOnlyOrigin`. With no origin, the stack is a plain cache.
- **Invalidation across processes.** A `Notifier` layer (optional) reports every
  key that changed in it, by any writer: another process through its driver, a
  client that bypasses the driver, or the server. The stack evicts the key,
  generations included, from the layers above it. A Notifier may also report
  its own client's writes; eviction is idempotent.
- **Resync.** A Notifier either never loses a notice (Memory) or is a
  `Resyncer`, which reports each gap: `lost` when notices may have stopped,
  within its `NoticeBound`, and `resynced` once they flow again. From `lost`
  the stack stops serving from and filling the layers above it and flushes
  them; on `resynced` it flushes them once more and resumes. Both flushes drop
  every partition and generation, since it cannot know which keys changed;
  those layers must be `Flusher`s (Memory is), or `New` refuses the stack. A
  fill whose read began before the gap stores nothing in them.
- **Failure.** Under `Degrade` (the default) a failing layer is skipped behind
  a breaker; reads fall through to the next layer or the origin. Under
  `FailClosed` the operation fails with `ErrUnavailable` instead.
  `Invalidate` is always attempted and reports failures, because a missed
  invalidation serves stale data; a layer that missed one is not read again
  until the invalidation is replayed there.
- **Negative caching.** An origin's `ErrNotFound` is cached for
  `WithNegativeTTL` (default 5s; 0 disables).
- **Fill leases outlive the load they guard.** `WithLeaseTTL` defaults to
  `WithLoadTimeout` (30s), which bounds that load. A lease shorter than the load
  is not a smaller optimisation: a fill whose lease expired first is stored in
  no tier at all, so while loads stay slower than the lease the key is never
  cached by any process and every read reaches the origin.

### Limits, stated

- Until the breaker opens (default 5 failures), each read pays the driver's
  timeouts and retries against an unreachable server; drivers document how to
  tune them.
- Notices arrive after the write: until one arrives, the layers above keep
  serving the old copy. That window is what `NoticeBound` declares and
  `InvalidateOnWrite` promises; a gap is reported within it. A Notifier that
  loses notices without reporting the gap breaks the contract, and leaves
  those copies until their freshness bound.
- Without a Notifier, layers above are bounded only by their freshness mode:
  keep in-process TTLs short relative to how stale a value may be, or use
  `Validated` for the reads that cannot be stale.
- Writes to the origin that bypass the stack reach cached copies only through
  the freshness mode, an explicit `Stack.Invalidate` (which reaches every
  partition), or an origin that declares `ChangeFeed`
  (`Pitfalls/OutOfBandOriginWrites` measures each).
- `WriteBehind` loses an acknowledged write when the process dies before the
  origin takes it. `Stack.Drain` before shutdown; `Stack.Close` does not wait,
  but it does report every write it abandons, through `Stack.Drain`'s error and
  `WithWriteBehindErrors`.
- A `Validated` read costs one origin round trip even on a hit; a hot key read
  that way puts its full read rate on the origin (see
  [Benchmarks](#benchmarks)).
- Processes on different interface versions must not share a layer: they key
  it differently, so neither invalidates the other's copies. Upgrade them
  together, or point the new version at an empty keyspace.

## Versioning

The interface version (`version` in `interface.codefly.yaml`) moves only when the contract
changes: the definition, or the Go API consumers or drivers program against. A
driver fix does not move it. A provider declares the interface version it
serves, so a breaking move requires each provider to declare the new version,
even when, as in 0.2.0, the configuration it emits is unchanged: a consumer
that requires `^0.2` does not bind to a provider that still declares 0.1.

Publishing a version of the definition:

1. The PR that changes the contract bumps `version` in `interface.codefly.yaml`
   and adds `definition/published/X.Y.Z/interface.codefly.yaml`, identical to it.
   A published directory is never edited afterwards.
2. CI runs core's `EvolveInterface` from each published version to the next. A
   version that under-states its surface change fails: a breaking change (a
   removed key, a flipped `secret`, a renamed group, a new required key) needs
   a new minor below 1.0.0 and a new major from it; an additive one (a new
   optional key) must not be a patch.
3. Once merged, the commit on `main` is tagged `vX.Y.Z`. CI checks that every
   `v*` tag serves a definition core resolves as exactly that identity, with
   the surface its `published/` directory records.

Core's diff is structural: it sees the group and its keys, not the Go API or
the behaviour behind them. 0.1.0 → 0.2.0 changed no key, so core finds nothing
to object to; the breaking bump was the authors' call, and a behavioural break
still is.

Modules are tagged per path (`go/cache/vX.Y.Z`,
`go/sources/objectstorage/vX.Y.Z`). `go.work` ties them together for development;
a released module requires a tagged `go/cache`. While a contract change is
untagged, a module that needs it builds against it with a `replace` directive,
dropped once `go/cache` is tagged. A driver pins the `go/cache` version it
implements.

0.2.0 is a breaking change: every `Stack` and `Typed` operation takes a
`Partition`, the stack's keys in a layer change (see Limits), and `RunStack`
gains the partition cases under a `Partitions` subtest. The Notifier contract
changes: it reports changes by any writer, may report its own client's
writes, and a Notifier that can lose notices must be a `Resyncer`. A driver
whose notifications carry only what drivers publish (Redis pub/sub) no longer
conforms. `Layer` and `Leaser` are unchanged; the Memory layer is also a
`Flusher`.

0.3.0 is a breaking change, the release notes of the consistency modes:

- **New:** the four [mode](#consistency-modes) dimensions, configurable per
  stack (`WithMode`), key space (`WithKeySpace`) and call (a `...ModeOption`
  on `Get`, `GetEntry`, `Set`, `Delete` and `Typed`); `Capabilities` and
  `Declarer`, with `New` refusing a mode the declarations cannot keep
  (`ErrUnsupportedMode`); `Fencer`, `ChangeFeed`, `ErrFenced`,
  `ErrUnavailable`, `ErrPartialWrite`; `WithSchema`, `WithClock`,
  `WithClockSkew`, `WithExecutor`, `WithWriteBehindErrors`,
  `WithWriteBehindQueue` (`ErrWriteBehindFull`), `Stack.Drain`;
  `Memory`'s byte budget (`MaxBytes`, default 64 MiB) and fenced fills; the
  `Modes`, `Pitfalls` and `Simulation` cases of `RunStack`, and `Simulate`.
- **Breaking, for consumers:** fills are leased across processes only under
  `FillLease`; the default is `FillSingleflight`, once per process.
  `Entry.FreshUntil` and `Entry.Fresh` are replaced by `Entry.Confirmed` and
  `Entry.Age`; `Entry` gains `Sequence`. Layer keys start with the key format
  `3` and carry the schema, so a 0.3.0 stack neither reads nor invalidates a
  0.2.0 stack's copies (see Limits).
- **Breaking, for drivers:** a layer's capabilities count only when declared
  through `Capabilities()`: a `Leaser` or `Notifier` that declares nothing is
  used as a plain layer, and `cachetest.Run` fails a layer that implements an
  interface it does not declare. `Resyncer.SubscribeResync(fn)` becomes
  `SubscribeGaps(lost, resynced)`: the gap is reported as it starts, within
  the declared `NoticeBound`, as well as when it ends. `InvalidateOnWrite`
  needs the notifier to declare a `NoticeBound`.
- **For origins:** `Validated` and `FillVersionFenced` need the origin to
  declare `ConditionalLoads` and `Sequenced`; an origin that declares nothing
  still serves every other mode. The `objectstorage` source declares
  `ConditionalLoads`.

The definition's configuration group is unchanged, so core's structural check
finds nothing; the bump to 0.3.0 is the behavioural judgement, and every
provider must declare `codefly.dev/cache@0.3.0`.

## Test

```bash
cd definition && go test ./...
cd go/cache && go test -race ./...
cd go/sources/objectstorage && go test -race -count=1 -timeout 30m ./...
```

`go/cache`'s tests run the whole conformance suite on `Memory`, the
simulation's fixed seeds included; see [Simulation](#simulation) for replaying
a seed and the longer randomized run.

`definition` runs against core v0.5.10's loader and checks, and reads the `v*`
tags from git. `go/sources/objectstorage` installs the object-storage gateway at the version
its `go.mod` pins and runs it on the in-memory backend.
