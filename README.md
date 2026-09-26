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
| `interface.codefly.yaml` | The published definition, `codefly.dev/cache@0.2.0`, in core's format: type `capability`, the `cache` configuration group (`driver`; `connection`, secret). Providers are held to it. See [The published definition](#the-published-definition). |
| `definition` | A module of its own that holds the definition to core's checks (core's loader, the Go constants, a provider's emitted configuration, evolution between published versions), so `go/cache` never depends on core. `definition/published/` records every published version. |
| `go/cache` | `Layer`, `Source`, `Store`, `Partition`, the `Stack` (any number of layers), `Open` + driver registry, `Typed[T]`, and `Memory`: a complete in-process backend (leases, notifications; `Share()` gives several stacks one store). Depends on nothing but `golang.org/x/sync`. |
| `go/cache/cachetest` | Conformance suite: `Run` (Layer, Leaser, Notifier and Resyncer contracts) and `RunStack` (fill-once across processes, partition isolation, eviction on external writes and resyncs). A harness may provide `ExternalWrite` (change a key bypassing the driver) and must provide `Interrupt` for a Resyncer. |
| `go/sources/objectstorage` | Origin adapter over the object-storage gateway client: loads conditional on ETag. Safe only behind the consumer's own authorization check (see [Partitions](#partitions-and-authorization)). |

Each directory under `go/`, and `definition`, is its own module. This
repository holds no backend code: an implementation depends on the interface,
never the reverse. A provider service ships its driver as a module of its own
repository, proves it with `cachetest` against the server its agent runs, and
adds it to [Drivers](#drivers).

## The published definition

`interface.codefly.yaml` publishes **`codefly.dev/cache@0.2.0`** in the format
core defines (`docs/interfaces.md` in codefly-dev/core, v0.5.10). Core defines
no interface itself: it loads, binds and checks this one without knowing it.

```yaml
kind: interface
publisher: codefly.dev
name: cache
version: 0.2.0
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
              - codefly.dev/cache@0.2.0
```

The service must then emit the `cache` group exactly as the definition says.
The host checks what it emits with core's
`Module.ValidateProvidedConfiguration`: the group is there, each key once,
`connection` secret and `driver` not, no other key.

### Requiring it

A consumer depends on the interface by range instead of naming the service:

```yaml
service-dependencies:
    - interface: codefly.dev/cache@^0.2
```

Core binds the requirement to the one provider in scope that implements a
version in range, and fails when there is none or several with no
`interface-bindings` entry to choose. A provider still declaring `0.1.0` does
not satisfy `^0.2`.

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

## Semantics

- **Reads** walk the layers top-down and fill the ones above a hit, never
  fresher than the entry was below.
- **Fill-once.** Concurrent misses for one key in one partition share one load
  in a process (singleflight).
  Across processes, the deepest layer that is a `Leaser` (the most widely
  shared) grants one fill lease; the others wait for that fill. A load that
  was running when a write landed is stored under the generation the write
  replaced, so it cannot bring back a value older than the write.
- **Revalidation.** With `WithStaleWindow`, an expired entry that carries a
  version is reloaded conditionally; `ErrNotModified` renews it without moving
  the value.
- **Writes.** Over a `Store` origin, `Set`/`Delete` write the origin, then
  replace the key's generation; `WriteThrough` also stores the new value as the
  writer's copy. Over a read-only origin they are `ErrReadOnlyOrigin`. With no
  origin, the stack is a plain cache.
- **Invalidation across processes.** A `Notifier` layer (optional) reports every
  key that changed in it, by any writer: another process through its driver, a
  client that bypasses the driver, or the server. The stack evicts the key,
  generations included, from the layers above it. A Notifier may also report
  its own client's writes; eviction is idempotent.
- **Resync.** A Notifier either never loses a notice (Memory) or is a
  `Resyncer`, which signals each gap in which notices may have been lost. On
  the signal the stack flushes every layer above it, every partition and
  generation alike, since it cannot know which keys changed; those layers must
  be `Flusher`s (Memory is), or `New` refuses the stack. A fill whose read
  began before the resync stores nothing in them.
- **Degrade.** A failing layer is skipped behind a breaker; reads fall through
  to the next layer or the origin. `Invalidate` is always attempted and reports
  failures, because a missed invalidation serves stale data.
- **Negative caching.** An origin's `ErrNotFound` is cached for
  `WithNegativeTTL` (default 5s; 0 disables).

### Limits, stated

- Until the breaker opens (default 5 failures), each read pays the driver's
  timeouts and retries against an unreachable server; drivers document how to
  tune them.
- Notices arrive after the write: until one arrives, the layers above keep
  serving the old copy. Between a gap and its resync signal they may serve
  copies of any key that changed during it. A Notifier that loses notices
  without signalling breaks the contract, and leaves those copies until their
  TTL.
- Without a Notifier, layers above are bounded only by their TTL: keep
  in-process TTLs short relative to how stale a value may be.
- Writes to the origin that bypass the stack reach cached copies only through
  TTL, or through an explicit `Stack.Invalidate`, which reaches every
  partition.
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

## Test

```bash
cd definition && go test ./...
cd go/cache && go test -race ./...
cd go/sources/objectstorage && go test -race -count=1 -timeout 30m ./...
```

`definition` runs against core v0.5.10's loader and checks, and reads the `v*`
tags from git. `go/sources/objectstorage` installs the object-storage gateway at the version
its `go.mod` pins and runs it on the in-memory backend.
