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
| `definition/cache.json` | The published interface: kind `capability`, the `cache` configuration group (`driver`; `connection`, secret). Providers are held to it. |
| `go/cache` | `Layer`, `Source`, `Store`, `Partition`, the `Stack` (any number of layers), `Open` + driver registry, `Typed[T]`, and `Memory`: a complete in-process backend (leases, notifications; `Share()` gives several stacks one store). Depends on nothing but `golang.org/x/sync`. |
| `go/cache/cachetest` | Conformance suite: `Run` (Layer, Leaser, Notifier contracts) and `RunStack` (fill-once across processes, partition isolation). |
| `go/sources/objectstorage` | Origin adapter over the object-storage gateway client: loads conditional on ETag. Safe only behind the consumer's own authorization check (see [Partitions](#partitions-and-authorization)). |

Each directory under `go/` is its own module. This repository holds no backend
code: an implementation depends on the interface, never the reverse. A provider
service ships its driver as a module of its own repository, proves it with
`cachetest` against the server its agent runs, and lists it under `drivers` in
the definition.

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
runs without a provider. Selecting the provider by interface rather than by
service name waits on codefly-dev/core#655.

## Partitions and authorization

**The cache stores data, never decisions.** The authorization check runs on
every read, before the stack is consulted, exactly as it would without a cache.
A cache hit proves only that someone in the same partition loaded the value; it
says nothing about whether this caller may read it. Partitioning prevents
cross-viewer *data* leaks; it does not replace the check.

The `objectstorage` source is the clearest case: it loads with the gateway
credential on its client's connection, a service credential, not the caller's.
The gateway therefore answers for the service, and a stack over it will return
any object to whoever asks with a valid partition. It is only safe behind the
consumer's own check of the caller against the object.

Every `Stack` operation takes a `Partition`:

- `cache.NewPartition(key)` builds one from an opaque key the caller derives
  from who is asking. It always names the tenant, and adds a digest of the
  effective authorization view (scopes plus `authorization_revision`) when the
  data itself varies by viewer (redaction, row filtering). It never includes
  session or task ids: they carry no authorization meaning and would make every
  read a miss. codefly's SDK derives the key from a verified Work Context; this
  module never interprets identity and depends on neither core nor the SDK.
- The stack prefixes every key with the partition in every layer
  (`Partition.LayerKey` gives the stored key). Values, fill leases, negative
  entries, in-process fill-once and invalidation notices are all scoped by it:
  two partitions never observe each other. Drivers are unchanged; they never see
  a partition.
- **Fail closed.** `Get`, `GetEntry`, `Set`, `Delete` and `Invalidate` with the
  zero `Partition` (or an empty key) return `ErrNoPartition`. A stack that holds
  data every caller may read opts out with `WithGlobal()` and passes
  `cache.Global()`; each refuses the other (`ErrWrongPartition`), so the choice
  is visible at construction and at every call site.
- **Write-around.** `cache.NewPartition(key, cache.WriteAround())` marks work done
  under an approval grant (codefly-dev/core#658). Its reads go to the origin
  alone: they consult no layer, take no fill lease, share their load with no
  other caller, and store nothing, found or not found. Its writes reach the
  origin and drop the key's copies in that partition key. Without an origin a
  write-around `Set` stores nothing.
- **Revocation.** Because `authorization_revision` is part of the view digest,
  a bump moves callers to a fresh partition, so no entry cached before it is
  served after it. The cost: a bump makes that view's cache cold, and the old
  partition's entries sit unused until their TTL.

## Semantics

- **Reads** walk the layers top-down and fill the ones above a hit, never
  fresher than the entry was below.
- **Fill-once.** Concurrent misses for one key in one partition share one load
  in a process (singleflight).
  Across processes, the deepest layer that is a `Leaser` (the most widely
  shared) grants one fill lease; the others wait for that fill. A `Set` or `Delete` revokes an
  outstanding lease, so a slow load cannot store a value older than the write.
- **Revalidation.** With `WithStaleWindow`, an expired entry that carries a
  version is reloaded conditionally; `ErrNotModified` renews it without moving
  the value.
- **Writes.** Over a `Store` origin, `Set`/`Delete` write the origin, then
  invalidate (default) or write through. Over a read-only origin they are
  `ErrReadOnlyOrigin`. With no origin, the stack is a plain cache.
- **Invalidation across processes.** A `Notifier` layer (Redis) reports keys
  other processes changed, and the stack evicts them from the layers above it.
- **Degrade.** A failing layer is skipped behind a breaker; reads fall through
  to the next layer or the origin. `Invalidate` is always attempted and reports
  failures, because a missed invalidation serves stale data.
- **Negative caching.** An origin's `ErrNotFound` is cached for
  `WithNegativeTTL` (default 5s; 0 disables).

### Limits, stated

- Invalidation notices may be lossy (Redis pub/sub is): a missed notice leaves
  the key in the layers above until their own TTL. Keep in-process TTLs short
  relative to how stale a value may be.
- Until the breaker opens (default 5 failures), each read pays the driver's
  timeouts and retries against an unreachable server; drivers document how to
  tune them.
- Writes that bypass the stack reach cached copies only through TTL, or through
  an explicit `Stack.Invalidate`.
- A write, delete or invalidation reaches the cached copies in its own
  partition only. The stack cannot enumerate partitions, so other partitions'
  copies of the same origin key are served until their TTL. Size TTLs for how
  stale another viewer's copy may be, or hold viewer-independent data in a
  `WithGlobal()` stack.

## Versioning

The interface version (`definition/cache.json`) moves only when the contract
changes: the definition or the Go API that consumers and drivers share. A driver
fix does not move it. Modules are tagged per path (`go/cache/vX.Y.Z`,
`go/sources/objectstorage/vX.Y.Z`). `go.work` ties them together for development;
a released module requires a tagged `go/cache`. While a contract change is
untagged, a module that needs it builds against it with a `replace` directive,
dropped once `go/cache` is tagged. A driver pins the `go/cache` version it
implements.

0.2.0 is a breaking change: every `Stack` and `Typed` operation takes a
`Partition`, and `RunStack` gains the partition cases. The `Layer`, `Leaser`
and `Notifier` contracts are unchanged, so a driver moves by bumping its
`go/cache` requirement; its tests that call the stack pass a partition.

## Test

```bash
cd go/cache && go test -race ./...
cd go/sources/objectstorage && go test -race -count=1 -timeout 30m ./...
```

`go/sources/objectstorage` installs the object-storage gateway at the version
its `go.mod` pins and runs it on the in-memory backend.
