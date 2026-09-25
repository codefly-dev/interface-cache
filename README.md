# interface-cache

The **`codefly.dev/cache`** interface: its published definition, a layered
cache library, the drivers that implement it, and the conformance suite every
implementation must pass.

Caching is composed by the **consumer**. The services it caches never know about
it: object storage stores objects, Postgres stores rows. A provider service such
as `service-redis` runs the server and emits configuration that follows the
definition; the consumer opens the matching driver from that configuration at
runtime. Roadmap: [codefly-dev/.github#7](https://github.com/codefly-dev/.github/issues/7).

## Layout

| Path | What |
|---|---|
| `definition/cache.json` | The published interface: kind `capability`, the `cache` configuration group (`driver`; `connection`, secret). Providers are held to it. |
| `go/cache` | `Layer`, `Source`, `Store`, the `Stack`, the `Memory` layer, `Open` + driver registry, `Typed[T]`. Depends on nothing but `golang.org/x/sync`. |
| `go/cache/cachetest` | Conformance suite: `Run` (Layer, Leaser, Notifier contracts) and `RunStack` (fill-once across processes). |
| `go/redis` | Driver for any Redis-protocol server (Redis, Valkey, ElastiCache, Memorystore): leases, invalidation notices. |
| `go/sources/objectstorage` | Origin adapter over the object-storage gateway client: loads conditional on ETag. |

Each directory under `go/` is its own module, so a consumer never downloads a
driver it does not import.

## Use

```go
import (
    "github.com/codefly-dev/interface-cache/go/cache"
    _ "github.com/codefly-dev/interface-cache/go/redis" // registers "redis"
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
doc, err := docs.Get(ctx, "reports/2026-09.json")
```

`cache.Open` takes anything with `Configuration(group, key)` and
`Secret(group, key)`; the SDK's per-service query has exactly those. It reads the
`cache` group, `driver` and `connection`, and calls the registered driver. With
a nil lookup (no provider in scope) it returns a `Memory` layer, so the same code
runs without a provider. Selecting the provider by interface rather than by
service name waits on codefly-dev/core#655.

## Semantics

- **Reads** walk the layers top-down and fill the ones above a hit, never
  fresher than the entry was below.
- **Fill-once.** Concurrent misses in a process share one load (singleflight).
  Across processes, the first shared layer that is a `Leaser` grants one fill
  lease; the others wait for that fill. A `Set` or `Delete` revokes an
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

- Redis invalidation notices use pub/sub, which is fire-and-forget: a notice
  sent while a subscriber is disconnected is lost, and that key stays in the
  layers above until their own TTL. Keep in-process TTLs short relative to how
  stale a value may be.
- Until the breaker opens (default 5 failures), each read pays the Redis
  client's timeouts and retries against an unreachable server. Tune them in the
  connection URL, e.g. `?dial_timeout=200ms&max_retries=0`.
- Writes that bypass the stack reach cached copies only through TTL, or through
  an explicit `Stack.Invalidate`.

## Versioning

The interface version (`definition/cache.json`) moves only when the contract
changes: the definition or the Go API that consumers and drivers share. A driver
fix does not move it. Modules are tagged per path (`go/cache/vX.Y.Z`,
`go/redis/vX.Y.Z`, …). Until `go/cache` has its first tag, the other modules
build against it with a `replace` directive, and `go.work` ties them together
for development.

## Test

```bash
cd go/cache && go test -race ./...
cd go/redis && CACHE_INFRA_TESTS=required go test -race -count=1 -timeout 30m ./...
cd go/sources/objectstorage && go test -race -count=1 -timeout 30m ./...
```

`go/redis` runs against `REDIS_URL` when set, otherwise it starts the Redis
image `service-redis` pins, with a password, in Docker. Without either it skips,
unless `CACHE_INFRA_TESTS=required`, which turns the skip into a failure; CI sets
it. `go/sources/objectstorage` installs the object-storage gateway at the version
its `go.mod` pins and runs it on the in-memory backend.
