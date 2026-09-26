# Working in codefly-dev/interface-cache

This repo owns the `codefly.dev/cache` interface: the published definition
(`definition/cache.json`), the Go library (`go/cache`), origin adapters
(`go/sources/*`) and the conformance suite (`go/cache/cachetest`). It holds no
backend code. `README.md` is the reference for semantics and limits.

It does **not** own:
- the generic `Interface` type and its checks: that is `codefly-dev/core`, which
  must never carry this interface's details;
- drivers: each lives with the service that provides it. `service-redis` runs
  Redis, emits the `cache` group and ships the Redis driver
  (`github.com/codefly-dev/service-redis/cache`), proven with `cachetest`;
- the data services used as origins: `service-object-storage` stays unaware of
  caching.

## How to behave

Fleet standard: [handbook#68](https://github.com/obin-ai/handbook/issues/68).

- **A gap belongs where it lives.** If the provider emits the wrong
  configuration, fix the provider; if core cannot express something, file it on
  core. Never add a local workaround here.
- **Never hardcode what the system resolves.** Drivers take connections from the
  provider's configuration via `cache.Open`. Do not parse codefly's environment
  encoding; `Lookup` exists so the SDK owns that.
- **Diagnose, do not pattern-match.** A lease or invalidation bug that "went
  away" has not been fixed until a conformance test fails without the fix.
- **Say what you did not verify.** Name the modules and servers you actually
  ran against.

## Build and test

Each `go/*` directory is its own module; `go.work` ties them together locally.
CI (`.github/workflows/ci.yml`) checks each module with `GOWORK=off`, as a
consumer would:

```bash
go build ./... && go vet ./... && go mod tidy -diff
go test -race -count=1 -v -timeout 30m ./...
```

Keep `-timeout 30m`: the first run installs the object-storage gateway.

## Rules that bite

- **Do not mock a server.** Drivers prove themselves with `cachetest` in their
  own repository, against the server their agent runs. The object-storage
  adapter here runs against the real gateway at its pinned module version.
- **`Memory` is the reference backend.** It implements Layer, Leaser and
  Notifier, and `Share()` gives several stacks one store, the way processes
  share a server. That is how this repo tests the whole multi-layer system:
  leases, the deepest-layer lease rule and eviction across stacks, with no
  server. A change to the lease or notification contract must still be run
  against `service-redis/cache` before release.
- **The stack leases on the deepest leasing layer.** A private Memory above a
  shared layer also grants leases, but only inside one process;
  `TestMemoryStackFillOnce` fails if the stack leases on the top layer.
- **The definition and the Go constants must agree.**
  `TestConstantsMatchPublishedDefinition` enforces it. Change them together, and
  bump the interface version when the contract changes. A driver fix does not
  bump the interface version.
- **The gateway version in `go/sources/objectstorage/go.mod` follows
  `service-object-storage` releases.** Move it with them.
- **Partitioning lives in the Stack, never in a driver.** `Partition.layerKey`
  and `Partition.entryKey` are the only places a partition becomes a layer key;
  the singleflight key, leases, negative entries and notifications all follow
  from it. Do not add a partition to `Layer`, `Leaser` or `Notifier`, and never
  give a Stack operation a default partition: without one it fails closed.
  `RunStack`'s partition cases, run by `TestMemoryStackFillOnce` here and by
  every driver's suite, hold it; a change to the stack's keying must still be
  run against `service-redis/cache`.
- **A write reaches every partition through the key's generation.** Read the
  generation before loading the origin, store every copy under it, and replace
  it on every write; never mint one over an existing generation (the lease
  holder re-reads before it fills). `WritesReachEveryPartition` fails without
  each of these.
- **The cache stores data, never decisions.** Nothing here authorizes a read,
  and docs or examples must not suggest a partition does.
- **A lease is revoked by any write.** `Set`, `Delete` and expiry must all make
  a later `Fill` return `ErrLeaseLost`; `cachetest` holds every driver to it.
  That is what keeps a slow load from caching a value older than the write.
- **Keep `go/cache` dependency-free.** Anything backend-specific goes in its own
  module. It never imports core or the SDK: a partition key is opaque here.
- **An untagged contract change reaches the other modules by `replace`.** A
  module that needs it points at `../../cache` until `go/cache` is tagged; the
  release then tags `go/cache/vX.Y.Z`, requires it and drops the `replace`.

## Workflow

Branch and PR; never commit to `main`. Conventional Commits. Treat this file as
code: the PR that changes a process updates it.
