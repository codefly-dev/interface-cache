# Working in codefly-dev/interface-cache

This repo owns the `codefly.dev/cache` interface end to end: the published
definition (`definition/cache.json`), the Go library (`go/cache`), every driver
(`go/redis`), origin adapters (`go/sources/*`) and the conformance suite
(`go/cache/cachetest`). `README.md` is the reference for semantics and limits.

It does **not** own:
- the generic `Interface` type and its checks: that is `codefly-dev/core`, which
  must never carry this interface's details;
- the servers: `service-redis` runs Redis and emits the `cache` group;
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
  away" has not been fixed until a conformance test fails without the fix. The
  suite is known to catch a missing lease check in `fillScript`: removing it
  fails `DeleteRevokesLease`, `SetRevokesLease` and `LeaseExpires`.
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

Keep `-timeout 30m`: the first run pulls the Redis image and installs the
object-storage gateway.

## Rules that bite

- **Do not mock a server.** Driver behaviour is proven by `cachetest` against a
  real server: Redis from `service-redis`'s pinned image, the gateway at the
  pinned module version. `CACHE_INFRA_TESTS=required` turns a missing server
  into a failure; a green run without it may have skipped.
- **The definition and the Go constants must agree.**
  `TestConstantsMatchPublishedDefinition` enforces it. Change them together, and
  bump the interface version when the contract changes. A driver fix does not
  bump the interface version.
- **Two pins are copies of other repos' pins.** `redisImage` in
  `go/redis/redis_test.go` mirrors `service-redis/runtime-image.json`, and the
  gateway version in `go/sources/objectstorage/go.mod` follows
  `service-object-storage` releases. Move them together with their source.
- **A lease is revoked by any write.** `Set`, `Delete` and expiry must all make
  a later `Fill` return `ErrLeaseLost`. That is what keeps a slow load from
  caching a value older than the write.
- **Keep `go/cache` dependency-free.** Anything backend-specific goes in its own
  module.

## Workflow

Branch and PR; never commit to `main`. Conventional Commits. Treat this file as
code: the PR that changes a process updates it.
