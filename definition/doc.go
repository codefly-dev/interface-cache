// Package definition holds codefly.dev/cache's published definition,
// interface.codefly.yaml at the repository root, to core's checks: it loads
// with core's loader, the Go constants in go/cache equal it, a provider's
// emitted configuration conforms to it, and each published version evolves
// from the one before it. It is a module of its own so that go/cache never
// depends on core. Nothing imports it; its tests are its content.
package definition
