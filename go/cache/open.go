package cache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// The configuration a provider of codefly.dev/cache emits, as fixed by the
// published definition (interface.codefly.yaml at the repository root).
const (
	// Group is the configuration group a provider emits.
	Group = "cache"
	// KeyDriver names the driver that speaks to the provider, e.g. "redis".
	KeyDriver = "driver"
	// KeyConnection is the driver-specific connection string. It is secret.
	KeyConnection = "connection"
)

// Lookup reads a provider's configuration. The codefly SDK's per-service query
// satisfies it — codefly.For(ctx).Service("<provider>") — so this package
// neither imports the SDK nor depends on how codefly carries configuration
// into a process.
type Lookup interface {
	Configuration(group string, key string) (string, error)
	Secret(group string, key string) (string, error)
}

// Config is a provider's resolved configuration, handed to its driver.
type Config struct {
	Driver     string
	Connection string
}

// Opener builds a Layer from a provider's configuration. It must not require
// the provider to be reachable: a cache that is down degrades reads, it does
// not stop a consumer from starting.
type Opener func(ctx context.Context, cfg Config) (Layer, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Opener{}
)

// Register makes a driver available to Open under name. Drivers live with the
// service that provides them and call it from init, so a consumer enables one
// with a blank import. Registering a name twice panics.
func Register(name string, open Opener) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if open == nil {
		panic("cache: Register opener is nil")
	}
	if _, dup := registry[name]; dup {
		panic("cache: Register called twice for driver " + name)
	}
	registry[name] = open
}

// Open returns the shared layer of the cache provider that lookup describes,
// through the driver the provider names. With a nil lookup — no provider in
// scope — it returns a Memory layer, so a consumer runs unchanged without one.
func Open(ctx context.Context, lookup Lookup) (Layer, error) {
	if lookup == nil {
		return NewMemory(), nil
	}
	driver, err := lookup.Configuration(Group, KeyDriver)
	if err != nil {
		return nil, fmt.Errorf("cache: read %s.%s: %w", Group, KeyDriver, err)
	}
	connection, err := lookup.Secret(Group, KeyConnection)
	if err != nil {
		return nil, fmt.Errorf("cache: read %s.%s: %w", Group, KeyConnection, err)
	}
	registryMu.RLock()
	open, ok := registry[driver]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("cache: provider names driver %q, which is not registered (registered: %s); "+
			"blank-import the driver package its provider ships (the interface-cache README lists them)",
			driver, registered())
	}
	return open(ctx, Config{Driver: driver, Connection: connection})
}

func registered() string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	if len(registry) == 0 {
		return "none"
	}
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
