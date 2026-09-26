package definition_test

import (
	"context"
	"testing"

	"github.com/codefly-dev/core/resources"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// root is the repository root, where the definition is published.
const root = ".."

// published loads interface.codefly.yaml with core's own loader, which decodes
// strictly and validates: a misspelled field or a definition core would refuse
// fails here, not in a provider's run.
func published(t *testing.T) *resources.Interface {
	t.Helper()
	definition, err := resources.LoadInterfaceFromDir(context.Background(), root)
	if err != nil {
		t.Fatalf("interface.codefly.yaml does not load with core: %v", err)
	}
	return definition
}

func TestDefinitionIsCodeflyDevCache(t *testing.T) {
	definition := published(t)
	if got := definition.Identity().Key(); got != "codefly.dev/cache" {
		t.Fatalf("the definition publishes %s, want codefly.dev/cache", got)
	}
	if definition.Type != resources.InterfaceTypeCapability {
		t.Fatalf("codefly.dev/cache is of type %q, want %q", definition.Type, resources.InterfaceTypeCapability)
	}
}

// The Go constants are how consumers read a provider's configuration; the
// published definition is what providers are held to. They must agree.
func TestConstantsMatchPublishedDefinition(t *testing.T) {
	capability := published(t).Capability
	if capability.Configuration != cache.Group {
		t.Fatalf("definition group %q, cache.Group %q", capability.Configuration, cache.Group)
	}
	type read struct{ secret, optional bool }
	declared := map[string]read{}
	for _, key := range capability.Keys {
		declared[key.Name] = read{key.Secret, key.Optional}
	}
	// cache.Open reads both keys and fails without either, so neither may be
	// optional.
	want := map[string]read{cache.KeyDriver: {secret: false}, cache.KeyConnection: {secret: true}}
	if len(declared) != len(want) {
		t.Fatalf("definition keys %v, Go reads %v", declared, want)
	}
	for name, w := range want {
		got, ok := declared[name]
		if !ok || got != w {
			t.Fatalf("key %q: definition %+v (present=%v), Go reads it as %+v", name, got, ok, w)
		}
	}
}
