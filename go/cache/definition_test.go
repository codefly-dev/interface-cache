package cache_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// The Go constants are how consumers read a provider's configuration; the
// published definition is what providers are held to. They must agree.
func TestConstantsMatchPublishedDefinition(t *testing.T) {
	raw, err := os.ReadFile("../../definition/cache.json")
	if err != nil {
		t.Fatal(err)
	}
	var def struct {
		Interface     string `json:"interface"`
		Kind          string `json:"kind"`
		Configuration struct {
			Group string `json:"group"`
			Keys  []struct {
				Name   string `json:"name"`
				Secret bool   `json:"secret"`
			} `json:"keys"`
		} `json:"configuration"`
	}
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatal(err)
	}
	if def.Interface != "codefly.dev/cache" || def.Kind != "capability" {
		t.Fatalf("definition is %q of kind %q", def.Interface, def.Kind)
	}
	if def.Configuration.Group != cache.Group {
		t.Fatalf("definition group %q, cache.Group %q", def.Configuration.Group, cache.Group)
	}
	secret := map[string]bool{}
	for _, k := range def.Configuration.Keys {
		secret[k.Name] = k.Secret
	}
	want := map[string]bool{cache.KeyDriver: false, cache.KeyConnection: true}
	if len(secret) != len(want) {
		t.Fatalf("definition keys %v, Go reads %v", secret, want)
	}
	for name, isSecret := range want {
		got, ok := secret[name]
		if !ok || got != isSecret {
			t.Fatalf("key %q: definition secret=%v present=%v, Go reads it as secret=%v", name, got, ok, isSecret)
		}
	}
}
