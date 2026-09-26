package definition_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
)

// resolve serves the definition at the repository root, the one this commit
// publishes, the way the CLI's InterfaceResolver serves the one it fetched and
// pinned. core's ResolveInterface refuses it for any other identity.
func resolve(ctx context.Context, _ *resources.InterfaceIdentity) (*resources.Interface, error) {
	return resources.LoadInterfaceFromDir(ctx, root)
}

// providerWorkspace writes a workspace whose platform module runs
// service-redis and declares that it provides the published definition, and
// whose apps module has a service requiring it by range, as the README tells
// providers and consumers to.
func providerWorkspace(t *testing.T, definition *resources.Interface, requirement string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"workspace.codefly.yaml": "name: product\nlayout: modules\nmodules:\n    - name: platform\n    - name: apps\n",
		"modules/platform/module.codefly.yaml": fmt.Sprintf(`kind: module
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
              - %s
`, definition.Identity()),
		"modules/platform/services/redis/service.codefly.yaml": `kind: service
name: redis
version: 0.0.0
agent:
    kind: codefly:service
    name: redis
    version: 0.0.94
    publisher: codefly.dev
endpoints:
    - name: tcp
      api: tcp
      visibility: public
`,
		"modules/apps/module.codefly.yaml": "kind: module\nname: apps\nservices:\n    - name: web\n",
		"modules/apps/services/web/service.codefly.yaml": fmt.Sprintf(`kind: service
name: web
version: 0.0.0
agent:
    kind: codefly:service
    name: go-grpc
    version: 0.0.1
    publisher: codefly.dev
endpoints:
    - name: http
      api: http
service-dependencies:
    - interface: %s
`, requirement),
	}
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// caret is the range a consumer of the definition's line requires:
// codefly.dev/cache@^0.2 for 0.2.0.
func caret(definition *resources.Interface) string {
	major, rest, _ := strings.Cut(definition.Version, ".")
	minor, _, _ := strings.Cut(rest, ".")
	if major == "0" {
		return definition.Identity().Key() + "@^0." + minor
	}
	return definition.Identity().Key() + "@^" + major
}

// emitted is the configuration service-redis's CreateConnectionConfiguration
// returns (main.go, cacheConfiguration, on service-redis main at 52b0189): the
// redis group, and the cache group naming its driver with the same connection.
// A restricted deployment returns both connections as value-free secret
// references, connection "".
func emitted(connection string) *basev0.Configuration {
	instance := resources.NewNetworkInstance("127.0.0.1", 6379)
	instance.Access = resources.NewNativeNetworkAccess()
	return &basev0.Configuration{
		Origin:         "platform/redis",
		RuntimeContext: resources.RuntimeContextFromInstance(instance),
		Infos: []*basev0.ConfigurationInformation{
			{Name: "redis", ConfigurationValues: []*basev0.ConfigurationValue{
				{Key: "connection", Value: connection, Secret: true},
			}},
			{Name: "cache", ConfigurationValues: []*basev0.ConfigurationValue{
				{Key: "driver", Value: "redis"},
				{Key: "connection", Value: connection, Secret: true},
			}},
		},
	}
}

// cacheGroup returns the configuration with its cache group rewritten.
func cacheGroup(conf *basev0.Configuration, change func(values []*basev0.ConfigurationValue) []*basev0.ConfigurationValue) *basev0.Configuration {
	for _, info := range conf.Infos {
		if info.Name == "cache" {
			info.ConfigurationValues = change(info.ConfigurationValues)
		}
	}
	return conf
}

func TestServiceRedisConformsToThePublishedDefinition(t *testing.T) {
	definition := published(t)
	ctx := resources.WithInterfaceResolver(context.Background(), resolve)
	dir := providerWorkspace(t, definition, caret(definition))

	platform, err := resources.LoadModuleFromDir(ctx, filepath.Join(dir, "modules/platform"))
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.ValidateInterfaceConformance(ctx); err != nil {
		t.Fatalf("a module declaring service-redis as a provider of %s: %v", definition.Identity(), err)
	}
	const url = "redis://:p%40ss%20word@127.0.0.1:6379"
	for name, conf := range map[string]*basev0.Configuration{"run": emitted(url), "restricted": emitted("")} {
		if err := platform.ValidateProvidedConfiguration(ctx, "redis", conf); err != nil {
			t.Fatalf("%s: service-redis's emitted configuration does not conform: %v", name, err)
		}
	}

	// Each departure from the definition fails with core's own verdict.
	refused := func(problem string) string {
		return fmt.Sprintf(`platform/redis: interface %s: configuration group "cache" from "platform/redis" does not conform: %s`,
			definition.Identity(), problem)
	}
	for problem, conf := range map[string]*basev0.Configuration{
		refused(`required key "connection" is missing`): cacheGroup(emitted(url), func(v []*basev0.ConfigurationValue) []*basev0.ConfigurationValue {
			return v[:1]
		}),
		refused(`key "connection" must have secret=true`): cacheGroup(emitted(url), func(v []*basev0.ConfigurationValue) []*basev0.ConfigurationValue {
			v[1].Secret = false
			return v
		}),
		refused(`key "driver" must have secret=false`): cacheGroup(emitted(url), func(v []*basev0.ConfigurationValue) []*basev0.ConfigurationValue {
			v[0].Secret = true
			return v
		}),
		refused(`key "password" is not part of the interface`): cacheGroup(emitted(url), func(v []*basev0.ConfigurationValue) []*basev0.ConfigurationValue {
			return append(v, &basev0.ConfigurationValue{Key: "password", Value: "p@ss word", Secret: true})
		}),
		refused(`key "connection" is emitted more than once`): cacheGroup(emitted(url), func(v []*basev0.ConfigurationValue) []*basev0.ConfigurationValue {
			return append(v, &basev0.ConfigurationValue{Key: "connection", Value: url, Secret: true})
		}),
		fmt.Sprintf(`platform/redis: interface %s: configuration from "platform/redis" has no "cache" group`, definition.Identity()): func() *basev0.Configuration {
			conf := emitted(url)
			conf.Infos = conf.Infos[:1]
			return conf
		}(),
	} {
		err := platform.ValidateProvidedConfiguration(ctx, "redis", conf)
		if err == nil || err.Error() != problem {
			t.Errorf("ValidateProvidedConfiguration = %v\nwant %s", err, problem)
		}
	}
}

// A consumer requiring the definition's line binds to the one provider that
// declares it, and core checks that provider against the published definition
// while binding.
func TestConsumerBindsByInterface(t *testing.T) {
	definition := published(t)
	ctx := resources.WithInterfaceResolver(context.Background(), resolve)
	workspace, err := resources.LoadWorkspaceFromDir(ctx, providerWorkspace(t, definition, caret(definition)))
	if err != nil {
		t.Fatal(err)
	}
	web, err := workspace.LoadService(ctx, &resources.ServiceWithModule{Module: "apps", Name: "web"})
	if err != nil {
		t.Fatalf("binding %s: %v", caret(definition), err)
	}
	if dep := web.ServiceDependencies[0]; dep.Module != "platform" || dep.Name != "redis" {
		t.Fatalf("%s bound to %s/%s, want platform/redis", dep.Interface, dep.Module, dep.Name)
	}
}
