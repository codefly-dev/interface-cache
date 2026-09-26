package definition_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/codefly-dev/core/composition"
	"github.com/codefly-dev/core/resources"
)

// publishedDir holds every version of the definition ever published, one
// directory per version, oldest first once sorted. A version is added when it
// is published and never edited after.
const publishedDir = "published"

func history(t *testing.T) []*resources.Interface {
	t.Helper()
	entries, err := os.ReadDir(publishedDir)
	if err != nil {
		t.Fatal(err)
	}
	var definitions []*resources.Interface
	for _, entry := range entries {
		definition, err := resources.LoadInterfaceFromDir(context.Background(), filepath.Join(publishedDir, entry.Name()))
		if err != nil {
			t.Fatalf("published %s does not load with core: %v", entry.Name(), err)
		}
		if definition.Version != entry.Name() {
			t.Fatalf("published/%s holds version %s", entry.Name(), definition.Version)
		}
		definitions = append(definitions, definition)
	}
	slices.SortFunc(definitions, func(a, b *resources.Interface) int {
		return semver.MustParse(a.Version).Compare(semver.MustParse(b.Version))
	})
	if len(definitions) == 0 {
		t.Fatal("no published version")
	}
	return definitions
}

// Every published version evolves from the one before it as core computes it:
// a version that under-states its change is refused, whatever its author
// thought it was.
func TestEachPublishedVersionEvolvesFromThePrevious(t *testing.T) {
	versions := history(t)
	for i := 1; i < len(versions); i++ {
		evolution, err := resources.EvolveInterface(versions[i-1], versions[i])
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s %s -> %s: breaking %v, additive %v", evolution.Interface, evolution.Before, evolution.After, evolution.Breaking, evolution.Additive)
	}
}

// The file at the repository root is the newest published version, byte for
// byte, so what the history is checked against is what a tag serves. Bumping
// the version therefore means adding its directory, and the evolution check
// above runs on it.
func TestRootIsTheNewestPublishedVersion(t *testing.T) {
	versions := history(t)
	newest := versions[len(versions)-1].Version
	current, err := os.ReadFile(filepath.Join(root, resources.InterfaceConfigurationName))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := os.ReadFile(filepath.Join(publishedDir, newest, resources.InterfaceConfigurationName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(current, snapshot) {
		t.Fatalf("interface.codefly.yaml differs from published/%s/%s; a new version adds its own directory, a published one is never edited",
			newest, resources.InterfaceConfigurationName)
	}
	if got := published(t).Version; got != newest {
		t.Fatalf("interface.codefly.yaml is %s, the newest published version %s", got, newest)
	}
}

// Changes a consumer of the previous version cannot survive: a provider of
// that version no longer satisfies the next, or a consumer reads what is gone.
// The names they add are ones no definition declares.
var breakingChanges = map[string]func(*resources.Interface){
	"remove a key": func(i *resources.Interface) { i.Capability.Keys = i.Capability.Keys[1:] },
	"flip a secret": func(i *resources.Interface) {
		i.Capability.Keys[0].Secret = !i.Capability.Keys[0].Secret
	},
	"flip optional": func(i *resources.Interface) {
		i.Capability.Keys[0].Optional = !i.Capability.Keys[0].Optional
	},
	"rename the group": func(i *resources.Interface) { i.Capability.Configuration += "-renamed" },
	"add a required key": func(i *resources.Interface) {
		i.Capability.Keys = append(i.Capability.Keys, &resources.InterfaceCapabilityKey{Name: "added-by-test"})
	},
	"add a required secret": func(i *resources.Interface) {
		i.Capability.Keys = append(i.Capability.Keys, &resources.InterfaceCapabilityKey{Name: "added-by-test", Secret: true})
	},
}

// compatible is the next version inside ^version, the release a consumer
// pinned to version takes without asking: a minor from 1.0.0, a patch below
// it. breaking is the next version outside it.
func bumps(t *testing.T, version string) (compatible, breaking string) {
	t.Helper()
	v := semver.MustParse(version)
	if v.Major() > 0 {
		return v.IncMinor().String(), v.IncMajor().String()
	}
	if v.Minor() > 0 {
		return v.IncPatch().String(), v.IncMinor().String()
	}
	t.Fatalf("version %s: the evolution cases assume a published line of at least 0.1", version)
	return "", ""
}

func changed(t *testing.T, from *resources.Interface, version string, change func(*resources.Interface)) *resources.Interface {
	t.Helper()
	next, err := resources.LoadInterfaceFromDir(context.Background(), filepath.Join(publishedDir, from.Version))
	if err != nil {
		t.Fatal(err)
	}
	next.Version = version
	if change != nil {
		change(next)
	}
	if err := next.Validate(); err != nil {
		t.Fatalf("the changed definition is invalid, so it tests nothing: %v", err)
	}
	return next
}

// Against every published version, 0.1.0 first: a breaking change released
// as the compatible bump is refused with core's verdict, and the same change
// as a breaking bump is accepted.
func TestBreakingChangeCannotBeReleasedAsCompatible(t *testing.T) {
	for _, from := range history(t) {
		compatible, breaking := bumps(t, from.Version)
		for name, change := range breakingChanges {
			_, err := resources.EvolveInterface(from, changed(t, from, compatible, change))
			want := "interface " + from.Identity().Key() + ": " + compatible + " is not a breaking version of " + from.Version + ", but the surface breaks: "
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("%s: %s as %s = %v, want %q", from.Identity(), name, compatible, err, want)
			}
			if _, err := resources.EvolveInterface(from, changed(t, from, breaking, change)); err != nil {
				t.Errorf("%s: %s as %s: %v", from.Identity(), name, breaking, err)
			}
		}
	}
}

// An additive change (an optional key) needs at least the bump that adds to
// the line, never a patch of it.
func TestAdditiveChangeCannotBeAPatch(t *testing.T) {
	addOptional := func(i *resources.Interface) {
		i.Capability.Keys = append(i.Capability.Keys, &resources.InterfaceCapabilityKey{Name: "added-by-test", Optional: true})
	}
	for _, from := range history(t) {
		patch := semver.MustParse(from.Version).IncPatch().String()
		_, err := resources.EvolveInterface(from, changed(t, from, patch, addOptional))
		want := "interface " + from.Identity().Key() + ": " + patch + " only bumps the patch of " + from.Version + ", but the surface grows: added key added-by-test"
		if err == nil || err.Error() != want {
			t.Errorf("%s: an optional key as %s = %v, want %q", from.Identity(), patch, err, want)
		}
	}
}

// One identity names one definition. A tag vX.Y.Z is where the CLI's resolver
// fetches codefly.dev/cache@X.Y.Z, so every such tag must serve a file core
// resolves as exactly that identity, with the surface published/X.Y.Z records.
// core's module-update report is the judge: a version published again with a
// different surface blocks it.
func TestTagsServeTheirPublishedVersion(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	list, err := exec.Command("git", "-C", root, "tag", "--list", "v*").Output()
	if err != nil {
		t.Skipf("not a git checkout: %v", err)
	}
	tags := strings.Fields(string(list))
	if len(tags) == 0 {
		t.Log("no vX.Y.Z tag yet: nothing is served")
	}
	for _, tag := range tags {
		identity, err := resources.ParseInterfaceIdentity("codefly.dev/cache@" + strings.TrimPrefix(tag, "v"))
		if err != nil {
			t.Errorf("tag %s does not name a version: %v", tag, err)
			continue
		}
		fetch := func(ctx context.Context, identity *resources.InterfaceIdentity) (*resources.Interface, error) {
			content, err := exec.Command("git", "-C", root, "show", "v"+identity.Version+":"+resources.InterfaceConfigurationName).Output()
			if err != nil {
				return nil, errors.New("no interface.codefly.yaml at v" + identity.Version)
			}
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, resources.InterfaceConfigurationName), content, 0o600); err != nil {
				return nil, err
			}
			return resources.LoadInterfaceFromDir(ctx, dir)
		}
		served, err := resources.ResolveInterface(resources.WithInterfaceResolver(context.Background(), fetch), identity)
		if err != nil {
			t.Errorf("tag %s: %v", tag, err)
			continue
		}
		recorded, err := resources.LoadInterfaceFromDir(context.Background(), filepath.Join(publishedDir, identity.Version))
		if err != nil {
			t.Errorf("tag %s has no published/%s: %v", tag, identity.Version, err)
			continue
		}
		report := &composition.SemanticReport{}
		if err := report.AddInterfaceEvolutions([]*resources.Interface{served}, []*resources.Interface{recorded}); err != nil {
			t.Fatal(err)
		}
		if len(report.BlockedReasons) > 0 {
			t.Errorf("tag %s: %v", tag, report.BlockedReasons)
		}
	}
}
