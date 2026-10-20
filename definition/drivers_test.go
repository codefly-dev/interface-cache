package definition_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const cacheModule = "github.com/codefly-dev/interface-cache/go/cache"

// driver is a row of the README's Drivers table.
type driver struct {
	name, module string
	// builds is the table's claim about the newest published version: yes,
	// or no with the port named.
	builds bool
	claim  string
}

// drivers reads the README's Drivers table: driver, Go package, provider, and
// whether it builds against this go/cache. Core's format has no place for
// drivers, so the table is their only list.
func drivers(t *testing.T) []driver {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, section, found := strings.Cut(string(content), "\n### Drivers\n")
	if !found {
		t.Fatal("README.md has no Drivers section")
	}
	var rows []driver
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "|") {
			if len(rows) > 0 {
				break
			}
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		for i := range cells {
			cells[i] = strings.Trim(strings.TrimSpace(cells[i]), "`")
		}
		if cells[0] == "Driver" || strings.HasPrefix(cells[0], "-") {
			continue
		}
		if len(cells) != 4 {
			t.Fatalf("README.md Drivers row %q: want driver, Go package, provider, builds", line)
		}
		row := driver{name: cells[0], module: cells[1], claim: cells[3]}
		switch {
		case row.claim == "yes":
			row.builds = true
		case strings.HasPrefix(row.claim, "no (port: "):
		default:
			t.Fatalf("README.md Drivers row %s: the last column is %q, want yes or no (port: <link>)", row.name, row.claim)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		t.Fatal("README.md lists no driver")
	}
	return rows
}

// goIn runs go in dir, outside the development workspace, as a consumer does.
func goIn(dir string, args ...string) (string, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// newest downloads the newest published version of module, the one go get
// resolves for a consumer, and returns it with its directory in the module
// cache.
func newest(t *testing.T, module string) (version, dir string) {
	t.Helper()
	scratch := t.TempDir()
	if out, err := goIn(scratch, "mod", "init", "gate"); err != nil {
		t.Fatalf("go mod init: %v\n%s", err, out)
	}
	out, err := goIn(scratch, "mod", "download", "-json", module+"@latest")
	var m struct{ Version, Dir, Error string }
	if jsonErr := json.Unmarshal([]byte(out), &m); jsonErr != nil || err != nil || m.Error != "" {
		t.Fatalf("go mod download %s@latest: %v\n%s", module, err, out)
	}
	t.Logf("%s@%s is the newest published version", module, m.Version)
	return m.Version, m.Dir
}

// The drivers of this interface are modules of other repositories, each
// pinned to a tagged go/cache. A change to the Go contract here — Entry, a
// layer or origin interface — reaches a driver only when it moves, so neither
// repository's CI sees a driver that no longer builds; the consumer that
// moves first does, once the release is out (#21).
//
// This holds every listed driver, at the newest version a consumer gets, to
// the go/cache of this commit: a copy of the published module is pointed at
// ../go/cache and must tidy, build, vet and pass its own tests. It is the
// driver's module alone; its conformance run against the server its agent
// runs stays in its repository.
//
// The table's last column is where a deliberate break is acknowledged: yes
// requires the driver to build; no, naming the driver's port, requires it not
// to, so the row returns to yes once the port is published and a break never
// ships unannounced.
func TestPublishedDriversBuildAgainstThisCache(t *testing.T) {
	cacheDir, err := filepath.Abs(filepath.Join(root, "go", "cache"))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range drivers(t) {
		t.Run(d.name, func(t *testing.T) {
			version, published := newest(t, d.module)
			// The module cache is read-only; the copy takes the replace.
			dir := t.TempDir()
			if err := os.CopyFS(dir, os.DirFS(published)); err != nil {
				t.Fatal(err)
			}
			if out, err := goIn(dir, "mod", "edit", "-replace", cacheModule+"="+cacheDir); err != nil {
				t.Fatalf("go mod edit: %v\n%s", err, out)
			}
			var failure string
			for _, step := range [][]string{
				{"mod", "tidy"},
				{"build", "./..."},
				{"vet", "./..."},
				{"test", "-race", "-count=1", "./..."},
			} {
				if out, err := goIn(dir, step...); err != nil {
					failure = fmt.Sprintf("go %s: %v\n%s", strings.Join(step, " "), err, out)
					break
				}
			}
			at := d.module + "@" + version
			switch {
			case failure == "" && d.builds:
				t.Logf("%s builds, vets and passes its tests against the go/cache of this commit", at)
			case failure != "" && d.builds:
				t.Fatalf("%s, the newest published %s driver, does not build against the go/cache of this commit:\n%s\n"+
					"A consumer moving to this go/cache cannot open the driver until it is ported (#21). Keep the contract, "+
					"or acknowledge the break in README.md's Drivers table as `no (port: <link>)`, naming the driver's port, "+
					"and record it in the release notes.", at, d.name, failure)
			case failure == "" && !d.builds:
				t.Fatalf("README.md's Drivers table says the %s driver does not build against this go/cache (%q), but %s does: set the row back to yes",
					d.name, d.claim, at)
			default:
				t.Logf("%s does not build against the go/cache of this commit, as README.md acknowledges (%s):\n%s", at, d.claim, failure)
			}
		})
	}
}
