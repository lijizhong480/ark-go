package template

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedModulesBuildAndTest(t *testing.T) {
	out := filepath.Join(t.TempDir(), "order")
	if err := Generate(Options{Name: "order", ModulePrefix: "example.com/team", Output: out}); err != nil {
		t.Fatal(err)
	}
	for _, module := range []string{"order-api", "order-svr"} {
		cmd := exec.Command("go", "test", "./...")
		cmd.Dir = filepath.Join(out, module)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s tests: %v\n%s", module, err, output)
		}
	}
	cmd := exec.Command("go", "build", "./cmd/server")
	cmd.Dir = filepath.Join(out, "order-svr")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated server build: %v\n%s", err, output)
	}
	mod, err := os.ReadFile(filepath.Join(out, "order-svr", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mod), "replace ") {
		t.Fatal("generated module contains relative replace")
	}
	integration, err := os.ReadFile(filepath.Join(out, "order-svr", "framework-example.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(integration), `"github.com/lijizhong480/ark-go/core/app"`) || !strings.Contains(string(integration), `"github.com/lijizhong480/ark-go/web"`) {
		t.Fatal("generated integration sketch does not assemble core and web")
	}
}

func TestRejectsExistingOutput(t *testing.T) {
	root := t.TempDir()
	if err := Generate(Options{Name: "order", ModulePrefix: "example.com/team", Output: root}); err == nil {
		t.Fatal("existing output overwritten")
	}
}
