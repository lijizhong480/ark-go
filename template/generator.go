package template

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

//go:embed skeleton
var skeleton embed.FS

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
var modulePattern = regexp.MustCompile(`^[A-Za-z0-9._~/-]+$`)

type Options struct {
	Name         string
	ModulePrefix string
	Output       string
}

type values struct{ Name, Prefix, API, Server string }

// Generate creates independent API and service modules. A local go.work
// connects them for development, without relative replace directives.
func Generate(opts Options) error {
	if !namePattern.MatchString(opts.Name) {
		return fmt.Errorf("invalid service name %q", opts.Name)
	}
	prefix := strings.TrimSuffix(opts.ModulePrefix, "/")
	if !modulePattern.MatchString(prefix) || !strings.Contains(prefix, "/") {
		return fmt.Errorf("invalid module prefix %q", opts.ModulePrefix)
	}
	if opts.Output == "" {
		return errors.New("output directory required")
	}
	if _, err := os.Stat(opts.Output); err == nil {
		return fmt.Errorf("output already exists: %s", opts.Output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	v := values{Name: opts.Name, Prefix: prefix, API: prefix + "/" + opts.Name + "-api", Server: prefix + "/" + opts.Name + "-svr"}
	if err := os.MkdirAll(opts.Output, 0755); err != nil {
		return err
	}
	return fs.WalkDir(skeleton, "skeleton", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(path, "skeleton/")
		rel = strings.ReplaceAll(rel, "name_placeholder", opts.Name)
		if strings.HasSuffix(rel, ".tmpl") {
			rel = strings.TrimSuffix(rel, ".tmpl")
		}
		target := filepath.Join(opts.Output, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		content, err := skeleton.ReadFile(path)
		if err != nil {
			return err
		}
		t, err := template.New(path).Option("missingkey=error").Parse(string(content))
		if err != nil {
			return err
		}
		mode := fs.FileMode(0644)
		if strings.HasSuffix(target, ".sh") {
			mode = 0755
		}
		f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return err
		}
		if err := t.Execute(f, v); err != nil {
			f.Close()
			return err
		}
		return f.Close()
	})
}
