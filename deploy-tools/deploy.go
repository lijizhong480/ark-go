package deploy

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func ServiceName(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[0] == "module" {
			name := filepath.Base(parts[1])
			if !strings.HasSuffix(name, "-svr") {
				return "", fmt.Errorf("service module must end in -svr: %s", parts[1])
			}
			return strings.TrimSuffix(name, "-svr"), nil
		}
	}
	return "", errors.New("go.mod module directive missing")
}

// Build creates a static placement path compatible with Package and Start.
func Build(ctx context.Context, serviceDir string) (string, error) {
	name, err := ServiceName(serviceDir)
	if err != nil {
		return "", err
	}
	dist := filepath.Join(serviceDir, "dist")
	if err := os.MkdirAll(dist, 0755); err != nil {
		return "", err
	}
	binary := filepath.Join(dist, name)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/server")
	cmd.Dir = serviceDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build: %w: %s", err, output)
	}
	return binary, nil
}

func Package(ctx context.Context, serviceDir, archivePath string) error {
	name, err := ServiceName(serviceDir)
	if err != nil {
		return err
	}
	binary, err := Build(ctx, serviceDir)
	if err != nil {
		return err
	}
	if archivePath == "" {
		archivePath = filepath.Join(serviceDir, "dist", name+".tar.gz")
	}
	if err := os.MkdirAll(filepath.Dir(archivePath), 0755); err != nil {
		return err
	}
	f, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	if err := addFile(tw, binary, "bin/"+name, 0755); err != nil {
		return err
	}
	configDir := filepath.Join(serviceDir, "configs")
	if err := filepath.WalkDir(configDir, func(path string, entry os.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("non-regular config file: %s", path)
		}
		rel, err := filepath.Rel(serviceDir, path)
		if err != nil {
			return err
		}
		return addFile(tw, path, filepath.ToSlash(rel), 0644)
	}); err != nil {
		return err
	}
	if err := addBytes(tw, "start.sh", []byte(startScript(name)), 0755); err != nil {
		return err
	}
	if err := addBytes(tw, "stop.sh", []byte(stopScript(name)), 0755); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

func addFile(tw *tar.Writer, src, dest string, mode int64) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() {
		return fmt.Errorf("not a regular file: %s", src)
	}
	if err := tw.WriteHeader(&tar.Header{Name: dest, Mode: mode, Size: stat.Size(), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}

func addBytes(tw *tar.Writer, dest string, data []byte, mode int64) error {
	if err := tw.WriteHeader(&tar.Header{Name: dest, Mode: mode, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func startScript(name string) string {
	return fmt.Sprintf(`#!/bin/sh
set -eu
base_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$base_dir"
mkdir -p run logs
if [ -f run/%[1]s.pid ] && kill -0 "$(cat run/%[1]s.pid)" 2>/dev/null; then
  echo "%[1]s already running" >&2
  exit 1
fi
if [ -f configs/local.env ]; then
  set -a
  . ./configs/local.env
  set +a
fi
nohup ./bin/%[1]s >>logs/%[1]s.log 2>&1 &
echo $! >run/%[1]s.pid
`, name)
}

func stopScript(name string) string {
	return fmt.Sprintf(`#!/bin/sh
set -eu
base_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$base_dir"
pid_file=run/%[1]s.pid
[ -f "$pid_file" ] || exit 0
pid=$(cat "$pid_file")
kill -TERM "$pid" 2>/dev/null || true
i=0
while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 100 ]; do
  sleep 0.1
  i=$((i + 1))
done
if kill -0 "$pid" 2>/dev/null; then
  echo "%[1]s did not stop" >&2
  exit 1
fi
rm -f "$pid_file"
`, name)
}

type StartOptions struct {
	ServiceDir string
	Env        map[string]string
}

func Start(ctx context.Context, options StartOptions) (int, error) {
	name, err := ServiceName(options.ServiceDir)
	if err != nil {
		return 0, err
	}
	pidFile := filepath.Join(options.ServiceDir, "dist", name+".pid")
	if pid, err := readPID(pidFile); err == nil && processAlive(pid) {
		return 0, fmt.Errorf("%s already running as pid %d", name, pid)
	}
	binary, err := Build(ctx, options.ServiceDir)
	if err != nil {
		return 0, err
	}
	logFile, err := os.OpenFile(filepath.Join(options.ServiceDir, "dist", name+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	cmd := exec.Command(binary)
	cmd.Dir = options.ServiceDir
	cmd.Stdout, cmd.Stderr = logFile, logFile
	env := map[string]string{}
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			env[key] = value
		}
	}
	if fileEnv, err := readEnvFile(filepath.Join(options.ServiceDir, "configs", "local.env")); err == nil {
		for k, v := range fileEnv {
			if _, exists := env[k]; !exists {
				env[k] = v
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	for k, v := range options.Env {
		env[k] = v
	}
	cmd.Env = make([]string, 0, len(env))
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
		return 0, err
	}
	go func() { _ = cmd.Wait() }()
	return cmd.Process.Pid, nil
}

func Stop(ctx context.Context, serviceDir string) error {
	name, err := ServiceName(serviceDir)
	if err != nil {
		return err
	}
	pidFile := filepath.Join(serviceDir, "dist", name+".pid")
	pid, err := readPID(pidFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !processAlive(pid) {
		return os.Remove(pidFile)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !processAlive(pid) {
			return os.Remove(pidFile)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func readPID(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, errors.New("invalid pid file")
	}
	return pid, nil
}
func processAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func readEnvFile(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("invalid env line %d", i+1)
		}
		result[key] = value
	}
	return result, nil
}
