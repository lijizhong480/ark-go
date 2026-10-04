package deploy

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "order-svr")
	if err := os.MkdirAll(filepath.Join(dir, "cmd", "server"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "configs"), 0755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"go.mod":             "module example.com/team/order-svr\n\ngo 1.23\n",
		"cmd/server/main.go": "package main\nimport (\"net/http\";\"os\";\"os/signal\";\"context\";\"syscall\")\nfunc main(){s:=&http.Server{Addr:os.Getenv(\"HTTP_ADDR\"), Handler:http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){w.Write([]byte(\"ok\"))})};ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGTERM);defer stop();go s.ListenAndServe();<-ctx.Done();s.Shutdown(context.Background())}\n",
		"configs/local.env":  "HTTP_ADDR=127.0.0.1:0\n",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestBuildPackageAndLifecycle(t *testing.T) {
	dir := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	binary, err := Build(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "order.tar.gz")
	if err := Package(ctx, dir, archive); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	r := tar.NewReader(gz)
	seen := map[string]bool{}
	for {
		h, err := r.Next()
		if err != nil {
			break
		}
		seen[h.Name] = true
	}
	for _, name := range []string{"bin/order", "configs/local.env", "start.sh", "stop.sh"} {
		if !seen[name] {
			t.Fatalf("archive missing %s: %v", name, seen)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	pid, err := Start(ctx, StartOptions{ServiceDir: dir, Env: map[string]string{"HTTP_ADDR": addr}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = Stop(stopCtx, dir)
	}()
	if pid <= 0 {
		t.Fatalf("invalid pid %d", pid)
	}
	ready := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		resp, err := http.Get("http://" + addr + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
	}
	if !ready {
		t.Fatal("service did not become healthy")
	}
	if _, err := Start(ctx, StartOptions{ServiceDir: dir}); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("duplicate start = %v", err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := Stop(stopCtx, dir); err != nil {
		t.Fatal(err)
	}
}
