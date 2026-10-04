package redis

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
)

// Run only against a disposable Redis server. REDISSON_INTEROP_CP must contain
// Redisson 3.22.1 and its Java runtime dependencies.
func TestRedissonJavaInterop(t *testing.T) {
	address, jars := os.Getenv("REDISSON_INTEROP_ADDR"), os.Getenv("REDISSON_INTEROP_CP")
	if address == "" || jars == "" {
		t.Skip("set REDISSON_INTEROP_ADDR and REDISSON_INTEROP_CP for real Java interoperability test")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	javaOut := t.TempDir()
	compile := exec.Command("javac", "-cp", jars, "-d", javaOut, filepath.Join("testdata", "RedissonInterop.java"))
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile Java probe: %v\n%s", err, output)
	}
	classpath := javaOut + string(os.PathListSeparator) + jars
	runJava := func(mode string) (string, error) {
		cmd := exec.Command("java", "-cp", classpath, "RedissonInterop", mode, port)
		output, err := cmd.CombinedOutput()
		return string(output), err
	}
	ctx := context.Background()
	client := redisv8.NewClient(&redisv8.Options{Addr: address})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, "shared-lock").Err(); err != nil {
		t.Fatal(err)
	}
	defer client.Del(ctx, "shared-lock")

	goLock, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("Go acquire: %v %v", ok, err)
	}
	renewCtx, stopRenew := context.WithCancel(ctx)
	renewErrors, err := goLock.AutoRenew(renewCtx, 2*time.Second, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if out, err := runJava("try"); err != nil || !strings.Contains(out, "ACQUIRED=false") {
		t.Fatalf("Java blocked by Go: %v\n%s", err, out)
	}
	for _, mode := range []string{"read", "write"} {
		if out, err := runJava(mode); err != nil || !strings.Contains(out, "ACQUIRED=false") {
			t.Fatalf("Java %s blocked by Go: %v\n%s", mode, err, out)
		}
	}
	if released, err := goLock.Release(ctx); err != nil || !released {
		t.Fatalf("Go release: %v %v", released, err)
	}
	stopRenew()
	for err := range renewErrors {
		t.Fatalf("normal renewal stop: %v", err)
	}
	if out, err := runJava("try"); err != nil || !strings.Contains(out, "ACQUIRED=true") {
		t.Fatalf("Java acquires after Go release: %v\n%s", err, out)
	}
	for _, mode := range []string{"read", "write"} {
		if out, err := runJava(mode); err != nil || !strings.Contains(out, "ACQUIRED=true") {
			t.Fatalf("Java %s acquires after Go release: %v\n%s", mode, err, out)
		}
	}

	java := exec.Command("java", "-cp", classpath, "RedissonInterop", "hold", port, "4000")
	stdout, err := java.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	java.Stderr = &stderr
	if err := java.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case out := <-ready:
		if !strings.Contains(out, "ACQUIRED=true") {
			java.Process.Kill()
			t.Fatalf("Java hold failed: %s %s", out, stderr.String())
		}
	case <-time.After(8 * time.Second):
		java.Process.Kill()
		t.Fatal("Java hold did not start")
	}
	if _, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second); err != nil || ok {
		java.Process.Kill()
		t.Fatalf("Go blocked by Java: %v %v", ok, err)
	}
	if err := java.Wait(); err != nil {
		t.Fatalf("Java hold: %v\n%s", err, stderr.String())
	}
	goLock, ok, err = TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second)
	if err != nil || !ok {
		t.Fatalf("Go acquires after Java release: %v %v", ok, err)
	}
	goLock.Release(ctx)

	if err := client.Set(ctx, "shared-lock", "legacy-token", 10*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second); !errors.Is(err, ErrIncompatibleLockKey) {
		t.Fatalf("legacy key not rejected: %v", err)
	}
	if out, err := runJava("try"); err == nil || !strings.Contains(out, "WRONGTYPE") {
		t.Fatalf("Java legacy-key failure not observed: %v\n%s", err, out)
	}
}
