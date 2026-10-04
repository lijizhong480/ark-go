package xxl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	hessian "github.com/apache/dubbo-go-hessian2"
)

func javaVectors(t *testing.T) map[string][]byte {
	t.Helper()
	file, err := os.Open("testdata/java-vectors.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	vectors := map[string][]byte{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		name, raw, ok := strings.Cut(scanner.Text(), "=")
		if !ok {
			t.Fatal("malformed vector")
		}
		data, err := hex.DecodeString(raw)
		if err != nil {
			t.Fatal(err)
		}
		vectors[name] = data
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return vectors
}
func TestJavaHessianVectors(t *testing.T) {
	vectors := javaVectors(t)
	for _, name := range []string{"beat", "run"} {
		decoded, err := hessian.NewDecoder(vectors[name]).Decode()
		if err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		request, ok := decoded.(map[string]any)
		if !ok || request["_class"] != rpcRequestClass || request["methodName"] != name {
			t.Fatalf("%s request: %#v", name, decoded)
		}
	}
	encoded := hessian.NewEncoder()
	if err := encoded.Encode(&wireResponse{RequestID: "req-beat", Result: &wireReturn{Code: 200}}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded.Buffer(), vectors["response"]) {
		t.Fatalf("Go response differs from Java vector\nGo: %x\nJava: %x", encoded.Buffer(), vectors["response"])
	}
}
func TestRPCHandlerAgainstJavaRequests(t *testing.T) {
	vectors := javaVectors(t)
	done := make(chan CallbackParam, 1)
	executor := NewExecutor(nil, func(_ context.Context, items []CallbackParam) error { done <- items[0]; return nil })
	if err := executor.Register("demo", func(ctx context.Context, param string) ReturnT[string] {
		if param != "hello" {
			return failure[string](fmt.Sprintf("unexpected param %s", param))
		}
		return success("")
	}, Lifecycle{}); err != nil {
		t.Fatal(err)
	}
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop(context.Background())
	handler := executor.rpcHandler("secret", func() time.Time { return time.UnixMilli(1700000000000) })
	for _, name := range []string{"beat", "run"} {
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(vectors[name]))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != 200 {
			t.Fatalf("%s HTTP status %d", name, recorder.Code)
		}
		decoded, err := hessian.NewDecoder(recorder.Body.Bytes()).Decode()
		if err != nil {
			t.Fatalf("%s response: %v", name, err)
		}
		reply, ok := decoded.(*wireResponse)
		if !ok || reply.ErrorMsg != nil {
			t.Fatalf("%s reply: %#v", name, decoded)
		}
		result, ok := reply.Result.(*wireReturn)
		if !ok || result.Code != 200 {
			t.Fatalf("%s result: %#v", name, reply.Result)
		}
	}
	select {
	case callback := <-done:
		if callback.LogID != 123 || callback.ExecuteResult.Code != 200 {
			t.Fatalf("callback: %+v", callback)
		}
	case <-time.After(time.Second):
		t.Fatal("no callback")
	}
}
func TestRPCSerializedTokenAndTimestamp(t *testing.T) {
	vector := javaVectors(t)["beat"]
	executor := NewExecutor(nil, nil)
	for _, tc := range []struct {
		name, token string
		now         time.Time
	}{
		{"token", "wrong", time.UnixMilli(1700000000000)},
		{"stale", "secret", time.UnixMilli(1700000200000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := executor.rpcHandler(tc.token, func() time.Time { return tc.now })
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest("POST", "/", bytes.NewReader(vector)))
			decoded, err := hessian.NewDecoder(rec.Body.Bytes()).Decode()
			if err != nil {
				t.Fatal(err)
			}
			reply := decoded.(*wireResponse)
			if reply.ErrorMsg == nil {
				t.Fatalf("expected error: %#v", reply)
			}
		})
	}
}
