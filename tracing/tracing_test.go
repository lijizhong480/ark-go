package tracing

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestPropagationAndAttributes(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	sid, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	ctx = WithBusinessAttribute(ctx, "inner_uid", "42")
	ctx = WithExtraAttribute(ctx, "biz_scene", "home")
	ctx = WithExtraAttribute(ctx, "wrong", "ignored")
	ctx = WithCustomAttribute(ctx, "x", strings.Repeat("界", 50))
	a := AttributesFrom(ctx)
	if a.Business["inner_uid"] != "42" || a.Extra["biz_scene"] != "home" || len(a.Extra) != 1 || len(a.Custom["biz-x"]) > 128 {
		t.Fatalf("attributes: %#v", a)
	}
	h := http.Header{}
	Inject(ctx, h, nil)
	if !HasTracingInfo(h) || !strings.HasPrefix(h.Get("traceparent"), "00-0123456789abcdef") || h.Get("baggage") != "inner_uid=42" {
		t.Fatalf("headers: %v", h)
	}
	child := Extract(context.Background(), h, nil)
	got, _ := IDs(child)
	if got != tid.String() || len(AttributesFrom(child).Extra) != 0 {
		t.Fatalf("extracted %q", got)
	}
	a.Business["inner_uid"] = "bad"
	if AttributesFrom(ctx).Business["inner_uid"] != "42" {
		t.Fatal("mutable snapshot")
	}
}

func TestCustomLimit(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 11; i++ {
		ctx = WithCustomAttribute(ctx, string(rune('a'+i)), "v")
	}
	if got := len(AttributesFrom(ctx).Custom); got != 10 {
		t.Fatalf("got %d", got)
	}
}
