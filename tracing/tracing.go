// Package tracing carries OpenTelemetry spans and bounded business metadata
// through Go contexts. It replaces Java ThreadLocal state with explicit context.
package tracing

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type attrsKey struct{}

// Attributes separates propagated business fields from local biz fields.
type Attributes struct {
	Business map[string]string
	Extra    map[string]string
	Custom   map[string]string
}

func AttributesFrom(ctx context.Context) Attributes {
	if ctx == nil {
		return Attributes{Business: map[string]string{}, Extra: map[string]string{}, Custom: map[string]string{}}
	}
	v, _ := ctx.Value(attrsKey{}).(Attributes)
	return Attributes{Business: clone(v.Business), Extra: clone(v.Extra), Custom: clone(v.Custom)}
}

func clone(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// WithBusinessAttribute records a span attribute and propagates it as baggage.
func WithBusinessAttribute(ctx context.Context, key, value string) context.Context {
	if ctx == nil || key == "" || value == "" {
		return ctx
	}
	a := AttributesFrom(ctx)
	a.Business[key] = value
	trace.SpanFromContext(ctx).SetAttributes(attribute.String(key, value))
	if member, err := baggage.NewMember(key, value); err == nil {
		b := baggage.FromContext(ctx)
		if b2, err := b.SetMember(member); err == nil {
			ctx = baggage.ContextWithBaggage(ctx, b2)
		}
	}
	return context.WithValue(ctx, attrsKey{}, a)
}

// WithExtraAttribute records local-only biz attributes on the current span.
func WithExtraAttribute(ctx context.Context, key, value string) context.Context {
	if ctx == nil || !strings.HasPrefix(key, "biz") || value == "" {
		return ctx
	}
	a := AttributesFrom(ctx)
	a.Extra[key] = value
	trace.SpanFromContext(ctx).SetAttributes(attribute.String(key, value))
	return context.WithValue(ctx, attrsKey{}, a)
}

// WithCustomAttribute stores up to 10 biz- prefixed fields for local logging.
// Values longer than 128 bytes are truncated at a UTF-8 boundary.
func WithCustomAttribute(ctx context.Context, key, value string) context.Context {
	if ctx == nil || key == "" || value == "" {
		return ctx
	}
	if !strings.HasPrefix(key, "biz-") {
		key = "biz-" + key
	}
	a := AttributesFrom(ctx)
	if len(a.Custom) >= 10 {
		if _, exists := a.Custom[key]; !exists {
			return ctx
		}
	}
	if len(value) > 128 {
		v := value[:125]
		for !utf8.ValidString(v) {
			v = v[:len(v)-1]
		}
		value = v + "..."
	}
	a.Custom[key] = value
	return context.WithValue(ctx, attrsKey{}, a)
}

// Propagator defaults to W3C tracecontext and baggage, matching the standard
// OpenTelemetry agent configuration; callers may inject their own propagator.
func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

func Extract(ctx context.Context, header http.Header, p propagation.TextMapPropagator) context.Context {
	if p == nil {
		p = Propagator()
	}
	return p.Extract(ctx, propagation.HeaderCarrier(header))
}

func Inject(ctx context.Context, header http.Header, p propagation.TextMapPropagator) {
	if p == nil {
		p = Propagator()
	}
	p.Inject(ctx, propagation.HeaderCarrier(header))
}

func HasTracingInfo(header http.Header) bool {
	return header.Get("traceparent") != "" || header.Get("tracestate") != "" || header.Get("X-Trace-Id") != "" || header.Get("X-Request-ID") != ""
}

func IDs(ctx context.Context) (traceID, spanID string) {
	if ctx == nil {
		return "", ""
	}
	s := trace.SpanContextFromContext(ctx)
	if !s.IsValid() {
		return "", ""
	}
	return s.TraceID().String(), s.SpanID().String()
}

// TraceLogID preserves an existing logging ID or supplies the OTel trace ID.
func TraceLogID(ctx context.Context, existing string) string {
	if existing != "" {
		return strings.TrimPrefix(existing, "OTEL_")
	}
	id, _ := IDs(ctx)
	return id
}

// HTTPMiddleware extracts the parent context and creates a server span.
func HTTPMiddleware(next http.Handler, tracer trace.Tracer, p propagation.TextMapPropagator) http.Handler {
	if tracer == nil {
		tracer = otel.Tracer("infra-user-rule/biz")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := Extract(r.Context(), r.Header, p)
		ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
