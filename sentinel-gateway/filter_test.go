package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeChecker struct {
	seen      []Resource
	blockName string
	done      []string
}

func (f *fakeChecker) Check(_ context.Context, resource Resource) (func(bool, time.Duration), BlockKind, error) {
	f.seen = append(f.seen, resource)
	if resource.Name == f.blockName {
		return nil, FlowBlock, nil
	}
	return func(success bool, _ time.Duration) {
		if success {
			f.done = append(f.done, resource.Name)
		}
	}, "", nil
}

func TestRouteAndPathOrderAndOrigin(t *testing.T) {
	checker := &fakeChecker{}
	filter := Filter{Checker: checker, Origin: func(*http.Request) string { return "mobile" }}
	request := httptest.NewRequest("GET", "/orders", nil)
	request = request.WithContext(WithRouteID(request.Context(), "order-route"))
	w := httptest.NewRecorder()
	filter.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })).ServeHTTP(w, request)
	if w.Code != 204 {
		t.Fatalf("status = %d", w.Code)
	}
	if len(checker.seen) != 2 || checker.seen[0].Name != "/orders" || checker.seen[0].Type != GatewayPath || checker.seen[1].Name != "order-route" || checker.seen[1].Type != GatewayRoute || checker.seen[1].Origin != "mobile" {
		t.Fatalf("resources = %+v", checker.seen)
	}
	if !reflect.DeepEqual(checker.done, []string{"order-route", "/orders"}) {
		t.Fatalf("release order = %v", checker.done)
	}
}

func TestBlockAndApolloDisabledBypass(t *testing.T) {
	checker := &fakeChecker{blockName: "order-route"}
	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	request := httptest.NewRequest("GET", "/orders", nil)
	request = request.WithContext(WithRouteID(request.Context(), "order-route"))
	w := httptest.NewRecorder()
	Filter{Checker: checker}.Wrap(next).ServeHTTP(w, request)
	if called || w.Code != 200 || !strings.Contains(w.Body.String(), `"code":100410`) {
		t.Fatalf("fallback = %d %s called=%v", w.Code, w.Body.String(), called)
	}
	checker.seen = nil
	called = false
	w = httptest.NewRecorder()
	Filter{Checker: checker, Enabled: func(context.Context) bool { return false }}.Wrap(next).ServeHTTP(w, request)
	if !called || len(checker.seen) != 0 {
		t.Fatalf("disabled filter still checked rules: %v", checker.seen)
	}
}

func TestGatewayParamParserStrategiesAndDefault(t *testing.T) {
	request := httptest.NewRequest("GET", "http://example.test/orders?region=apac&region=eu", nil)
	request.RemoteAddr = "192.0.2.10:4321"
	request.Header.Set("X-Tenant", "vip-gold")
	request.Header.Set("Cookie", "sid=abc123")
	rules := []GatewayRule{
		{Resource: "orders", ParamItem: &GatewayParamItem{ParseStrategy: ParamClientIP, Pattern: "192.0.2.10", MatchStrategy: MatchExact}},
		{Resource: "orders", ParamItem: &GatewayParamItem{ParseStrategy: ParamHost, Pattern: "example", MatchStrategy: MatchPrefix}},
		{Resource: "orders", ParamItem: &GatewayParamItem{ParseStrategy: ParamHeader, FieldName: "X-Tenant", Pattern: "gold", MatchStrategy: MatchContains}},
		{Resource: "orders", ParamItem: &GatewayParamItem{ParseStrategy: ParamURL, FieldName: "region", Pattern: "ap.*", MatchStrategy: MatchRegex}},
		{Resource: "orders", ParamItem: &GatewayParamItem{ParseStrategy: ParamCookie, FieldName: "sid", Pattern: "wrong", MatchStrategy: MatchExact}},
		{Resource: "orders"},
	}
	params, err := ParseRouteParams("orders", request, rules)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"192.0.2.10", "example.test", "vip-gold", "apac", GatewayNotMatched, GatewayDefaultParam}
	if !reflect.DeepEqual(params, want) {
		t.Fatalf("params=%v want=%v", params, want)
	}
	request.Header.Del("X-Tenant")
	params, err = ParseRouteParams("orders", request, rules)
	if err != nil || params[2] != nil {
		t.Fatalf("missing header should be nil: %v %v", params, err)
	}
	rules[0].ResourceMode = 1
	params, err = ParseRouteParams("orders", request, rules)
	if err != nil || len(params) != 0 {
		t.Fatalf("mixed resource mode should yield no params: %v %v", params, err)
	}
}

func TestGatewayFilterPassesParsedParams(t *testing.T) {
	checker := &fakeChecker{}
	filter := Filter{Checker: checker, Rules: func(string) []GatewayRule {
		return []GatewayRule{{Resource: "orders", ParamItem: &GatewayParamItem{ParseStrategy: ParamHeader, FieldName: "X-Tenant"}}}
	}}
	request := httptest.NewRequest("GET", "/orders", nil)
	request.Header.Set("X-Tenant", "tenant-a")
	request = request.WithContext(WithRouteID(request.Context(), "orders"))
	filter.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })).ServeHTTP(httptest.NewRecorder(), request)
	if len(checker.seen) != 2 || len(checker.seen[0].Params) != 0 || !reflect.DeepEqual(checker.seen[1].Params, []any{"tenant-a"}) {
		t.Fatalf("resource params = %+v", checker.seen)
	}
}
