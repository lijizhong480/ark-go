package web

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lijizhong480/ark-go/core/requestctx"
	"github.com/lijizhong480/ark-go/core/response"
	"github.com/golang-jwt/jwt/v4"
)

type listSource map[string][]string

func (s listSource) List(key, delimiter string, fallback []string) ([]string, error) {
	return s[key], nil
}
func TestRequestContextAndBanRules(t *testing.T) {
	keys := []string{BanUID, BanDID, BanIP, BanURL, BanUIDURL, BanDIDURL, BanIPURL}
	for _, kind := range keys {
		t.Run(kind, func(t *testing.T) {
			source := listSource{kind: []string{map[string]string{BanUID: "42", BanDID: "did", BanIP: "1.2.3.4", BanURL: "/order", BanUIDURL: "42_/order", BanDIDURL: "did_/order", BanIPURL: "1.2.3.4_/order"}[kind]}}
			filter, err := NewBanFilter(source, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer filter.Close()
			hit := false
			filter.Metric = func(gotKind, uri string) {
				hit = true
				if gotKind != kind || uri != "/order" {
					t.Fatalf("metric: %s %s", gotKind, uri)
				}
			}
			called := false
			handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }), RequestContext, filter.Middleware)
			request := httptest.NewRequest("GET", "/order", nil)
			request.Header.Set("gateway_uid", "42")
			request.Header.Set("device_id", "did")
			request.Header.Set("ip", "1.2.3.4")
			responseRecorder := httptest.NewRecorder()
			handler.ServeHTTP(responseRecorder, request)
			if called || !hit || responseRecorder.Code != 200 {
				t.Fatalf("ban failed: called=%v metric=%v status=%d", called, hit, responseRecorder.Code)
			}
			var body response.Response[any]
			if err := json.Unmarshal(responseRecorder.Body.Bytes(), &body); err != nil || body.Code != response.BlackBan {
				t.Fatalf("ban response: %+v %v", body, err)
			}
		})
	}
}
func TestBanRefresh(t *testing.T) {
	source := listSource{BanURL: {"/old"}}
	filter, err := NewBanFilter(source, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer filter.Close()
	source[BanURL] = []string{"/new"}
	if err := filter.Refresh(); err != nil {
		t.Fatal(err)
	}
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), RequestContext, filter.Middleware)
	old := httptest.NewRecorder()
	handler.ServeHTTP(old, httptest.NewRequest("GET", "/old", nil))
	if old.Code != 204 {
		t.Fatal(old.Code)
	}
	newer := httptest.NewRecorder()
	handler.ServeHTTP(newer, httptest.NewRequest("GET", "/new", nil))
	if newer.Code != 200 {
		t.Fatal(newer.Code)
	}
}
func TestBanConcurrentClose(t *testing.T) {
	filter, err := NewBanFilter(listSource{}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() { defer group.Done(); filter.Close() }()
	}
	group.Wait()
}
func TestGatewayJWT(t *testing.T) {
	secret := []byte("a sufficiently long HMAC secret")
	middleware, err := InnerGateway(GatewayOptions{Secret: base64.StdEncoding.EncodeToString(secret), Ignore: []string{"/manage/health"}})
	if err != nil {
		t.Fatal(err)
	}
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, path := range []string{"/manage/health", "/public"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != 204 {
			t.Fatalf("bypass %s: %d", path, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/manage/action", nil))
	if rec.Code != 401 {
		t.Fatal(rec.Code)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(time.Minute).Unix()})
	signed, err := token.SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/manage/action", nil)
	request.Header.Set("Inner-Authentication", signed)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request)
	if rec.Code != 204 {
		t.Fatal(rec.Code)
	}
}
func TestAccessRedactionAndHooks(t *testing.T) {
	var got AccessRecord
	called := false
	access := NewAccessFilter(AccessOptions{Patterns: []string{"/internal/*"}, RecordGatewayBody: true,
		Desensitize: DesensitizeConfig{Mode: WhitelistMode, WhiteURLs: []string{"/internal/order"}, Rules: []SensitiveRule{{Path: "/internal/order", FilterField: []string{"token"}}}},
		Record:      func(record AccessRecord) { called = true; got = record }, Metric: func(record AccessRecord) {
			if record.URI != "/internal/order" {
				t.Fatal(record.URI)
			}
		}})
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"code":0,"token":"abcdefgh"}`)) }), RequestContext, access.Middleware)
	request := httptest.NewRequest("POST", "/internal/order?token=secret&other=ok", strings.NewReader(`{"token":"abcdefgh"}`))
	request.Header.Set("gateway_uid", "42")
	request.Header.Set("reqServer", "caller")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request)
	if !called || rec.Code != 200 || got.Code != 0 || got.Context.UID != 42 || got.SourceService != "caller" {
		t.Fatalf("record: %+v", got)
	}
	if strings.Contains(got.Query, "secret") || strings.Contains(got.Request, "abcdefgh") || strings.Contains(got.Response, "abcdefgh") {
		t.Fatalf("secret leaked: %+v", got)
	}
	if !strings.Contains(got.Query, "token=****") {
		t.Fatalf("query: %s", got.Query)
	}
}
func TestAccessGatewayBodyDefault(t *testing.T) {
	var got AccessRecord
	access := NewAccessFilter(AccessOptions{Record: func(record AccessRecord) { got = record }})
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"code":0}`)) }), RequestContext, access.Middleware)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/order", nil))
	if got.Request != "-1" || got.Response != "-1" {
		t.Fatalf("gateway body logged: %+v", got)
	}
}
func TestCityHash64JavaVectors(t *testing.T) {
	// Generated with Hutool 5.7.14 HashUtil.cityHash64, ByteUtil.longToBytes,
	// and HexUtil.encodeHexStr on Java 17 with explicit UTF-8 input bytes.
	vectors := []struct{ input, want string }{
		{"abc", "69f3e774a0b3a524"},
		{"abcdefgh", "0929c89029d2e9fe"},
		{"abcdefghijklmnopqrstuvwxyz012345", "1af460b248d5f70c"},
		{"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMN", "f1cc76e9363366d0"},
		{"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", "147a0b9238c27a0b"},
		{"密码测试中文12345678", "7238cef38c8ce050"},
	}
	for _, vector := range vectors {
		if got := city64ToHex(vector.input); got != vector.want {
			t.Errorf("city64ToHex(%q) = %q, want %q", vector.input, got, vector.want)
		}
	}
	if got := city64ToHex(" \t"); got != " \t" {
		t.Fatalf("blank: %q", got)
	}
}
func TestAccessHashRedactionAndRuleSnapshot(t *testing.T) {
	rules := DesensitizeConfig{Rules: []SensitiveRule{{Path: "/internal/order", FilterField: []string{"token"}, IsHash: true}}}
	var got AccessRecord
	filter := NewAccessFilter(AccessOptions{Patterns: []string{"/internal/*"}, RecordGatewayBody: true, Desensitize: rules, Record: func(record AccessRecord) { got = record }})
	rules.Rules[0].FilterField[0] = "other"
	rules.Rules[0].Path = "/wrong"
	handler := filter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"code":0,"token":"abcdefgh"}`))
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/internal/order", strings.NewReader(`{"token":"abcdefgh"}`)))
	want := `{"token":"ab` + city64ToHex("cdef") + `gh"}`
	if got.Request != want {
		t.Fatalf("request hash: %q", got.Request)
	}
	if got.Response != `{"code":0,"token":"ab`+city64ToHex("cdef")+`gh"}` {
		t.Fatalf("response hash: %q", got.Response)
	}
	filter.UpdateRules(DesensitizeConfig{Rules: []SensitiveRule{{Path: "/internal/order", FilterField: []string{"token"}, IsHash: true}}}, nil)
	// Mutations after UpdateRules return must not alter the installed snapshot.
	cfg := DesensitizeConfig{Rules: []SensitiveRule{{Path: "/internal/order", FilterField: []string{"token"}, IsHash: true}}}
	filter.UpdateRules(cfg, nil)
	cfg.Rules[0].Path = "/wrong"
	cfg.Rules[0].FilterField[0] = "other"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/internal/order", strings.NewReader(`{"token":"abcdefgh"}`)))
	if got.Request != want {
		t.Fatalf("updated snapshot: %q", got.Request)
	}
}

func TestAccessCallerMutatesOldRulesConcurrently(t *testing.T) {
	config := DesensitizeConfig{Rules: []SensitiveRule{{Path: "/internal/order", FilterField: []string{"token"}}}}
	filter := NewAccessFilter(AccessOptions{Patterns: []string{"/internal/*"}, RecordGatewayBody: true, Desensitize: config})
	filter.UpdateRules(config, nil)
	handler := filter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"code":0,"token":"abcdefgh"}`))
	}))
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for i := 0; i < 100; i++ {
			config.Rules[0].FilterField[0] = "other"
			config.Rules[0].FilterField[0] = "token"
		}
	}()
	go func() {
		defer group.Done()
		for i := 0; i < 100; i++ {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/internal/order", strings.NewReader(`{"token":"abcdefgh"}`)))
		}
	}()
	group.Wait()
}
func TestContextAndHealth(t *testing.T) {
	handler := RequestContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := requestctx.From(r.Context())
		if !ok || p.UID != 7 {
			t.Fatalf("context: %+v %v", p, ok)
		}
	}))
	request := httptest.NewRequest("GET", "/x", nil)
	request.Header.Set("gateway_uid", "7")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	rec := httptest.NewRecorder()
	HealthHandler(func() bool { return false }).ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 503 {
		t.Fatal(rec.Code)
	}
}

type stringSource map[string]string

func (s stringSource) String(key, fallback string) string {
	if v, ok := s[key]; ok {
		return v
	}
	return fallback
}
func TestLoadDesensitizeConfig(t *testing.T) {
	source := stringSource{"app.desensitize.mode": "WHITELIST_MODE", "app.desensitize.white-list": `["/internal/order"]`, "app.desensitize.sensitive-field-list": `[{"path":"/internal/order","filterField":["token"]}]`, "permit.urls": " /internal/order , /internal/other "}
	cfg, err := LoadDesensitizeConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != WhitelistMode || len(cfg.Rules) != 1 || len(cfg.WhiteURLs) != 1 {
		t.Fatalf("config: %+v", cfg)
	}
	urls := LoadPermitURLs(source)
	if len(urls) != 2 || urls[0] != "/internal/order" {
		t.Fatalf("permit URLs: %v", urls)
	}
}
