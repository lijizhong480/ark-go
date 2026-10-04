// Package httpclient provides outbound HTTP calls with ark request headers and
// Feign-compatible status and metric classification.
package httpclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lijizhong480/ark-go/core/requestctx"
	"github.com/lijizhong480/ark-go/core/response"
)

type Result string

const (
	Success     Result = "success"
	SystemError Result = "system_error"
)

type Metric struct {
	TargetService, Host, Method, API, TraceID string
	Status, Code                              int
	Result                                    Result
	Error                                     bool
	Duration                                  time.Duration
}
type MetricHook func(Metric)

// EndpointSelector supplies a concrete base URL for a logical service name.
// A Nacos or other registry adapter can implement this without a dependency here.
type EndpointSelector interface {
	Select(context.Context, string) (string, error)
}
type EndpointSelectorFunc func(context.Context, string) (string, error)

func (f EndpointSelectorFunc) Select(ctx context.Context, service string) (string, error) {
	return f(ctx, service)
}

// HeaderInjector lets a tracing library inject traceparent/tracestate/baggage.
type HeaderInjector func(context.Context, http.Header)

type Options struct {
	ServiceName                                        string
	Timeout                                            time.Duration
	DialTimeout                                        time.Duration
	IdleConnTimeout                                    time.Duration
	MaxIdleConns, MaxIdleConnsPerHost, MaxConnsPerHost int
	Transport                                          http.RoundTripper
	Metric                                             MetricHook
	Selector                                           EndpointSelector
	InjectHeaders                                      HeaderInjector
}
type Client struct {
	httpClient   *http.Client
	ownTransport *http.Transport
	serviceName  string
	metric       MetricHook
	selector     EndpointSelector
	inject       HeaderInjector
}

func New(options Options) (*Client, error) {
	if options.Timeout <= 0 {
		return nil, errors.New("httpclient: timeout must be positive")
	}
	transport := options.Transport
	var owned *http.Transport
	if transport == nil {
		dial := options.DialTimeout
		if dial <= 0 {
			dial = 5 * time.Second
		}
		idle := options.IdleConnTimeout
		if idle <= 0 {
			idle = 90 * time.Second
		}
		maxIdle := options.MaxIdleConns
		if maxIdle <= 0 {
			maxIdle = 100
		}
		perHost := options.MaxIdleConnsPerHost
		if perHost <= 0 {
			perHost = 10
		}
		owned = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: dial, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns: maxIdle, MaxIdleConnsPerHost: perHost, MaxConnsPerHost: options.MaxConnsPerHost,
			IdleConnTimeout: idle, TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second}
		transport = owned
	}
	return &Client{httpClient: &http.Client{Timeout: options.Timeout, Transport: transport}, ownTransport: owned, serviceName: options.ServiceName, metric: options.Metric, selector: options.Selector, inject: options.InjectHeaders}, nil
}
func (c *Client) CloseIdleConnections() {
	if c.ownTransport != nil {
		c.ownTransport.CloseIdleConnections()
	}
}
func (c *Client) HTTPClient() *http.Client { return c.httpClient }

type CallOptions struct {
	TargetService string
	API           string
	// IgnoreStatus implements IgnoreFeignException. When enabled, all HTTP
	// errors except ExcludeStatus are classified as metric success.
	IgnoreStatus  bool
	ExcludeStatus []int
	Retry         RetryPolicy
	headersReady  bool
}

// RetryPolicy is opt-in. Defaults to one attempt, matching the archetype's
// lack of configured retries. Non-idempotent methods need explicit permission.
type RetryPolicy struct {
	MaxAttempts        int
	StatusCodes        []int
	TransportErrors    bool
	AllowNonIdempotent bool
	Delay              time.Duration
}
type StatusError struct {
	Status      int
	Method, URL string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("httpclient: %s %s returned HTTP %d", e.Method, e.URL, e.Status)
}

// Do sends a cloned request with forwarded headers. Non-2xx HTTP responses
// return both response and *StatusError; callers must close response.Body.
// Ignoring a status affects metrics only, never error delivery.
func (c *Client) Do(req *http.Request, call CallOptions) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("httpclient: nil request")
	}
	outbound := req.Clone(req.Context())
	outbound.Header = req.Header.Clone()
	started := time.Now()
	if err := c.resolve(outbound, call.TargetService); err != nil {
		if c.metric != nil {
			c.metric(Metric{TargetService: call.TargetService, Method: outbound.Method, API: call.API, Code: int(response.InternalServerError), Result: SystemError, Error: true, Duration: time.Since(started)})
		}
		return nil, err
	}
	traceID := outbound.Header.Get("traceid")
	if !call.headersReady {
		traceID = forwardHeaders(outbound, c.serviceName)
		if c.inject != nil {
			c.inject(outbound.Context(), outbound.Header)
		}
	}
	res, err := c.sendWithRetry(outbound, call.Retry)
	metric := Metric{TargetService: call.TargetService, Host: outbound.URL.Host, Method: outbound.Method, API: call.API, TraceID: traceID, Duration: time.Since(started)}
	if res != nil {
		metric.Status = res.StatusCode
	}
	if err != nil {
		metric.Code = int(response.InternalServerError)
		metric.Result = SystemError
		metric.Error = true
	} else if res.StatusCode < 200 || res.StatusCode >= 300 {
		err = &StatusError{Status: res.StatusCode, Method: outbound.Method, URL: outbound.URL.String()}
		if ignored(res.StatusCode, call) {
			metric.Code = res.StatusCode + 100000
			metric.Result = Success
		} else {
			metric.Code = int(response.InternalServerError)
			metric.Result = SystemError
			metric.Error = true
		}
	} else {
		metric.Code = 0
		metric.Result = Success
	}
	if c.metric != nil {
		c.metric(metric)
	}
	return res, err
}

// DoJSON reads a bounded ServerResponse body and applies FeignMetricAspect's
// business code classification. It still returns the original response body.
func (c *Client) DoJSON(req *http.Request, call CallOptions, maxBody int64) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("httpclient: nil request")
	}
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	// DoJSON owns metric emission so business code is recorded only once.
	prepared := req.Clone(req.Context())
	prepared.Header = req.Header.Clone()
	traceID := forwardHeaders(prepared, c.serviceName)
	if c.inject != nil {
		c.inject(prepared.Context(), prepared.Header)
	}
	clone := *c
	clone.metric = nil
	call.headersReady = true
	started := time.Now()
	res, err := clone.Do(prepared, call)
	metric := Metric{TargetService: call.TargetService, Method: req.Method, API: call.API, Host: req.URL.Host, Status: 0, Duration: time.Since(started)}
	if res != nil {
		metric.Status = res.StatusCode
		metric.Host = res.Request.URL.Host
	}
	metric.TraceID = traceID
	if err != nil {
		var statusErr *StatusError
		if errors.As(err, &statusErr) && ignored(statusErr.Status, call) {
			metric.Code = statusErr.Status + 100000
			metric.Result = Success
		} else {
			metric.Code = int(response.InternalServerError)
			metric.Result = SystemError
			metric.Error = true
		}
	} else if res == nil {
		metric.Code = int(response.InternalServerError)
		metric.Result = SystemError
		metric.Error = true
	} else {
		body, readErr := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
		if readErr != nil {
			err = readErr
			metric.Code = int(response.InternalServerError)
			metric.Result = SystemError
			metric.Error = true
		} else {
			res.Body = &replayBody{Reader: io.MultiReader(bytes.NewReader(body), res.Body), closer: res.Body}
			var envelope struct {
				Code *response.Code `json:"code"`
			}
			if int64(len(body)) > maxBody || json.Unmarshal(body, &envelope) != nil || envelope.Code == nil {
				metric.Code = 999999
				metric.Result = SystemError
				metric.Error = true
			} else {
				metric.Code = int(*envelope.Code)
				metric.Result = classifyBusiness(*envelope.Code, req.URL.Path)
				metric.Error = *envelope.Code != 0
			}
		}
	}
	metric.Duration = time.Since(started)
	if c.metric != nil {
		c.metric(metric)
	}
	return res, err
}
func ignored(status int, call CallOptions) bool {
	if !call.IgnoreStatus {
		return false
	}
	excluded := call.ExcludeStatus
	if len(excluded) == 0 {
		excluded = []int{500, 501, 502, 503, 504}
	}
	for _, s := range excluded {
		if s == status {
			return false
		}
	}
	return true
}
func classifyBusiness(code response.Code, path string) Result {
	n := int(code)
	if n == 0 || n == 100001 || (path == "/api/v1/assets/transferAsset" && n == 80012) || (n%1000 >= 200 && n%1000 < 500) {
		return Success
	}
	return SystemError
}

type traceKey struct{}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceKey{}, traceID)
}
func forwardHeaders(req *http.Request, serviceName string) string {
	paras, ok := requestctx.From(req.Context())
	if !ok {
		paras = requestctx.CommonParas{}
	}
	h := req.Header
	setIfMissing := func(key, value string) {
		if value != "" && h.Get(key) == "" {
			h.Set(key, value)
		}
	}
	traceID := h.Get("traceid")
	if traceID == "" {
		if v, ok := req.Context().Value(traceKey{}).(string); ok {
			traceID = v
		}
	}
	if traceID == "" {
		traceID = paras.ClientTraceID
	}
	if traceID == "" {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err == nil {
			traceID = hex.EncodeToString(random)
		}
	}
	setIfMissing("traceid", traceID)
	pairs := map[string]string{"channel": paras.Channel, "app_version": paras.AppVersion, "device_id": paras.DeviceID, "antiDeviceId": paras.AntiDeviceID,
		"ab_deviceId": paras.ABDeviceID, "platformId": paras.PlatformID, "appId": paras.AppID, "x-router-tag": paras.RouterTag, "lang": paras.Lang,
		"country-code": paras.CountryCode, "ip": paras.IP, "x-amzn-waf-is-use-vpn": paras.VPNHeader, "mainAppId": paras.MainAppID,
		"appSiteId": paras.AppSiteID, "inner-gateway-account-name": paras.InnerAccountName, "inner-gateway-account-id": paras.InnerAccountID}
	for key, value := range pairs {
		setIfMissing(key, value)
	}
	setIfMissing("gateway_uid", strconv.FormatInt(paras.UID, 10))
	setIfMissing("login-type", strconv.Itoa(paras.LoginType))
	if paras.MainUID != -1 {
		setIfMissing("parent_uid", strconv.FormatInt(paras.MainUID, 10))
	}
	for _, part := range []struct{ key, value string }{{"country", paras.Country}, {"province", paras.Province}, {"city", paras.City}} {
		if part.value != "" {
			setIfMissing(part.key, percentEncode(part.value))
		}
	}
	setIfMissing("reqServer", serviceName)
	return traceID
}
func percentEncode(s string) string {
	var b strings.Builder
	for _, by := range []byte(s) {
		if by >= 'A' && by <= 'Z' || by >= 'a' && by <= 'z' || by >= '0' && by <= '9' || by == '-' || by == '_' || by == '.' || by == '~' {
			b.WriteByte(by)
		} else {
			fmt.Fprintf(&b, "%%%02X", by)
		}
	}
	return b.String()
}

type replayBody struct {
	io.Reader
	closer io.Closer
}

func (r *replayBody) Close() error { return r.closer.Close() }

func (c *Client) resolve(req *http.Request, service string) error {
	if req.URL.Scheme != "" || req.URL.Host != "" {
		return nil
	}
	if service == "" {
		return errors.New("httpclient: relative URL needs target service")
	}
	if c.selector == nil {
		return errors.New("httpclient: endpoint selector required for relative URL")
	}
	endpoint, err := c.selector.Select(req.Context(), service)
	if err != nil {
		return err
	}
	base, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return errors.New("httpclient: selected endpoint must be an absolute HTTP base URL")
	}
	req.URL.Scheme, req.URL.Host = base.Scheme, base.Host
	escapedPath := strings.TrimRight(base.EscapedPath(), "/") + "/" + strings.TrimLeft(req.URL.EscapedPath(), "/")
	req.URL.Path = strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(req.URL.Path, "/")
	req.URL.RawPath = escapedPath
	return nil
}

func retryableMethod(method string, allow bool) bool {
	if allow {
		return true
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}
func retryStatus(status int, codes []int) bool {
	for _, code := range codes {
		if status == code {
			return true
		}
	}
	return false
}

func (c *Client) sendWithRetry(req *http.Request, policy RetryPolicy) (*http.Response, error) {
	attempts := policy.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	if !retryableMethod(req.Method, policy.AllowNonIdempotent) {
		attempts = 1
	}
	if attempts > 1 && req.Body != nil && req.GetBody == nil && (policy.TransportErrors || len(policy.StatusCodes) > 0) {
		return nil, errors.New("httpclient: retry requires replayable request body")
	}
	var res *http.Response
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		current := req.Clone(req.Context())
		if attempt > 0 && req.Body != nil {
			if req.GetBody == nil {
				return res, errors.New("httpclient: retry requires replayable request body")
			}
			current.Body, err = req.GetBody()
			if err != nil {
				return res, err
			}
		}
		res, err = c.httpClient.Do(current)
		if attempt == attempts-1 || req.Context().Err() != nil {
			return res, err
		}
		retry := err != nil && policy.TransportErrors || err == nil && res != nil && retryStatus(res.StatusCode, policy.StatusCodes)
		if !retry {
			return res, err
		}
		if res != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
			_ = res.Body.Close()
		}
		if policy.Delay > 0 {
			timer := time.NewTimer(policy.Delay)
			select {
			case <-timer.C:
			case <-req.Context().Done():
				timer.Stop()
				return nil, req.Context().Err()
			}
		}
	}
	return res, err
}
