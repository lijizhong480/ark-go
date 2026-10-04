package web

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/lijizhong480/ark-go/core/requestctx"
	"github.com/go-faster/city"
)

type SensitiveRule struct {
	Path        string   `json:"path"`
	FilterField []string `json:"filterField"`
	IsHash      bool     `json:"isHash"`
}
type DesensitizeMode string

const (
	BlacklistMode DesensitizeMode = "BLACKLIST_MODE"
	WhitelistMode DesensitizeMode = "WHITELIST_MODE"
)

type DesensitizeConfig struct {
	Mode      DesensitizeMode
	BlackURLs []string
	WhiteURLs []string
	Rules     []SensitiveRule
}

// AccessRecord carries the Java AccessMetrics fields most useful to logs and
// metrics exporters. Request and Response contain only bounded, redacted data.
type AccessRecord struct {
	URI, Method, Query, Request, Response       string
	Status, Code, RequestLength, ResponseLength int
	Duration                                    time.Duration
	TraceID, SourceService                      string
	Context                                     requestctx.CommonParas
}
type AccessOptions struct {
	Patterns          []string
	Ignore            []string
	SensitiveLogURLs  []string
	PermitOnly        bool
	PermitURLs        []string
	RecordGatewayBody bool
	MaxBodyLength     int
	Desensitize       DesensitizeConfig
	Record            func(AccessRecord)
	Metric            func(AccessRecord)
}
type AccessFilter struct {
	mu      sync.RWMutex
	options AccessOptions
}

func NewAccessFilter(options AccessOptions) *AccessFilter {
	if len(options.Patterns) == 0 {
		options.Patterns = []string{"/api/*", "/internal/*"}
	}
	if options.MaxBodyLength <= 0 {
		options.MaxBodyLength = 10000
	}
	return &AccessFilter{options: cloneAccessOptions(options)}
}

func cloneAccessOptions(o AccessOptions) AccessOptions {
	o.Patterns = append([]string(nil), o.Patterns...)
	o.Ignore = append([]string(nil), o.Ignore...)
	o.SensitiveLogURLs = append([]string(nil), o.SensitiveLogURLs...)
	o.PermitURLs = append([]string(nil), o.PermitURLs...)
	o.Desensitize = cloneDesensitize(o.Desensitize)
	return o
}

func cloneDesensitize(c DesensitizeConfig) DesensitizeConfig {
	c.BlackURLs = append([]string(nil), c.BlackURLs...)
	c.WhiteURLs = append([]string(nil), c.WhiteURLs...)
	c.Rules = append([]SensitiveRule(nil), c.Rules...)
	for i := range c.Rules {
		c.Rules[i].FilterField = append([]string(nil), c.Rules[i].FilterField...)
	}
	return c
}

// UpdateRules installs a new immutable snapshot for dynamic Apollo rules.
func (f *AccessFilter) UpdateRules(config DesensitizeConfig, permitURLs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.options.Desensitize = cloneDesensitize(config)
	f.options.PermitURLs = append([]string(nil), permitURLs...)
}
func (f *AccessFilter) snapshot() AccessOptions { f.mu.RLock(); defer f.mu.RUnlock(); return f.options }
func (f *AccessFilter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o := f.snapshot()
		uri := r.URL.Path
		if !matchAny(o.Patterns, uri) || isMedia(uri) {
			next.ServeHTTP(w, r)
			return
		}
		max := o.MaxBodyLength
		requestCapture := &captureReader{ReadCloser: r.Body, max: max}
		request := r.Clone(r.Context())
		request.Body = requestCapture
		writer := &captureWriter{ResponseWriter: w, status: 200, max: max}
		start := time.Now()
		next.ServeHTTP(writer, request)
		if matchAny(o.Ignore, uri) {
			return
		}
		paras, ok := requestctx.From(r.Context())
		if !ok {
			paras = requestctx.FromRequest(r)
		}
		rule := findRule(o.Desensitize.Rules, uri)
		record := AccessRecord{URI: uri, Method: r.Method, Query: redactQuery(r.URL.RawQuery, rule.FilterField), Status: writer.status,
			RequestLength: requestCapture.total, ResponseLength: writer.total, Duration: time.Since(start),
			TraceID: paras.ClientTraceID, SourceService: paras.ReqServer, Context: paras}
		record.Code = parseResponseCode(uri, r.Method, writer.body.Bytes())
		if allowBody(o, uri) {
			if !matchAny(o.SensitiveLogURLs, uri) {
				record.Request = redactBody(requestCapture.body.Bytes(), rule, max)
			} else {
				record.Request = "-1"
			}
			record.Response = redactBody(writer.body.Bytes(), rule, max)
		} else {
			record.Request = "-1"
			record.Response = "-1"
		}
		if o.Record != nil {
			o.Record(record)
		}
		if o.Metric != nil {
			o.Metric(record)
		}
	})
}
func isMedia(uri string) bool {
	return strings.HasSuffix(uri, "js") || strings.HasSuffix(uri, "html") || strings.HasSuffix(uri, "css")
}
func allowBody(o AccessOptions, uri string) bool {
	if !o.RecordGatewayBody && strings.HasPrefix(uri, "/api/") {
		return false
	}
	if o.Desensitize.Mode == BlacklistMode && matchAny(o.Desensitize.BlackURLs, uri) {
		return false
	}
	if o.Desensitize.Mode == WhitelistMode && !matchAny(o.Desensitize.WhiteURLs, uri) {
		return false
	}
	if o.PermitOnly && !matchAny(o.PermitURLs, uri) {
		return false
	}
	return true
}
func findRule(rules []SensitiveRule, uri string) SensitiveRule {
	for _, rule := range rules {
		if rule.Path != "" && strings.Contains(uri, rule.Path) {
			return rule
		}
	}
	return SensitiveRule{}
}
func redactQuery(raw string, fields []string) string {
	if raw == "" || len(fields) == 0 {
		return raw
	}
	sensitive := make(map[string]bool, len(fields))
	for _, field := range fields {
		sensitive[field] = true
	}
	parts := strings.Split(raw, "&")
	for i, part := range parts {
		key, _, ok := strings.Cut(part, "=")
		if ok {
			decoded, err := url.QueryUnescape(key)
			if err == nil && sensitive[decoded] {
				parts[i] = key + "=****"
			}
		}
	}
	return strings.Join(parts, "&")
}
func redactBody(body []byte, rule SensitiveRule, max int) string {
	if len(body) == 0 {
		return ""
	}
	truncated := len(body) >= max
	if len(body) > max {
		body = body[:max]
	}
	if len(rule.FilterField) == 0 {
		if truncated {
			return strings.TrimSpace(string(body)) + "..."
		}
		return strings.TrimSpace(string(body))
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return "****"
	} // fail closed
	fields := make(map[string]bool, len(rule.FilterField))
	for _, field := range rule.FilterField {
		fields[field] = true
	}
	maskJSON(value, fields, rule.IsHash)
	encoded, err := json.Marshal(value)
	if err != nil {
		return "****"
	}
	if truncated {
		return string(encoded) + "..."
	}
	return string(encoded)
}
func maskJSON(value any, fields map[string]bool, hash bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if fields[key] {
				if str, ok := item.(string); ok {
					prefix, middle, suffix := javaMaskParts(str)
					masked := "****"
					if hash {
						masked = city64ToHex(middle)
					}
					v[key] = prefix + masked + suffix
				} else {
					v[key] = "****"
				}
			} else {
				maskJSON(item, fields, hash)
			}
		}
	case []any:
		for _, item := range v {
			maskJSON(item, fields, hash)
		}
	}
}

// Java StringBuilder indexes UTF-16 code units when selecting the middle half.
func javaMaskParts(s string) (string, string, string) {
	units := utf16.Encode([]rune(s))
	n := len(units) / 4
	return string(utf16.Decode(units[:n])), string(utf16.Decode(units[n : len(units)-n])), string(utf16.Decode(units[len(units)-n:]))
}

// Hutool ByteUtil.longToBytes emits little-endian bytes before HexUtil encodes them.
func city64ToHex(s string) string {
	if strings.TrimSpace(s) == "" {
		return s
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], city.Hash64([]byte(s)))
	return hex.EncodeToString(b[:])
}
func parseResponseCode(uri, method string, body []byte) int {
	if uri == "/api/v1/zen-desk/jwt" || method == http.MethodOptions {
		return 0
	}
	var value struct {
		Code json.Number `json:"code"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return -1
	}
	code, err := strconv.Atoi(value.Code.String())
	if err != nil {
		return -1
	}
	return code
}

type captureReader struct {
	io.ReadCloser
	body       bytes.Buffer
	total, max int
}

func (r *captureReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.total += n
	if remain := r.max - r.body.Len(); remain > 0 {
		if n < remain {
			remain = n
		}
		_, _ = r.body.Write(p[:remain])
	}
	return n, err
}

type captureWriter struct {
	http.ResponseWriter
	body               bytes.Buffer
	status, total, max int
	wrote              bool
}

func (w *captureWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *captureWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.total += n
	if remain := w.max - w.body.Len(); remain > 0 {
		if n < remain {
			remain = n
		}
		_, _ = w.body.Write(p[:remain])
	}
	return n, err
}
func (w *captureWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap lets http.ResponseController reach optional capabilities of the
// original writer, including full-duplex and deadlines.
func (w *captureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *captureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	conn, rw, err := hijacker.Hijack()
	if err == nil {
		w.status, w.wrote = http.StatusSwitchingProtocols, true
	}
	return conn, rw, err
}

func (w *captureWriter) Push(target string, options *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, options)
}

// ReadFrom preserves bounded capture when io.Copy uses ReaderFrom.
func (w *captureWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{w}, r)
}
