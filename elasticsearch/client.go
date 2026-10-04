package elasticsearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Cluster describes one Elasticsearch endpoint. Indexes is the Java
// indexesList mapping after splitting the comma-separated property.
type Cluster struct {
	URL                string
	Indexes            []string
	Primary            bool
	Username, Password string
	Timeout            time.Duration
	AWS                *AWSConfig
}

type AWSConfig struct{ AccessKeyID, SecretAccessKey, SessionToken, Region, Service string }

type Client struct {
	mu       sync.RWMutex
	clusters map[string]*endpoint
	order    []string
	index    map[string]string
	closed   bool
}

type endpoint struct {
	base *url.URL
	http *http.Client
	auth Cluster
}

func NewClient(configs map[string]Cluster) (*Client, error) {
	if len(configs) == 0 {
		return nil, errors.New("no Elasticsearch clusters configured")
	}
	c := &Client{clusters: make(map[string]*endpoint, len(configs)), index: make(map[string]string)}
	for name := range configs {
		c.order = append(c.order, name)
	}
	sort.Strings(c.order)
	primary := 0
	for _, name := range c.order {
		cfg := configs[name]
		if name == "" {
			return nil, errors.New("Elasticsearch cluster name is empty")
		}
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("Elasticsearch cluster %q has invalid URL", name)
		}
		if cfg.Timeout < 0 {
			return nil, fmt.Errorf("Elasticsearch cluster %q has negative timeout", name)
		}
		if cfg.AWS != nil {
			if cfg.AWS.AccessKeyID == "" || cfg.AWS.SecretAccessKey == "" || cfg.AWS.Region == "" {
				return nil, fmt.Errorf("Elasticsearch cluster %q has incomplete AWS credentials", name)
			}
			if cfg.Username != "" {
				return nil, fmt.Errorf("Elasticsearch cluster %q cannot combine basic and AWS auth", name)
			}
		}
		if cfg.Primary {
			primary++
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		timeout := cfg.Timeout
		if timeout == 0 {
			timeout = 5 * time.Second
		}
		c.clusters[name] = &endpoint{base: u, http: &http.Client{Transport: transport, Timeout: timeout}, auth: cfg}
		for _, rawIndex := range cfg.Indexes {
			index := strings.TrimSpace(rawIndex)
			if err := validSegment(index); err != nil {
				return nil, fmt.Errorf("cluster %q index: %w", name, err)
			}
			if other := c.index[index]; other != "" {
				return nil, fmt.Errorf("index %q mapped to both %q and %q", index, other, name)
			}
			c.index[index] = name
		}
	}
	if primary > 1 {
		return nil, errors.New("multiple primary Elasticsearch clusters")
	}
	return c, nil
}

func validSegment(value string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/?#,\\") {
		return fmt.Errorf("invalid Elasticsearch path segment %q", value)
	}
	return nil
}

// ClusterForIndex first checks explicit mappings, then probes unmapped indexes
// in deterministic cluster-name order and caches the discovered route.
func (c *Client) ClusterForIndex(ctx context.Context, index string) (string, error) {
	if err := validSegment(index); err != nil {
		return "", err
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return "", errors.New("Elasticsearch client is closed")
	}
	if name := c.index[index]; name != "" {
		c.mu.RUnlock()
		return name, nil
	}
	order := append([]string(nil), c.order...)
	c.mu.RUnlock()
	var probeErr error
	for _, name := range order {
		status, _, err := c.request(ctx, name, http.MethodHead, "/"+url.PathEscape(index), nil, "")
		if err != nil {
			probeErr = errors.Join(probeErr, fmt.Errorf("cluster %q: %w", name, err))
			continue
		}
		if status == http.StatusOK {
			c.mu.Lock()
			if !c.closed {
				c.index[index] = name
			}
			c.mu.Unlock()
			return name, nil
		}
		if status != http.StatusNotFound {
			probeErr = errors.Join(probeErr, fmt.Errorf("cluster %q probe returned %d", name, status))
		}
	}
	if probeErr != nil {
		return "", probeErr
	}
	return "", fmt.Errorf("index %q not found in any Elasticsearch cluster", index)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	for _, e := range c.clusters {
		e.http.CloseIdleConnections()
	}
	return nil
}

type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("Elasticsearch HTTP %d: %s", e.Status, e.Body)
}

func (c *Client) request(ctx context.Context, cluster, method, path string, body []byte, contentType string) (int, []byte, error) {
	c.mu.RLock()
	e, ok := c.clusters[cluster]
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return 0, nil, errors.New("Elasticsearch client is closed")
	}
	if !ok {
		return 0, nil, fmt.Errorf("unknown Elasticsearch cluster %q", cluster)
	}
	u := *e.base
	pathOnly, query, hasQuery := strings.Cut(path, "?")
	rawPath := strings.TrimRight(e.base.EscapedPath(), "/") + pathOnly
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return 0, nil, err
	}
	u.Path, u.RawPath = decodedPath, rawPath
	if hasQuery {
		u.RawQuery = query
	}
	request, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if e.auth.AWS != nil {
		cfg := e.auth.AWS
		service := cfg.Service
		if service == "" {
			service = "es"
		}
		hash := sha256.Sum256(body)
		creds := aws.Credentials{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey, SessionToken: cfg.SessionToken}
		if err := v4.NewSigner().SignHTTP(ctx, creds, request, hex.EncodeToString(hash[:]), service, cfg.Region, time.Now()); err != nil {
			return 0, nil, err
		}
	} else if e.auth.Username != "" {
		request.SetBasicAuth(e.auth.Username, e.auth.Password)
	}
	response, err := e.http.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	return response.StatusCode, data, err
}

func (c *Client) doIndex(ctx context.Context, index, method, suffix string, body []byte, contentType string) ([]byte, error) {
	cluster, err := c.ClusterForIndex(ctx, index)
	if err != nil {
		return nil, err
	}
	status, data, err := c.request(ctx, cluster, method, "/"+url.PathEscape(index)+suffix, body, contentType)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &StatusError{status, string(data)}
	}
	return data, nil
}

func encodeJSON(value any) ([]byte, error) { return json.Marshal(value) }
