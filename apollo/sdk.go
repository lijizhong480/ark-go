package apollo

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/apolloconfig/agollo/v4"
	apolloconfig "github.com/apolloconfig/agollo/v4/env/config"
	"github.com/apolloconfig/agollo/v4/storage"
)

type sdkSource struct{ client agollo.Client }

func (s sdkSource) Lookup(namespace, key string) (string, bool) {
	cfg := s.client.GetConfig(namespace)
	if cfg == nil || cfg.GetCache() == nil {
		return "", false
	}
	raw, err := cfg.GetCache().Get(key)
	if err != nil || raw == nil {
		return "", false
	}
	value, ok := raw.(string)
	return value, ok
}

type sdkListener struct{ config *Config }

func (l *sdkListener) OnChange(event *storage.ChangeEvent) {
	if event == nil {
		return
	}
	keys := make([]string, 0, len(event.Changes))
	for key := range event.Changes {
		keys = append(keys, key)
	}
	l.config.Notify(Change{Namespace: event.Namespace, Keys: keys})
}
func (l *sdkListener) OnNewestChange(event *storage.FullChangeEvent) {
	if event == nil {
		return
	}
	l.config.Notify(Change{Namespace: event.Namespace})
}

// NewWithClient attaches to an existing agollo client. Close removes only its
// listener; ownership of the client stays with the caller.
func NewWithClient(client agollo.Client, namespaces []string, ttl time.Duration) (*Config, error) {
	if client == nil {
		return nil, errors.New("apollo: nil client")
	}
	c, err := New(sdkSource{client: client}, namespaces, ttl)
	if err != nil {
		return nil, err
	}
	listener := &sdkListener{config: c}
	client.AddChangeListener(listener)
	c.closeSource = func() { client.RemoveChangeListener(listener) }
	return c, nil
}

// NewApollo starts an agollo client and owns its lifecycle. IP is the Apollo
// meta server URL. Namespace order is preserved, followed by common-rule.
type StartupOptions struct {
	AppID, Cluster, MetaURL string
	Namespaces              []string
	CacheTTL                time.Duration
	CheckAppID              bool
	CheckClient             *http.Client
}

// NewApolloWithOptions optionally verifies the App ID before starting the SDK.
func NewApolloWithOptions(ctx context.Context, options StartupOptions) (*Config, error) {
	if strings.TrimSpace(options.AppID) == "" || strings.TrimSpace(options.MetaURL) == "" {
		return nil, errors.New("apollo: app ID and meta URL are required")
	}
	ordered, err := New(noopSource{}, options.Namespaces, 0)
	if err != nil {
		return nil, err
	}
	if options.CheckAppID {
		if err := CheckAppID(ctx, options.CheckClient, options.MetaURL, options.AppID); err != nil {
			return nil, err
		}
	}
	all := ordered.Namespaces()
	client, err := agollo.StartWithConfig(func() (*apolloconfig.AppConfig, error) {
		return &apolloconfig.AppConfig{AppID: options.AppID, Cluster: options.Cluster, IP: options.MetaURL, NamespaceName: strings.Join(all, ","), MustStart: true}, nil
	})
	if err != nil {
		return nil, err
	}
	c, err := NewWithClient(client, options.Namespaces, options.CacheTTL)
	if err != nil {
		client.Close()
		return nil, err
	}
	removeListener := c.closeSource
	c.closeSource = func() { removeListener(); client.Close() }
	return c, nil
}

func NewApollo(appID, cluster, metaURL string, namespaces []string, ttl time.Duration) (*Config, error) {
	return NewApolloWithOptions(context.Background(), StartupOptions{AppID: appID, Cluster: cluster, MetaURL: metaURL, Namespaces: namespaces, CacheTTL: ttl})
}

// noopSource is used only to validate namespace order before starting the SDK.
type noopSource struct{}

func (noopSource) Lookup(string, string) (string, bool) { return "", false }
