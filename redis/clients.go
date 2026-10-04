// Package redis provides named Redis clients and shared Ark configuration.
package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
)

// Config mirrors the connection fields of RedisProperties.RedisConfig.
type Config struct {
	Host           string
	Port           int
	Database       int
	Password       string
	SSL            bool
	Primary        bool
	MaxIdle        int
	MinIdle        int
	MaxActive      int
	MaxWait        time.Duration
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
}

func DefaultConfig() Config {
	return Config{
		Host: "localhost", Port: 6379, Database: 0,
		MaxIdle: 100, MinIdle: 20, MaxActive: 200,
		MaxWait: 5 * time.Second, ConnectTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	}
}

func (c Config) validate() error {
	if c.Host == "" || c.Port < 1 || c.Port > 65535 {
		return errors.New("Redis host or port is invalid")
	}
	if c.Database < 0 {
		return errors.New("Redis database must be nonnegative")
	}
	if c.MaxActive < 0 || c.MinIdle < 0 {
		return errors.New("Redis pool size must be nonnegative")
	}
	return nil
}

// Decrypt supports Java-style ENC(...) passwords without binding this project
// to a particular key-management service.
type Decrypt func(ciphertext string) (string, error)

type Clients struct {
	mu      sync.RWMutex
	named   map[string]*redisv8.Client
	primary string
	closed  bool
}

func NewClients(configs map[string]Config, decrypt Decrypt) (*Clients, error) {
	if len(configs) == 0 {
		return nil, errors.New("no Redis clients configured")
	}
	group := &Clients{named: make(map[string]*redisv8.Client, len(configs))}
	for name, cfg := range configs {
		if name == "" {
			group.Close()
			return nil, errors.New("Redis client name is empty")
		}
		if err := cfg.validate(); err != nil {
			group.Close()
			return nil, fmt.Errorf("Redis client %q: %w", name, err)
		}
		if strings.HasPrefix(cfg.Password, "ENC(") {
			if decrypt == nil {
				group.Close()
				return nil, fmt.Errorf("Redis client %q requires password decryptor", name)
			}
			password, err := decrypt(cfg.Password)
			if err != nil {
				group.Close()
				return nil, fmt.Errorf("decrypt Redis client %q: %w", name, err)
			}
			cfg.Password = password
		}
		if cfg.Primary {
			if group.primary != "" {
				group.Close()
				return nil, errors.New("multiple primary Redis clients")
			}
			group.primary = name
		}
		options := &redisv8.Options{
			Addr:     net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
			Password: cfg.Password, DB: cfg.Database,
			PoolSize: cfg.MaxActive, MinIdleConns: cfg.MinIdle,
			PoolTimeout: cfg.MaxWait, DialTimeout: cfg.ConnectTimeout,
			ReadTimeout: cfg.ReadTimeout,
		}
		if cfg.SSL {
			options.TLSConfig = defaultTLSConfig()
		}
		group.named[name] = redisv8.NewClient(options)
	}
	return group, nil
}

func (c *Clients) Client(name string) (*redisv8.Client, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, false
	}
	client, ok := c.named[name]
	return client, ok
}

func (c *Clients) Primary() (*redisv8.Client, bool) { return c.Client(c.primary) }

func (c *Clients) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return errors.New("Redis clients are closed")
	}
	for name, client := range c.named {
		if err := client.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("Redis client %q: %w", name, err)
		}
	}
	return nil
}

func (c *Clients) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var errs []error
	for name, client := range c.named {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("Redis client %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}
