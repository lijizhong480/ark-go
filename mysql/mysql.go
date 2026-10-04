package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type Opener func(driverName, dsn string) (*sql.DB, error)
type Cluster struct {
	mu     sync.RWMutex
	cfg    Config
	dbs    map[string]*sql.DB
	closed bool
}

// Open constructs and pings each pool. On any failure it closes pools already
// opened. Use OpenWithOpener for tests or custom database/sql connectors.
func Open(ctx context.Context, cfg Config) (*Cluster, error) {
	return OpenWithOpener(ctx, cfg, sql.Open)
}
func OpenWithOpener(ctx context.Context, cfg Config, opener Opener) (*Cluster, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	if opener == nil {
		return nil, errors.New("mysql: nil opener")
	}
	cluster := &Cluster{cfg: cfg, dbs: map[string]*sql.DB{}}
	for name, source := range cfg.Sources {
		dsn, err := makeDSN(source)
		if err != nil {
			cluster.Close()
			return nil, fmt.Errorf("mysql: source %q: %w", name, err)
		}
		db, err := opener("mysql", dsn)
		if err != nil {
			cluster.Close()
			return nil, fmt.Errorf("mysql: open %q: %w", name, err)
		}
		cluster.dbs[name] = db
		if source.MaxOpenConns > 0 {
			db.SetMaxOpenConns(source.MaxOpenConns)
		}
		if source.MaxIdleConns > 0 {
			db.SetMaxIdleConns(source.MaxIdleConns)
		}
		if source.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(source.ConnMaxLifetime)
		}
		if source.ConnMaxIdleTime > 0 {
			db.SetConnMaxIdleTime(source.ConnMaxIdleTime)
		}
		if err := db.PingContext(ctx); err != nil {
			cluster.Close()
			return nil, fmt.Errorf("mysql: ping %q: %w", name, err)
		}
	}
	return cluster, nil
}

// DB selects a named source. With Strict=false, unknown names fall back to
// Primary, matching the archetype's dynamic.strict=false configuration.
func (c *Cluster) DB(name string) (*sql.DB, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, errors.New("mysql: cluster closed")
	}
	if name == "" {
		name = c.cfg.Primary
	}
	if db, ok := c.dbs[name]; ok {
		return db, nil
	}
	if !c.cfg.Strict {
		return c.dbs[c.cfg.Primary], nil
	}
	return nil, fmt.Errorf("mysql: unknown source %q", name)
}
func (c *Cluster) WriteDB() (*sql.DB, error) {
	c.mu.RLock()
	name := c.cfg.Write
	if name == "" {
		name = c.cfg.Primary
	}
	c.mu.RUnlock()
	return c.DB(name)
}
func (c *Cluster) ReadDB() (*sql.DB, error) {
	c.mu.RLock()
	name := c.cfg.Read
	if name == "" {
		write := c.cfg.Write
		if write == "" {
			write = c.cfg.Primary
		}
		if strings.HasSuffix(write, "Write") {
			candidate := strings.TrimSuffix(write, "Write") + "Read"
			if _, ok := c.dbs[candidate]; ok {
				name = candidate
			}
		}
	}
	c.mu.RUnlock()
	if name == "" {
		return c.WriteDB()
	}
	return c.DB(name)
}

// BeginTx always pins the transaction to the write pool. The caller owns
// Commit or Rollback; reads in a transaction must use the returned *sql.Tx.
func (c *Cluster) BeginTx(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error) {
	db, err := c.WriteDB()
	if err != nil {
		return nil, err
	}
	return db.BeginTx(ctx, options)
}
func (c *Cluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var first error
	for name, db := range c.dbs {
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("mysql: close %q: %w", name, err)
		}
	}
	return first
}
