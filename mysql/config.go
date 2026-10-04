// Package mysql provides explicit multi-datasource routing over database/sql.
package mysql

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

type SourceConfig struct {
	// DSN is a Go MySQL DSN or a JDBC MySQL URL from the Java archetype.
	DSN                              string
	Username, Password               string
	MaxOpenConns, MaxIdleConns       int
	ConnMaxLifetime, ConnMaxIdleTime time.Duration
}
type Config struct {
	Primary string
	Strict  bool
	Sources map[string]SourceConfig
	// Read and Write optionally select named sources. If empty, routing infers
	// a Read counterpart of Primary's Write suffix.
	Read, Write string
}

// ParseJavaProperties recognizes the dynamic-datasource keys emitted by the
// Java service archetype. Unknown properties are left to the application.
func ParseJavaProperties(properties map[string]string) (Config, error) {
	cfg := Config{Primary: properties["spring.datasource.dynamic.primary"], Sources: map[string]SourceConfig{}}
	cfg.Strict = strings.EqualFold(properties["spring.datasource.dynamic.strict"], "true")
	const prefix = "spring.datasource.dynamic.datasource."
	for key, value := range properties {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		name, field, ok := strings.Cut(rest, ".")
		if !ok || name == "" {
			continue
		}
		source := cfg.Sources[name]
		switch field {
		case "url":
			source.DSN = value
		case "username":
			source.Username = value
		case "password":
			source.Password = value
		case "druid.max-active":
			n, err := strconv.Atoi(value)
			if err != nil {
				return Config{}, fmt.Errorf("mysql: %s: %w", key, err)
			}
			source.MaxOpenConns = n
		case "druid.min-idle":
			n, err := strconv.Atoi(value)
			if err != nil {
				return Config{}, fmt.Errorf("mysql: %s: %w", key, err)
			}
			source.MaxIdleConns = n
		default:
			continue
		}
		cfg.Sources[name] = source
	}
	if cfg.Primary == "" && len(cfg.Sources) == 1 {
		for name := range cfg.Sources {
			cfg.Primary = name
		}
	}
	return cfg, validate(cfg)
}
func validate(cfg Config) error {
	if len(cfg.Sources) == 0 {
		return errors.New("mysql: no data sources")
	}
	if cfg.Primary == "" {
		return errors.New("mysql: primary data source required")
	}
	if _, ok := cfg.Sources[cfg.Primary]; !ok {
		return fmt.Errorf("mysql: primary %q not defined", cfg.Primary)
	}
	for name, source := range cfg.Sources {
		if source.DSN == "" {
			return fmt.Errorf("mysql: source %q has empty DSN", name)
		}
		if source.MaxOpenConns < 0 || source.MaxIdleConns < 0 || source.ConnMaxIdleTime < 0 || source.ConnMaxLifetime < 0 {
			return fmt.Errorf("mysql: source %q has negative pool setting", name)
		}
		if source.MaxOpenConns > 0 && source.MaxIdleConns > source.MaxOpenConns {
			return fmt.Errorf("mysql: source %q idle connections exceed max open", name)
		}
	}
	for _, alias := range []string{cfg.Read, cfg.Write} {
		if alias != "" {
			if _, ok := cfg.Sources[alias]; !ok {
				return fmt.Errorf("mysql: source %q not defined", alias)
			}
		}
	}
	return nil
}

func makeDSN(source SourceConfig) (string, error) {
	if !strings.HasPrefix(source.DSN, "jdbc:mysql://") {
		// A native driver DSN can still have username/password overridden.
		parsed, err := driver.ParseDSN(source.DSN)
		if err != nil {
			return "", err
		}
		if source.Username != "" {
			parsed.User = source.Username
		}
		if source.Password != "" {
			parsed.Passwd = source.Password
		}
		return parsed.FormatDSN(), nil
	}
	address := strings.TrimPrefix(source.DSN, "jdbc:mysql://")
	parsed, err := url.Parse("mysql://" + address)
	if err != nil {
		return "", fmt.Errorf("mysql: JDBC URL: %w", err)
	}
	if parsed.Host == "" || strings.TrimPrefix(parsed.Path, "/") == "" {
		return "", errors.New("mysql: JDBC URL must contain host and database")
	}
	cfg := driver.NewConfig()
	cfg.User = source.Username
	cfg.Passwd = source.Password
	cfg.Net = "tcp"
	cfg.Addr = parsed.Host
	cfg.DBName = strings.TrimPrefix(parsed.Path, "/")
	cfg.ParseTime = true
	cfg.Params = map[string]string{}
	for key, values := range parsed.Query() {
		if len(values) == 0 {
			continue
		}
		switch key {
		case "allowMultiQueries":
			cfg.MultiStatements = strings.EqualFold(values[0], "true")
		case "useSSL":
			if strings.EqualFold(values[0], "true") {
				cfg.TLSConfig = "true"
			}
		case "serverTimezone":
			zone := values[0]
			if zone == "GMT+8" {
				zone = "Asia/Shanghai"
			}
			location, err := time.LoadLocation(zone)
			if err != nil {
				return "", fmt.Errorf("mysql: invalid serverTimezone %q: %w", zone, err)
			}
			cfg.Loc = location
		case "useUnicode", "characterEncoding", "allowPublicKeyRetrieval", "autoReconnect": // Java connector options, not Go driver settings.
		default:
			cfg.Params[key] = values[0]
		}
	}
	return cfg.FormatDSN(), nil
}
