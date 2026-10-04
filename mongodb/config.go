package mongodb

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type InstanceConfig struct {
	URI string
	// Template follows Java's five %s placeholders: username, password,
	// clusterEndpoint, database, readPreference.
	Template, Username, Password, Database, ClusterEndpoint, ReadPreference string
	ConnectTimeout, ReadTimeout, ServerSelectionTimeout                     time.Duration
	MaxPoolSize, MinPoolSize                                                uint64
	MaxConnIdleTime                                                         time.Duration
}
type Config struct {
	Instances map[string]InstanceConfig
	Decrypt   func(string) (string, error)
}

func (c Config) validate() error {
	if len(c.Instances) == 0 {
		return errors.New("mongodb: no instances")
	}
	for name, instance := range c.Instances {
		if name == "" || instance.Database == "" {
			return fmt.Errorf("mongodb: instance %q needs name and database", name)
		}
		if instance.URI == "" && instance.Template == "" {
			return fmt.Errorf("mongodb: instance %q needs URI or template", name)
		}
		if instance.MaxPoolSize > 0 && instance.MinPoolSize > instance.MaxPoolSize {
			return fmt.Errorf("mongodb: instance %q min pool exceeds max", name)
		}
		for _, duration := range []time.Duration{instance.ConnectTimeout, instance.ReadTimeout, instance.ServerSelectionTimeout, instance.MaxConnIdleTime} {
			if duration < 0 {
				return fmt.Errorf("mongodb: instance %q has negative timeout", name)
			}
		}
	}
	return nil
}
func (c Config) uri(instance InstanceConfig) (string, error) {
	password := instance.Password
	if strings.HasPrefix(password, "ENC(") {
		if !strings.HasSuffix(password, ")") {
			return "", errors.New("mongodb: malformed encrypted password")
		}
		if c.Decrypt == nil {
			return "", errors.New("mongodb: encrypted password requires decryptor")
		}
		var err error
		password, err = c.Decrypt(strings.TrimSuffix(strings.TrimPrefix(password, "ENC("), ")"))
		if err != nil {
			return "", err
		}
	}
	if instance.URI != "" {
		return instance.URI, nil
	}
	uri := fmt.Sprintf(instance.Template, url.QueryEscape(instance.Username), url.QueryEscape(password), instance.ClusterEndpoint, instance.Database, instance.ReadPreference)
	if strings.Contains(uri, "%!") {
		return "", errors.New("mongodb: invalid connection template")
	}
	if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
		return "", errors.New("mongodb: invalid connection URI scheme")
	}
	return uri, nil
}
