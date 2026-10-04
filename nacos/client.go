package nacos

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

// Config is explicit because the Java ark-nacos-core supplies no runtime
// configuration schema. The archetype only disables Nacos in local settings.
type Config struct {
	Servers            []string // host:port
	Namespace, AppName string
	Username, Password string
	CacheDir, LogDir   string
	Timeout            time.Duration
}

func (c Config) validate() error {
	if len(c.Servers) == 0 {
		return errors.New("Nacos requires at least one server")
	}
	for _, server := range c.Servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil || host == "" {
			return fmt.Errorf("invalid Nacos server %q", server)
		}
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return fmt.Errorf("invalid Nacos server port %q", port)
		}
	}
	if c.Timeout < 0 {
		return errors.New("negative Nacos timeout")
	}
	if (c.Username == "") != (c.Password == "") {
		return errors.New("Nacos username and password must be configured together")
	}
	return nil
}

type configClient interface {
	GetConfig(vo.ConfigParam) (string, error)
	ListenConfig(vo.ConfigParam) error
	CancelListenConfig(vo.ConfigParam) error
}
type namingClient interface {
	RegisterInstance(vo.RegisterInstanceParam) (bool, error)
	DeregisterInstance(vo.DeregisterInstanceParam) (bool, error)
	SelectInstances(vo.SelectInstancesParam) ([]model.Instance, error)
	Subscribe(*vo.SubscribeParam) error
	Unsubscribe(*vo.SubscribeParam) error
}

type Client struct {
	mu        sync.Mutex
	config    configClient
	naming    namingClient
	subs      map[*Subscription]struct{}
	instances []vo.DeregisterInstanceParam
	closed    bool
}

func New(cfg Config) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	servers := make([]constant.ServerConfig, 0, len(cfg.Servers))
	for _, address := range cfg.Servers {
		host, rawPort, _ := net.SplitHostPort(address)
		port, _ := strconv.ParseUint(rawPort, 10, 16)
		servers = append(servers, constant.ServerConfig{IpAddr: host, Port: port})
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	clientCfg := &constant.ClientConfig{NamespaceId: cfg.Namespace, AppName: cfg.AppName,
		Username: cfg.Username, Password: cfg.Password, CacheDir: cfg.CacheDir, LogDir: cfg.LogDir,
		TimeoutMs: uint64(timeout.Milliseconds())}
	param := vo.NacosClientParam{ClientConfig: clientCfg, ServerConfigs: servers}
	configuration, err := clients.NewConfigClient(param)
	if err != nil {
		return nil, fmt.Errorf("create Nacos config client: %w", err)
	}
	naming, err := clients.NewNamingClient(param)
	if err != nil {
		return nil, fmt.Errorf("create Nacos naming client: %w", err)
	}
	return newClient(configuration, naming), nil
}

func newClient(configuration configClient, naming namingClient) *Client {
	return &Client{config: configuration, naming: naming, subs: make(map[*Subscription]struct{})}
}

func configKey(dataID, group string) (vo.ConfigParam, error) {
	if strings.TrimSpace(dataID) == "" || strings.TrimSpace(group) == "" {
		return vo.ConfigParam{}, errors.New("Nacos data ID and group are required")
	}
	return vo.ConfigParam{DataId: dataID, Group: group}, nil
}

func (c *Client) GetConfig(dataID, group string) (string, error) {
	key, err := configKey(dataID, group)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return "", errors.New("Nacos client is closed")
	}
	c.mu.Unlock()
	return c.config.GetConfig(key)
}

type Subscription struct {
	owner      *Client
	configKey  *vo.ConfigParam
	serviceKey *vo.SubscribeParam
	closed     atomic.Bool
}

// WatchConfig registers an SDK callback. Call Subscription.Close to cancel it.
func (c *Client) WatchConfig(dataID, group string, onChange func(content string)) (*Subscription, error) {
	key, err := configKey(dataID, group)
	if err != nil {
		return nil, err
	}
	if onChange == nil {
		return nil, errors.New("Nacos config callback is nil")
	}
	sub := &Subscription{owner: c, configKey: &key}
	key.OnChange = func(_, changedGroup, changedID, content string) {
		if !sub.closed.Load() && changedID == dataID && changedGroup == group {
			onChange(content)
		}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("Nacos client is closed")
	}
	c.mu.Unlock()
	if err := c.config.ListenConfig(key); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		sub.closed.Store(true)
		_ = c.config.CancelListenConfig(key)
		return nil, errors.New("Nacos client is closed")
	}
	c.subs[sub] = struct{}{}
	c.mu.Unlock()
	return sub, nil
}

type Instance struct {
	IP        string
	Port      uint64
	Weight    float64
	Metadata  map[string]string
	Cluster   string
	Ephemeral bool
}

func (c *Client) Register(service, group string, instance Instance) error {
	if service == "" || instance.IP == "" || instance.Port == 0 {
		return errors.New("Nacos service, IP and port are required")
	}
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	weight := instance.Weight
	if weight == 0 {
		weight = 1
	}
	param := vo.RegisterInstanceParam{ServiceName: service, GroupName: group, Ip: instance.IP, Port: instance.Port,
		Weight: weight, Enable: true, Healthy: true, ClusterName: instance.Cluster,
		Ephemeral: instance.Ephemeral, Metadata: instance.Metadata}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("Nacos client is closed")
	}
	c.mu.Unlock()
	ok, err := c.naming.RegisterInstance(param)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("Nacos instance registration rejected")
	}
	registered := vo.DeregisterInstanceParam{ServiceName: service, GroupName: group,
		Ip: instance.IP, Port: instance.Port, Cluster: instance.Cluster, Ephemeral: instance.Ephemeral}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_, _ = c.naming.DeregisterInstance(registered)
		return errors.New("Nacos client is closed")
	}
	c.instances = append(c.instances, registered)
	c.mu.Unlock()
	return nil
}

func (c *Client) Instances(service, group string, healthyOnly bool) ([]model.Instance, error) {
	if service == "" {
		return nil, errors.New("Nacos service is empty")
	}
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("Nacos client is closed")
	}
	c.mu.Unlock()
	return c.naming.SelectInstances(vo.SelectInstancesParam{ServiceName: service, GroupName: group, HealthyOnly: healthyOnly})
}

func (c *Client) WatchService(service, group string, onChange func([]model.SubscribeService, error)) (*Subscription, error) {
	if service == "" || onChange == nil {
		return nil, errors.New("Nacos service and callback are required")
	}
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	sub := &Subscription{owner: c}
	param := &vo.SubscribeParam{ServiceName: service, GroupName: group,
		SubscribeCallback: func(services []model.SubscribeService, err error) {
			if !sub.closed.Load() {
				onChange(services, err)
			}
		}}
	sub.serviceKey = param
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("Nacos client is closed")
	}
	c.mu.Unlock()
	if err := c.naming.Subscribe(param); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		sub.closed.Store(true)
		_ = c.naming.Unsubscribe(param)
		return nil, errors.New("Nacos client is closed")
	}
	c.subs[sub] = struct{}{}
	c.mu.Unlock()
	return sub, nil
}

func (s *Subscription) Close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	c := s.owner
	c.mu.Lock()
	if s.closed.Swap(true) {
		c.mu.Unlock()
		return nil
	}
	delete(c.subs, s)
	c.mu.Unlock()
	return c.unsubscribe(s)
}

func (c *Client) unsubscribe(s *Subscription) error {
	if s.configKey != nil {
		return c.config.CancelListenConfig(*s.configKey)
	}
	return c.naming.Unsubscribe(s.serviceKey)
}

// Close removes this wrapper's subscriptions and registered instances. Nacos
// SDK v1.1.1 has no client Close API, so its own background resources remain
// under SDK lifecycle control.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	subs := c.subs
	instances := c.instances
	toClose := make([]*Subscription, 0, len(subs))
	for sub := range subs {
		if !sub.closed.Swap(true) {
			toClose = append(toClose, sub)
		}
	}
	c.subs = nil
	c.instances = nil
	c.mu.Unlock()
	var errs []error
	for _, sub := range toClose {
		if err := c.unsubscribe(sub); err != nil {
			errs = append(errs, err)
		}
	}
	for _, instance := range instances {
		ok, err := c.naming.DeregisterInstance(instance)
		if err != nil {
			errs = append(errs, err)
		} else if !ok {
			errs = append(errs, fmt.Errorf("Nacos deregistration rejected for %s", instance.ServiceName))
		}
	}
	return errors.Join(errs...)
}
