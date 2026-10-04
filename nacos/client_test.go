package nacos

import (
	"testing"

	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

type fakeConfig struct {
	current  string
	listener vo.ConfigParam
	canceled int
}

func (f *fakeConfig) GetConfig(vo.ConfigParam) (string, error) { return f.current, nil }
func (f *fakeConfig) ListenConfig(p vo.ConfigParam) error      { f.listener = p; return nil }
func (f *fakeConfig) CancelListenConfig(vo.ConfigParam) error  { f.canceled++; return nil }

type fakeNaming struct {
	registered, deregistered, subscribed, unsubscribed int
	subscription                                       *vo.SubscribeParam
	selected                                           vo.SelectInstancesParam
}

func (f *fakeNaming) RegisterInstance(vo.RegisterInstanceParam) (bool, error) {
	f.registered++
	return true, nil
}
func (f *fakeNaming) DeregisterInstance(vo.DeregisterInstanceParam) (bool, error) {
	f.deregistered++
	return true, nil
}
func (f *fakeNaming) SelectInstances(p vo.SelectInstancesParam) ([]model.Instance, error) {
	f.selected = p
	return []model.Instance{{Ip: "127.0.0.1"}}, nil
}
func (f *fakeNaming) Subscribe(p *vo.SubscribeParam) error {
	f.subscribed++
	f.subscription = p
	return nil
}
func (f *fakeNaming) Unsubscribe(*vo.SubscribeParam) error { f.unsubscribed++; return nil }

func TestConfigWatchAndClose(t *testing.T) {
	configuration := &fakeConfig{current: "first"}
	naming := &fakeNaming{}
	c := newClient(configuration, naming)
	if value, err := c.GetConfig("settings.yaml", "DEFAULT_GROUP"); err != nil || value != "first" {
		t.Fatalf("get: %s %v", value, err)
	}
	changes := 0
	sub, err := c.WatchConfig("settings.yaml", "DEFAULT_GROUP", func(value string) {
		if value != "changed" {
			t.Errorf("value: %q", value)
		}
		changes++
	})
	if err != nil {
		t.Fatal(err)
	}
	configuration.listener.OnChange("ns", "DEFAULT_GROUP", "other.yaml", "ignored")
	configuration.listener.OnChange("ns", "DEFAULT_GROUP", "settings.yaml", "changed")
	if changes != 1 {
		t.Fatalf("changes: %d", changes)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	configuration.listener.OnChange("ns", "DEFAULT_GROUP", "settings.yaml", "changed")
	if changes != 1 || configuration.canceled != 1 {
		t.Fatalf("cancel: changes=%d canceled=%d", changes, configuration.canceled)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetConfig("settings.yaml", "DEFAULT_GROUP"); err == nil {
		t.Fatal("read after close succeeded")
	}
}

func TestDiscoveryRegisterSubscribeAndClose(t *testing.T) {
	configuration := &fakeConfig{}
	naming := &fakeNaming{}
	c := newClient(configuration, naming)
	if err := c.Register("orders", "", Instance{IP: "127.0.0.1", Port: 8080}); err != nil {
		t.Fatal(err)
	}
	instances, err := c.Instances("orders", "", true)
	if err != nil || len(instances) != 1 || naming.selected.GroupName != "DEFAULT_GROUP" || !naming.selected.HealthyOnly {
		t.Fatalf("instances: %v %+v %v", instances, naming.selected, err)
	}
	changes := 0
	_, err = c.WatchService("orders", "", func(_ []model.SubscribeService, err error) {
		if err != nil {
			t.Error(err)
		}
		changes++
	})
	if err != nil {
		t.Fatal(err)
	}
	naming.subscription.SubscribeCallback(nil, nil)
	if changes != 1 {
		t.Fatalf("service changes: %d", changes)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	naming.subscription.SubscribeCallback(nil, nil)
	if naming.registered != 1 || naming.deregistered != 1 || naming.unsubscribed != 1 || changes != 1 {
		t.Fatalf("lifecycle: %+v changes=%d", naming, changes)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Register("orders", "", Instance{IP: "127.0.0.1", Port: 8080}); err == nil {
		t.Fatal("register after close succeeded")
	}
}

func TestValidation(t *testing.T) {
	for _, cfg := range []Config{{}, {Servers: []string{"bad"}}, {Servers: []string{"127.0.0.1:0"}}, {Servers: []string{"127.0.0.1:8848"}, Username: "user"}} {
		if err := cfg.validate(); err == nil {
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
	if err := (Config{Servers: []string{"127.0.0.1:8848"}}).validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := configKey("", "DEFAULT_GROUP"); err == nil {
		t.Fatal("empty data ID accepted")
	}
}
