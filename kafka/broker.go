package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lijizhong480/ark-go/kafka/lane"
	"github.com/IBM/sarama"
)

// Cluster mirrors the independent broker and consumer-group settings in
// KafkaProperties.MskConfig. Producer and consumer are opt-in per cluster.
type Cluster struct {
	Brokers       []string
	GroupID       string
	Produce       bool
	Consume       bool
	Primary       bool
	Oldest        bool
	Router        lane.Router
	Broadcast     bool
	InstanceID    string
	Concurrency   int
	BatchListener bool
	BatchSize     int
	AckMode       AckMode
	PollTimeout   time.Duration
}

func (c Cluster) consumerGroupID() (string, error) {
	if c.GroupID == "" {
		return "", errors.New("Kafka consumer group is empty")
	}
	if c.Broadcast {
		instance := c.InstanceID
		if instance == "" {
			var err error
			instance, err = os.Hostname()
			if err != nil {
				return "", err
			}
		}
		if instance == "" {
			return "", errors.New("broadcast Kafka consumer requires instance ID")
		}
		return c.GroupID + instance, nil
	}
	if c.Router.Enabled && strings.TrimSpace(c.Router.InstanceTag) != "" {
		return c.GroupID + "-" + c.Router.InstanceTag, nil
	}
	return c.GroupID, nil
}

func (c Cluster) validateConsumer() error {
	if !c.Consume {
		return nil
	}
	if _, err := c.consumerGroupID(); err != nil {
		return err
	}
	if c.Concurrency < 0 || c.BatchSize < 0 || c.PollTimeout < 0 {
		return errors.New("Kafka concurrency, batch size and poll timeout must be nonnegative")
	}
	return c.AckMode.validate()
}

type producer interface {
	SendMessage(*sarama.ProducerMessage) (int32, int64, error)
	Close() error
}

type Broker struct {
	mu        sync.RWMutex
	clusters  map[string]Cluster
	producers map[string]producer
	groups    map[sarama.ConsumerGroup]struct{}
	primary   string
	closed    bool
}

func NewBroker(clusters map[string]Cluster) (*Broker, error) {
	if len(clusters) == 0 {
		return nil, errors.New("no Kafka clusters configured")
	}
	b := &Broker{clusters: make(map[string]Cluster, len(clusters)), producers: make(map[string]producer), groups: make(map[sarama.ConsumerGroup]struct{})}
	for name, cfg := range clusters {
		cfg.AckMode = AckMode(strings.ToUpper(strings.TrimSpace(string(cfg.AckMode))))
		if name == "" || len(cfg.Brokers) == 0 {
			b.Close()
			return nil, fmt.Errorf("Kafka cluster %q requires name and brokers", name)
		}
		if err := cfg.validateConsumer(); err != nil {
			b.Close()
			return nil, fmt.Errorf("Kafka cluster %q: %w", name, err)
		}
		if cfg.Primary {
			if b.primary != "" {
				b.Close()
				return nil, errors.New("multiple primary Kafka clusters")
			}
			b.primary = name
		}
		b.clusters[name] = cfg
		if cfg.Produce {
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			config.Producer.RequiredAcks = sarama.WaitForAll
			p, err := sarama.NewSyncProducer(cfg.Brokers, config)
			if err != nil {
				b.Close()
				return nil, fmt.Errorf("Kafka producer %q: %w", name, err)
			}
			b.producers[name] = p
		}
	}
	return b, nil
}

func (b *Broker) Primary() string { b.mu.RLock(); defer b.mu.RUnlock(); return b.primary }

type Record struct {
	Topic      string
	Key, Value []byte
	Headers    map[string][]byte
}

type SendResult struct {
	Partition int32
	Offset    int64
}

// Send waits for broker acknowledgement. Context is checked before the call;
// Sarama's synchronous SendMessage cannot interrupt an in-flight send.
func (b *Broker) Send(ctx context.Context, cluster string, record Record) (SendResult, error) {
	if err := ctx.Err(); err != nil {
		return SendResult{}, err
	}
	if record.Topic == "" {
		return SendResult{}, errors.New("Kafka topic is empty")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return SendResult{}, errors.New("Kafka broker is closed")
	}
	if cluster == "" {
		cluster = b.primary
	}
	cfg, exists := b.clusters[cluster]
	if !exists {
		return SendResult{}, fmt.Errorf("unknown Kafka cluster %q", cluster)
	}
	p := b.producers[cluster]
	if p == nil {
		return SendResult{}, fmt.Errorf("Kafka cluster %q has producer disabled", cluster)
	}
	headers := cfg.Router.ProducerHeaders(record.Headers, TraceIDFromContext(ctx))
	msg := &sarama.ProducerMessage{Topic: record.Topic, Key: sarama.ByteEncoder(record.Key), Value: sarama.ByteEncoder(record.Value)}
	for key, value := range headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: value})
	}
	partition, offset, err := p.SendMessage(msg)
	return SendResult{Partition: partition, Offset: offset}, err
}

func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	producers := b.producers
	groups := b.groups
	b.producers = nil
	b.groups = nil
	b.mu.Unlock()
	var errs []error
	for name, p := range producers {
		if err := p.Close(); err != nil {
			errs = append(errs, fmt.Errorf("producer %q: %w", name, err))
		}
	}
	for g := range groups {
		if err := g.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type traceContextKey struct{}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceContextKey{}, traceID)
}
func TraceIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceContextKey{}).(string)
	return value
}
