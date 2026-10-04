package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lijizhong480/ark-go/kafka/lane"
	"github.com/IBM/sarama"
)

type Delivery struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key, Value []byte
	Headers    map[string][]byte
	TraceID    string
}

// Acknowledger explicitly marks a successfully processed offset. Ack must be
// called before the handler returns; it returns false after that boundary.
// Calls from another goroutine are safe but must be joined before return.
type Acknowledger interface{ Ack() bool }
type Handler func(context.Context, Delivery, Acknowledger) error
type BatchHandler func(context.Context, []Delivery, Acknowledger) error

type AckMode string

const (
	AckRecord          AckMode = "RECORD"
	AckBatch           AckMode = "BATCH"
	AckManual          AckMode = "MANUAL"
	AckManualImmediate AckMode = "MANUAL_IMMEDIATE"
)

func (m AckMode) validate() error {
	switch m {
	case "", AckRecord, AckBatch, AckManual, AckManualImmediate:
		return nil
	default:
		return fmt.Errorf("unsupported Kafka ack mode %q", m)
	}
}

type FailureAction uint8

const (
	// Stop stops consumption without marking the failed offset.
	Stop FailureAction = iota
	// Skip marks the failed offset after OnFailure runs.
	Skip
)

type Subscription struct {
	Topics       []string
	Handler      Handler
	BatchHandler BatchHandler
	OnFailure    func(Delivery, error) FailureAction
}

// Consume joins the named cluster's consumer group and rejoins after every
// rebalance. It blocks until context cancellation or a Stop failure.
func (b *Broker) Consume(ctx context.Context, cluster string, sub Subscription) error {
	if len(sub.Topics) == 0 {
		return errors.New("Kafka subscription requires topics")
	}
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return errors.New("Kafka broker is closed")
	}
	if cluster == "" {
		cluster = b.primary
	}
	cfg, exists := b.clusters[cluster]
	b.mu.RUnlock()
	if !exists || !cfg.Consume {
		return fmt.Errorf("Kafka cluster %q has consumer disabled", cluster)
	}
	if cfg.BatchListener && sub.BatchHandler == nil {
		return errors.New("batch listener requires BatchHandler")
	}
	if !cfg.BatchListener && sub.Handler == nil {
		return errors.New("record listener requires Handler")
	}
	groupID, err := cfg.consumerGroupID()
	if err != nil {
		return err
	}
	workers := cfg.Concurrency
	if workers == 0 {
		workers = 10
	}
	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	h := &groupHandler{router: cfg.Router, subscription: sub, cancel: cancel, batchListener: cfg.BatchListener,
		batchSize: cfg.BatchSize, pollTimeout: cfg.PollTimeout, mode: cfg.AckMode}
	if h.mode == "" {
		h.mode = AckRecord
	}
	results := make(chan error, workers)
	started := 0
	var startupErr error
	for i := 0; i < workers; i++ {
		config := sarama.NewConfig()
		config.Consumer.Offsets.AutoCommit.Enable = false
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
		if cfg.Oldest {
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		}
		group, err := sarama.NewConsumerGroup(cfg.Brokers, groupID, config)
		if err != nil {
			startupErr = err
			cancel()
			break
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			group.Close()
			startupErr = errors.New("Kafka broker is closed")
			cancel()
			break
		}
		b.groups[group] = struct{}{}
		b.mu.Unlock()
		started++
		go func(group sarama.ConsumerGroup) {
			defer func() { b.mu.Lock(); delete(b.groups, group); b.mu.Unlock(); group.Close() }()
			for consumerCtx.Err() == nil {
				if err := group.Consume(consumerCtx, sub.Topics, h); err != nil {
					results <- err
					cancel()
					return
				}
				if err := h.failure(); err != nil {
					results <- err
					cancel()
					return
				}
			}
			results <- nil
		}(group)
	}
	var firstErr error
	for i := 0; i < started; i++ {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := h.failure(); err != nil {
		return err
	}
	if startupErr != nil {
		return startupErr
	}
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

type marker interface {
	MarkMessage(*sarama.ConsumerMessage, string)
	Commit()
}
type ack struct {
	session   marker
	message   *sarama.ConsumerMessage
	mu        sync.Mutex
	active    bool
	done      bool
	immediate bool
}

func (a *ack) Ack() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.active {
		return false
	}
	if a.done {
		return true
	}
	a.done = true
	if a.immediate {
		a.commit()
	}
	return true
}
func (a *ack) finish() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active = false
	return a.done
}
func (a *ack) commit() { a.session.MarkMessage(a.message, ""); a.session.Commit() }

type groupHandler struct {
	router        lane.Router
	subscription  Subscription
	cancel        context.CancelFunc
	mode          AckMode
	batchListener bool
	batchSize     int
	pollTimeout   time.Duration
	mu            sync.Mutex
	err           error
}

func (h *groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *groupHandler) failure() error                            { h.mu.Lock(); defer h.mu.Unlock(); return h.err }
func (h *groupHandler) stop(err error) {
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.mu.Unlock()
	h.cancel()
}

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if h.batchListener || h.mode == AckBatch {
		return h.consumeBatches(session, claim)
	}
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if !h.process(session.Context(), session, message) {
				return nil
			}
		}
	}
}

func (h *groupHandler) consumeBatches(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batchSize := h.batchSize
	if batchSize == 0 {
		batchSize = 500
	}
	pollTimeout := h.pollTimeout
	if pollTimeout == 0 {
		pollTimeout = 3 * time.Second
	}
	for {
		var first *sarama.ConsumerMessage
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			first = msg
		}
		messages := []*sarama.ConsumerMessage{first}
		timer := time.NewTimer(pollTimeout)
	collect:
		for len(messages) < batchSize {
			select {
			case <-session.Context().Done():
				timer.Stop()
				return nil
			case msg, ok := <-claim.Messages():
				if !ok {
					break collect
				}
				messages = append(messages, msg)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		if !h.processBatch(session.Context(), session, messages) {
			return nil
		}
	}
}

func deliveryFromMessage(message *sarama.ConsumerMessage) Delivery {
	headers := make(map[string][]byte, len(message.Headers))
	for _, header := range message.Headers {
		headers[string(header.Key)] = append([]byte(nil), header.Value...)
	}
	return Delivery{Topic: message.Topic, Partition: message.Partition, Offset: message.Offset,
		Key: append([]byte(nil), message.Key...), Value: append([]byte(nil), message.Value...), Headers: headers,
		TraceID: string(headers[lane.TraceIDHeader])}
}

func (h *groupHandler) processBatch(ctx context.Context, session marker, messages []*sarama.ConsumerMessage) bool {
	if len(messages) == 0 {
		return true
	}
	var deliveries []Delivery
	for _, message := range messages {
		delivery := deliveryFromMessage(message)
		if h.router.ShouldProcess(string(delivery.Headers[lane.RouterTagHeader])) {
			deliveries = append(deliveries, delivery)
		}
	}
	commitBatch := func() { session.MarkMessage(messages[len(messages)-1], ""); session.Commit() }
	if len(deliveries) == 0 {
		commitBatch()
		return true
	}
	if h.batchListener {
		a := &ack{session: session, message: messages[len(messages)-1], active: true, immediate: h.mode == AckManualImmediate}
		if deliveries[0].TraceID != "" {
			ctx = WithTraceID(ctx, deliveries[0].TraceID)
		}
		err := h.subscription.BatchHandler(ctx, deliveries, a)
		acknowledged := a.finish()
		if err != nil {
			action := Stop
			if h.subscription.OnFailure != nil {
				action = h.subscription.OnFailure(deliveries[0], err)
			}
			if action == Skip {
				commitBatch()
				return true
			}
			h.stop(fmt.Errorf("Kafka batch %s/%d: %w", deliveries[0].Topic, deliveries[0].Partition, err))
			return false
		}
		if h.mode == AckManual || h.mode == AckManualImmediate {
			if !acknowledged {
				h.stop(errors.New("Kafka batch returned without acknowledgment"))
				return false
			}
		}
		if h.mode != AckManualImmediate {
			commitBatch()
		}
		return true
	}
	for _, delivery := range deliveries {
		recordCtx := ctx
		if delivery.TraceID != "" {
			recordCtx = WithTraceID(ctx, delivery.TraceID)
		}
		a := &ack{active: true}
		err := h.subscription.Handler(recordCtx, delivery, a)
		a.finish()
		if err != nil {
			action := Stop
			if h.subscription.OnFailure != nil {
				action = h.subscription.OnFailure(delivery, err)
			}
			if action == Skip {
				continue
			}
			h.stop(fmt.Errorf("Kafka message %s/%d/%d: %w", delivery.Topic, delivery.Partition, delivery.Offset, err))
			return false
		}
	}
	commitBatch()
	return true
}

func (h *groupHandler) process(ctx context.Context, session marker, message *sarama.ConsumerMessage) bool {
	delivery := deliveryFromMessage(message)
	a := &ack{session: session, message: message, active: true, immediate: h.mode == AckManualImmediate}
	if !h.router.ShouldProcess(string(delivery.Headers[lane.RouterTagHeader])) {
		a.commit()
		return true
	}
	if delivery.TraceID != "" {
		ctx = WithTraceID(ctx, delivery.TraceID)
	}
	err := h.subscription.Handler(ctx, delivery, a)
	acknowledged := a.finish()
	if err != nil {
		action := Stop
		if h.subscription.OnFailure != nil {
			action = h.subscription.OnFailure(delivery, err)
		}
		if action == Skip {
			a.commit()
			return true
		}
		h.stop(fmt.Errorf("Kafka message %s/%d/%d: %w", delivery.Topic, delivery.Partition, delivery.Offset, err))
		return false
	}
	if h.mode == AckRecord || (acknowledged && h.mode != AckManualImmediate) {
		a.commit()
	}
	if !acknowledged && h.mode != AckRecord {
		h.stop(fmt.Errorf("Kafka message %s/%d/%d returned without acknowledgment", delivery.Topic, delivery.Partition, delivery.Offset))
		return false
	}
	return true
}
