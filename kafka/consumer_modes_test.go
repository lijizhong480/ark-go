package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/lijizhong480/ark-go/kafka/lane"
	"github.com/IBM/sarama"
)

func TestConsumerGroupIDsAndModes(t *testing.T) {
	tests := []struct {
		cfg  Cluster
		want string
	}{
		{Cluster{GroupID: "orders"}, "orders"},
		{Cluster{GroupID: "orders", Router: lane.Router{Enabled: true, InstanceTag: "blue"}}, "orders-blue"},
		{Cluster{GroupID: "orders", Broadcast: true, InstanceID: "pod-1", Router: lane.Router{Enabled: true, InstanceTag: "blue"}}, "orderspod-1"},
	}
	for _, tc := range tests {
		got, err := tc.cfg.consumerGroupID()
		if err != nil || got != tc.want {
			t.Fatalf("group ID=%q want=%q err=%v", got, tc.want, err)
		}
	}
	for _, mode := range []AckMode{"", AckRecord, AckBatch, AckManual, AckManualImmediate} {
		if err := mode.validate(); err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
	}
	for _, mode := range []AckMode{"TIME", "COUNT", "COUNT_TIME", "INVALID"} {
		if err := mode.validate(); err == nil {
			t.Fatalf("unsupported mode %q accepted", mode)
		}
	}
}

func TestRecordModeCommitsWithoutExplicitAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	h := &groupHandler{cancel: cancel, mode: AckRecord, subscription: Subscription{Handler: func(context.Context, Delivery, Acknowledger) error { return nil }}}
	if !h.process(ctx, s, &sarama.ConsumerMessage{Topic: "orders"}) || s.marked != 1 || s.committed != 1 {
		t.Fatalf("record mode: %+v", s)
	}
}

func TestManualImmediateCommitsInsideHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	h := &groupHandler{cancel: cancel, mode: AckManualImmediate, subscription: Subscription{Handler: func(_ context.Context, _ Delivery, a Acknowledger) error {
		if !a.Ack() || !a.Ack() || s.committed != 1 {
			t.Fatalf("immediate ack count: %+v", s)
		}
		return nil
	}}}
	if !h.process(ctx, s, &sarama.ConsumerMessage{Topic: "orders"}) || s.marked != 1 || s.committed != 1 {
		t.Fatalf("manual immediate: %+v", s)
	}
}

func TestBatchListenerCommitAndManualAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := &sarama.ConsumerMessage{Topic: "orders", Offset: 5, Headers: []*sarama.RecordHeader{{Key: []byte(lane.TraceIDHeader), Value: []byte("batch-trace")}}}
	second := &sarama.ConsumerMessage{Topic: "orders", Offset: 6}
	s := &fakeMarker{}
	h := &groupHandler{cancel: cancel, batchListener: true, mode: AckManual,
		subscription: Subscription{BatchHandler: func(ctx context.Context, records []Delivery, a Acknowledger) error {
			if len(records) != 2 || TraceIDFromContext(ctx) != "batch-trace" {
				t.Fatalf("batch: %+v trace=%q", records, TraceIDFromContext(ctx))
			}
			if !a.Ack() {
				t.Fatal("ack rejected")
			}
			return nil
		}}}
	if !h.processBatch(ctx, s, []*sarama.ConsumerMessage{first, second}) || s.marked != 1 || s.committed != 1 {
		t.Fatalf("batch commit: %+v", s)
	}

	unacked := &fakeMarker{}
	h.subscription.BatchHandler = func(context.Context, []Delivery, Acknowledger) error { return nil }
	if h.processBatch(ctx, unacked, []*sarama.ConsumerMessage{first}) || unacked.marked != 0 {
		t.Fatalf("unacked batch committed: %+v", unacked)
	}
}

func TestBatchFailureDoesNotAdvancePastFilteredRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	h := &groupHandler{cancel: cancel, mode: AckBatch, router: lane.Router{Enabled: true},
		subscription: Subscription{Handler: func(context.Context, Delivery, Acknowledger) error { return errors.New("failed") }}}
	first := &sarama.ConsumerMessage{Topic: "orders", Offset: 1}
	filtered := &sarama.ConsumerMessage{Topic: "orders", Offset: 2, Headers: []*sarama.RecordHeader{{Key: []byte(lane.RouterTagHeader), Value: []byte("blue")}}}
	if h.processBatch(ctx, s, []*sarama.ConsumerMessage{first, filtered}) || s.marked != 0 || h.failure() == nil {
		t.Fatalf("failed batch advanced offset: %+v", s)
	}
}
