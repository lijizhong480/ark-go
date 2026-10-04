package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/lijizhong480/ark-go/kafka/lane"
	"github.com/IBM/sarama"
)

type fakeProducer struct {
	message *sarama.ProducerMessage
	closed  bool
}

func (p *fakeProducer) SendMessage(m *sarama.ProducerMessage) (int32, int64, error) {
	p.message = m
	return 2, 42, nil
}
func (p *fakeProducer) Close() error { p.closed = true; return nil }

func TestSendRoutesHeadersAndCloses(t *testing.T) {
	p := &fakeProducer{}
	b := &Broker{clusters: map[string]Cluster{"a": {Produce: true, Router: lane.Router{Enabled: true, InstanceTag: "blue"}}}, producers: map[string]producer{"a": p}, primary: "a"}
	ctx := WithTraceID(context.Background(), "trace-1")
	result, err := b.Send(ctx, "", Record{Topic: "orders", Key: []byte("k"), Value: []byte("v"), Headers: map[string][]byte{"other": []byte("x")}})
	if err != nil || result.Partition != 2 || result.Offset != 42 {
		t.Fatalf("send: %+v %v", result, err)
	}
	headers := make(map[string]string)
	for _, h := range p.message.Headers {
		headers[string(h.Key)] = string(h.Value)
	}
	if headers[lane.TraceIDHeader] != "trace-1" || headers[lane.RouterTagHeader] != "blue" || headers["other"] != "x" {
		t.Fatalf("headers: %+v", headers)
	}
	if err := b.Close(); err != nil || !p.closed {
		t.Fatalf("close: %v %v", p.closed, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send(ctx, "", Record{Topic: "orders"}); err == nil {
		t.Fatal("send after close succeeded")
	}
}

type fakeMarker struct{ marked, committed int }

func (s *fakeMarker) MarkMessage(*sarama.ConsumerMessage, string) { s.marked++ }
func (s *fakeMarker) Commit()                                     { s.committed++ }

func TestConsumeManualAckAndTrace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	h := &groupHandler{cancel: cancel, subscription: Subscription{Handler: func(ctx context.Context, d Delivery, a Acknowledger) error {
		if d.TraceID != "trace-2" || TraceIDFromContext(ctx) != "trace-2" {
			t.Fatalf("trace: %+v", d)
		}
		a.Ack()
		return nil
	}}}
	msg := &sarama.ConsumerMessage{Topic: "orders", Partition: 1, Offset: 3, Headers: []*sarama.RecordHeader{{Key: []byte(lane.TraceIDHeader), Value: []byte("trace-2")}}}
	if !h.process(ctx, s, msg) || s.marked != 1 || s.committed != 1 {
		t.Fatalf("ack: %+v", s)
	}
}

func TestConsumeFailureAndLaneFiltering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	called := 0
	h := &groupHandler{cancel: cancel, router: lane.Router{Enabled: true}, subscription: Subscription{Handler: func(context.Context, Delivery, Acknowledger) error { called++; return errors.New("failure") }}}
	laneMessage := &sarama.ConsumerMessage{Topic: "orders", Headers: []*sarama.RecordHeader{{Key: []byte(lane.RouterTagHeader), Value: []byte("blue")}}}
	if !h.process(ctx, s, laneMessage) || called != 0 || s.marked != 1 {
		t.Fatalf("lane skip: called=%d marked=%d", called, s.marked)
	}
	trunkMessage := &sarama.ConsumerMessage{Topic: "orders"}
	if h.process(ctx, s, trunkMessage) || h.failure() == nil || s.marked != 1 {
		t.Fatalf("failure advanced offset: %+v", s)
	}
}

func TestConsumeWithoutAckStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	h := &groupHandler{cancel: cancel, subscription: Subscription{Handler: func(context.Context, Delivery, Acknowledger) error { return nil }}}
	if h.process(ctx, s, &sarama.ConsumerMessage{Topic: "orders"}) || h.failure() == nil || s.marked != 0 {
		t.Fatalf("unacked advanced offset: %+v", s)
	}
}

func TestLateAsyncAckIsRejectedWithoutCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeMarker{}
	acknowledgers := make(chan Acknowledger, 1)
	h := &groupHandler{cancel: cancel, subscription: Subscription{Handler: func(context.Context, Delivery, Acknowledger) error {
		return nil
	}}}
	h.subscription.Handler = func(_ context.Context, _ Delivery, a Acknowledger) error {
		acknowledgers <- a
		return nil
	}
	if h.process(ctx, s, &sarama.ConsumerMessage{Topic: "orders"}) {
		t.Fatal("unacked message continued")
	}
	a := <-acknowledgers
	result := make(chan bool, 1)
	go func() { result <- a.Ack() }()
	if <-result || s.marked != 0 || s.committed != 0 {
		t.Fatalf("late ack committed: %+v", s)
	}
}

func TestAckRaceAtHandlerReturnHasConsistentCommit(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		s := &fakeMarker{}
		accepted := make(chan bool, 1)
		h := &groupHandler{cancel: cancel, subscription: Subscription{Handler: func(_ context.Context, _ Delivery, a Acknowledger) error {
			go func() { accepted <- a.Ack() }()
			return nil
		}}}
		continued := h.process(ctx, s, &sarama.ConsumerMessage{Topic: "orders"})
		ackAccepted := <-accepted
		if ackAccepted != continued || s.committed != s.marked || (continued && s.committed != 1) || (!continued && s.committed != 0) {
			t.Fatalf("ack=%v continued=%v marker=%+v", ackAccepted, continued, s)
		}
		cancel()
	}
}
