package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type component struct {
	name     string
	events   *[]string
	startErr error
}

func (c *component) Name() string { return c.name }
func (c *component) Start(context.Context) error {
	*c.events = append(*c.events, "start:"+c.name)
	return c.startErr
}
func (c *component) Stop(context.Context) error {
	*c.events = append(*c.events, "stop:"+c.name)
	return nil
}

func TestStartupFailureRollsBackIncludingFailedComponent(t *testing.T) {
	events := []string{}
	boom := errors.New("boom")
	a, err := New(&component{"one", &events, nil}, &component{"two", &events, boom}, &component{"three", &events, nil})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("start error = %v", err)
	}
	want := []string{"start:one", "start:two", "stop:two", "stop:one"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if a.Running() {
		t.Fatal("app reports running after rollback")
	}
}

func TestStopReversesOrderAndIsIdempotent(t *testing.T) {
	events := []string{}
	a, err := New(&component{"one", &events, nil}, &component{"two", &events, nil})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:one", "start:two", "stop:two", "stop:one"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}
