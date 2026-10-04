package lock

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fake struct {
	acquired bool
	events   *[]string
}

func (f fake) TryLock(context.Context, string, time.Duration) (Release, bool, error) {
	*f.events = append(*f.events, "acquire")
	return func(context.Context) error { *f.events = append(*f.events, "release"); return nil }, f.acquired, nil
}
func (fake) TryReadLock(context.Context, string, time.Duration, time.Duration) (Release, bool, error) {
	return nil, false, nil
}
func (fake) TryWriteLock(context.Context, string, time.Duration) (Release, bool, error) {
	return nil, false, nil
}
func (fake) IsReadLocked(context.Context, string) (bool, error) { return false, nil }

func TestExecuteReleasesAfterCallbackError(t *testing.T) {
	events := []string{}
	boom := errors.New("callback failed")
	_, err := Execute(context.Background(), fake{true, &events}, "order:1", time.Second, func(context.Context) (int, error) {
		events = append(events, "callback")
		return 0, boom
	}, nil)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"acquire", "callback", "release"}) {
		t.Fatalf("events = %v", events)
	}
}

func TestExecuteTimeoutCallback(t *testing.T) {
	events := []string{}
	v, err := Execute(context.Background(), fake{false, &events}, "order:1", time.Second, func(context.Context) (int, error) { t.Fatal("lock callback called"); return 0, nil }, func(context.Context) (int, error) { return 42, nil })
	if err != nil || v != 42 {
		t.Fatalf("fallback = %d, %v", v, err)
	}
}
