package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lijizhong480/ark-go/core/requestctx"
	"github.com/lijizhong480/ark-go/core/response"
)

const (
	BanUID    = "ban.uid"
	BanDID    = "ban.did"
	BanIP     = "ban.ip"
	BanURL    = "ban.url"
	BanUIDURL = "ban.uid_url"
	BanDIDURL = "ban.did_url"
	BanIPURL  = "ban.ip_url"
)

var banOrder = []string{BanUID, BanDID, BanIP, BanURL, BanUIDURL, BanDIDURL, BanIPURL}

// ListConfig is implemented by the Apollo project's Config without importing it.
type ListConfig interface {
	List(key, delimiter string, fallback []string) ([]string, error)
}
type BanMetric func(kind, uri string)
type BanEvent func(kind string, paras requestctx.CommonParas)

type banRules map[string]map[string]struct{}
type BanFilter struct {
	source    ListConfig
	rules     atomic.Pointer[banRules]
	Metric    BanMetric
	Event     BanEvent
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewBanFilter loads all seven Java ban keys atomically and refreshes at the
// supplied interval. An interval <= 0 disables background refresh.
func NewBanFilter(source ListConfig, interval time.Duration) (*BanFilter, error) {
	if source == nil {
		return nil, errors.New("web: nil ban config")
	}
	b := &BanFilter{source: source, stop: make(chan struct{}), done: make(chan struct{})}
	if err := b.Refresh(); err != nil {
		return nil, err
	}
	if interval > 0 {
		go b.refreshLoop(interval)
	} else {
		close(b.done)
	}
	return b, nil
}
func (b *BanFilter) Refresh() error {
	next := make(banRules, len(banOrder))
	for _, kind := range banOrder {
		values, err := b.source.List(kind, ",", nil)
		if err != nil {
			return fmt.Errorf("web: load %s: %w", kind, err)
		}
		members := map[string]struct{}{}
		for _, value := range values {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				members[trimmed] = struct{}{}
			}
		}
		next[kind] = members
	}
	b.rules.Store(&next)
	return nil
}
func (b *BanFilter) refreshLoop(interval time.Duration) {
	defer close(b.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			_ = b.Refresh()
		}
	}
}
func (b *BanFilter) Close() {
	b.closeOnce.Do(func() { close(b.stop) })
	<-b.done
}
func (b *BanFilter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paras, ok := requestctx.From(r.Context())
		if !ok {
			paras = requestctx.FromRequest(r)
		}
		rules := b.rules.Load()
		if rules != nil {
			uri := r.URL.Path
			uid := strconv.FormatInt(paras.UID, 10)
			candidates := [][2]string{
				{BanUID, uid}, {BanDID, paras.DeviceID}, {BanIP, paras.IP}, {BanURL, uri},
				{BanUIDURL, uid + "_" + uri}, {BanDIDURL, paras.DeviceID + "_" + uri}, {BanIPURL, paras.IP + "_" + uri},
			}
			for _, item := range candidates {
				if (item[0] == BanUID || item[0] == BanUIDURL) && paras.UID < 0 {
					continue
				}
				if (item[0] == BanDID || item[0] == BanDIDURL) && paras.DeviceID == "" {
					continue
				}
				if (item[0] == BanIP || item[0] == BanIPURL) && paras.IP == "" {
					continue
				}
				if _, found := (*rules)[item[0]][item[1]]; !found {
					continue
				}
				if b.Event != nil {
					b.Event(item[0], paras)
				}
				if b.Metric != nil {
					b.Metric(item[0], uri)
				}
				_ = WriteResponse(w, http.StatusOK, response.Failure(response.BlackBan, ""))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
