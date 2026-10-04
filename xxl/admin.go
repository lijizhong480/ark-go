package xxl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// AdminClient uses the 2.1.2 AdminBizClient JSON endpoints and token header.
type AdminClient struct {
	Addresses   []string
	AccessToken string
	Client      *http.Client
}

func (a AdminClient) post(ctx context.Context, path string, payload any) error {
	if len(a.Addresses) == 0 {
		return errors.New("xxl: no admin addresses")
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var last error
	for _, base := range a.Addresses {
		address := strings.TrimRight(strings.TrimSpace(base), "/") + "/api/" + path
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(body))
		if err != nil {
			last = err
			continue
		}
		request.Header.Set("Content-Type", "application/json;charset=UTF-8")
		if a.AccessToken != "" {
			request.Header.Set("XXL-RPC-ACCESS-TOKEN", a.AccessToken)
		}
		response, err := client.Do(request)
		if err != nil {
			last = err
			continue
		}
		var result ReturnT[json.RawMessage]
		decodeErr := json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			last = fmt.Errorf("xxl: admin %s HTTP %d", address, response.StatusCode)
			continue
		}
		if decodeErr != nil {
			last = fmt.Errorf("xxl: admin %s: %w", address, decodeErr)
			continue
		}
		if result.Code != SuccessCode {
			last = fmt.Errorf("xxl: admin %s code %d: %s", address, result.Code, result.Msg)
			continue
		}
		return nil
	}
	return last
}
func (a AdminClient) Register(ctx context.Context, appName, address string) error {
	return a.post(ctx, "registry", RegistryParam{"EXECUTOR", appName, address})
}
func (a AdminClient) Unregister(ctx context.Context, appName, address string) error {
	return a.post(ctx, "registryRemove", RegistryParam{"EXECUTOR", appName, address})
}
func (a AdminClient) Callback(ctx context.Context, items []CallbackParam) error {
	return a.post(ctx, "callback", items)
}
