package apollo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

var ErrAppNotFound = errors.New("apollo: app ID not found")

// CheckAppID verifies that the application namespace exists on a meta server.
// Unlike the Java startup check, transport failures are returned to the caller.
func CheckAppID(ctx context.Context, client *http.Client, metaURL, appID string) error {
	if strings.TrimSpace(metaURL) == "" || strings.TrimSpace(appID) == "" {
		return errors.New("apollo: meta URL and app ID are required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	base, err := url.Parse(metaURL)
	if err != nil {
		return fmt.Errorf("apollo: invalid meta URL: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return errors.New("apollo: meta URL must use http or https")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/configs/" + url.PathEscape(appID) + "/default/application"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrAppNotFound, appID)
	}
	if response.StatusCode >= 400 {
		return fmt.Errorf("apollo: app ID check returned HTTP %d", response.StatusCode)
	}
	return nil
}
