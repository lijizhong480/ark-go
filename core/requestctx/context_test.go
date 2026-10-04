package requestctx

import (
	"context"
	"net/http"
	"testing"
)

func TestParseHeadersJavaCompatibility(t *testing.T) {
	h := http.Header{}
	h.Set("gateway_uid", "oops")
	h.Set("ab_deviceid", "ios-device")
	h.Set("country", "United+States")
	h.Set("province", "%E5%B9%BF%E4%B8%9C")
	h.Set("SERVICE_TOKEN", "internal-token")
	h.Set("x-router-tag", "lane-a")
	p := ParseHeaders(h, "/orders")
	if p.UID != 0 || p.MainUID != -1 || p.ABDeviceID != "ios-device" || p.Country != "United States" || p.Province != "广东" || p.Authorization != "internal-token" || p.RouterTag != "lane-a" || p.ReqURI != "/orders" {
		t.Fatalf("unexpected parsed request: %+v", p)
	}
	ctx := With(context.Background(), p)
	got, ok := From(ctx)
	if !ok || got != p {
		t.Fatalf("context value = %+v, %v", got, ok)
	}
}
