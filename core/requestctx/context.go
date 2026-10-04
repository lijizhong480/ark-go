package requestctx

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// CommonParas mirrors fields populated by Java RequestContext.parseCommonParas.
// It is a value carried with context.Context, never process-global state.
type CommonParas struct {
	ReqURI, AppID, AppVersion, ClientTraceID, Lang, PlatformID          string
	Channel, DeviceBrand, OSVersion, DeviceID, AntiDeviceID, ABDeviceID string
	Authorization                                                       string
	UID                                                                 int64
	RouterTag, FeatureTag                                               string
	Country, CountryCode, Province, City                                string
	VPNHeader, IP, MainAppID, AppSiteID                                 string
	MainUID                                                             int64
	Web3UserFlag, LoginType                                             int
	ReqServer, InnerAccountName, InnerAccountID                         string
}

type contextKey struct{}

func With(ctx context.Context, paras CommonParas) context.Context {
	return context.WithValue(ctx, contextKey{}, paras)
}

func From(ctx context.Context) (CommonParas, bool) {
	if ctx == nil {
		return CommonParas{}, false
	}
	p, ok := ctx.Value(contextKey{}).(CommonParas)
	return p, ok
}

// ParseHeaders preserves the Java header names and fallback/default behavior.
// Invalid numeric values use their Java defaults. A malformed location escape
// leaves the original value intact.
func ParseHeaders(h http.Header, requestURI string) CommonParas {
	get := h.Get
	ab := get("ab_deviceId")
	if strings.TrimSpace(ab) == "" {
		ab = get("ab_deviceid")
	}
	uid, _ := strconv.ParseInt(get("gateway_uid"), 10, 64)
	mainUID := int64(-1)
	if parsed, err := strconv.ParseInt(get("parent_uid"), 10, 64); err == nil {
		mainUID = parsed
	}
	web3Flag, _ := strconv.Atoi(get("web3-user-flag"))
	loginType, _ := strconv.Atoi(get("login-type"))
	return CommonParas{
		ReqURI: requestURI, AppID: get("appId"), AppVersion: get("app_version"),
		ClientTraceID: get("traceid"), Lang: get("lang"), PlatformID: get("platformId"),
		Channel: get("channel"), DeviceBrand: get("device_brand"), OSVersion: get("os_version"),
		DeviceID: get("device_id"), AntiDeviceID: get("antiDeviceId"), ABDeviceID: ab,
		Authorization: get("SERVICE_TOKEN"), UID: uid, RouterTag: get("x-router-tag"),
		FeatureTag: get("x-feature-tag"), Country: decode(get("country")),
		CountryCode: get("country-code"), Province: decode(get("province")), City: decode(get("city")),
		VPNHeader: get("x-amzn-waf-is-use-vpn"), IP: get("ip"), MainAppID: get("mainAppId"),
		AppSiteID: get("appSiteId"), MainUID: mainUID, Web3UserFlag: web3Flag,
		LoginType: loginType, ReqServer: get("reqServer"),
		InnerAccountName: get("inner-gateway-account-name"), InnerAccountID: get("inner-gateway-account-id"),
	}
}

func FromRequest(r *http.Request) CommonParas { return ParseHeaders(r.Header, r.URL.EscapedPath()) }

func decode(s string) string {
	if strings.TrimSpace(s) == "" {
		return s
	}
	decoded, err := url.QueryUnescape(s)
	if err != nil {
		return s
	}
	return decoded
}
