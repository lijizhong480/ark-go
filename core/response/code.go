package response

import "fmt"

// Code preserves the numeric response codes in ark-base-core ResponseCode.
type Code int

const (
	Success                 Code = 0
	Unauthorized            Code = 100201
	SignInFail              Code = 10002
	InsufficientBalance     Code = 100202
	SearchNoContent         Code = 100204
	RepeatRequest           Code = 100205
	OrderAlreadyClose       Code = 100206
	SuccessHint             Code = 100207
	Processing              Code = 100208
	SystemMaintenance       Code = 100209
	IllegalArgument         Code = 100400
	AuthenticationFail      Code = 100401
	AuthorizationFail       Code = 100403
	MQ404                   Code = 100404
	FrequencyLimit          Code = 100410
	SignatureVerifyFail     Code = 100411
	SignatureAppkeyNotexist Code = 100412
	SignatureAppkeyInvalid  Code = 100413
	RiskControl             Code = 100414
	AreaRiskControl         Code = 100415
	ReqFastFail             Code = 100416
	OldVersion              Code = 100417
	MarketClose             Code = 100418
	IPLimit                 Code = 100419
	SMSWrongAttemptTimes    Code = 100420
	MismatchCondition       Code = 100421
	WalletMaintain          Code = 100422
	WalletRickControl       Code = 100423
	UserCancel              Code = 100424
	UserNotQulified         Code = 100425
	DataHasModify           Code = 100426
	APIKeyIllegal           Code = 100404 // Java defines the same numeric code as MQ_404.
	APIKeyLimited           Code = 100405
	APIKeyInvalid           Code = 100406
	BlackBan                Code = 100409
	SocialfeedPostBanned    Code = 100430
	SocialfeedNotTrader     Code = 100431
	UserSuspectWool         Code = 100442
	InternalServerError     Code = 100500
	ServerBusy              Code = 100503
	ServerDegrade           Code = 100504
	ServerLogicalErr        Code = 100505
	UserBasicKYC            Code = 100451
	UserAdvancedKYC         Code = 100452
	RiskControlTips         Code = 100453
)

type Definition struct {
	Code        Code
	Name        string
	Description string
}

func (d Definition) I18nMsgKey() string { return "errorCode_" + d.Name }

// Lookup follows Java's findRspByCode: first declaration wins for duplicate values.
func Lookup(code Code) (Definition, bool) {
	d, ok := definitions[code]
	return d, ok
}

func (c Code) String() string {
	if d, ok := Lookup(c); ok {
		return d.Name
	}
	return fmt.Sprint(int(c))
}

var definitions = map[Code]Definition{
	Success:                 {Success, "SUCCESS", "SUCCESS"},
	Unauthorized:            {Unauthorized, "UNAUTHORIZED", ""},
	SignInFail:              {SignInFail, "SIGN_IN_FAIL", ""},
	InsufficientBalance:     {InsufficientBalance, "INSUFFICIENT_BALANCE", ""},
	SearchNoContent:         {SearchNoContent, "SEARCH_NO_CONTENT", ""},
	RepeatRequest:           {RepeatRequest, "REPEAT_REQUEST", ""},
	OrderAlreadyClose:       {OrderAlreadyClose, "ORDER_ALREADY_CLOSE", ""},
	SuccessHint:             {SuccessHint, "SUCCESS_HINT", ""},
	Processing:              {Processing, "PROCESSING", ""},
	SystemMaintenance:       {SystemMaintenance, "SYSTEM_MAINTENANCE", ""},
	IllegalArgument:         {IllegalArgument, "ILLEGAL_ARGUMENT", ""},
	AuthenticationFail:      {AuthenticationFail, "AUTHENTICATION_FAIL", ""},
	AuthorizationFail:       {AuthorizationFail, "AUTHORIZATION_FAIL", ""},
	MQ404:                   {MQ404, "MQ_404", ""},
	FrequencyLimit:          {FrequencyLimit, "FREQUENCY_LIMIT", ""},
	SignatureVerifyFail:     {SignatureVerifyFail, "SIGNATURE_VERIFY_FAIL", ""},
	SignatureAppkeyNotexist: {SignatureAppkeyNotexist, "SIGNATURE_APPKEY_NOTEXIST", ""},
	SignatureAppkeyInvalid:  {SignatureAppkeyInvalid, "SIGNATURE_APPKEY_INVALID", ""},
	RiskControl:             {RiskControl, "RISK_CONTROL", ""},
	AreaRiskControl:         {AreaRiskControl, "AREA_RISK_CONTROL", ""},
	ReqFastFail:             {ReqFastFail, "REQ_FAST_FAIL", ""},
	OldVersion:              {OldVersion, "OLD_VERSION", ""},
	MarketClose:             {MarketClose, "MARKET_CLOSE", ""},
	IPLimit:                 {IPLimit, "IP_LIMIT", ""},
	SMSWrongAttemptTimes:    {SMSWrongAttemptTimes, "SMS_WRONG_ATTEMPT_TIMES", ""},
	MismatchCondition:       {MismatchCondition, "MISMATCH_CONDITION", ""},
	WalletMaintain:          {WalletMaintain, "WALLET_MAINTAIN", ""},
	WalletRickControl:       {WalletRickControl, "WALLET_RICK_CONTROL", ""},
	UserCancel:              {UserCancel, "USER_CANCEL", ""},
	UserNotQulified:         {UserNotQulified, "USER_NOT_QULIFIED", ""},
	DataHasModify:           {DataHasModify, "DATA_HAS_MODIFY", ""},
	APIKeyLimited:           {APIKeyLimited, "API_KEY_LIMITED", ""},
	APIKeyInvalid:           {APIKeyInvalid, "API_KEY_INVALID", ""},
	BlackBan:                {BlackBan, "BLACK_BAN", ""},
	SocialfeedPostBanned:    {SocialfeedPostBanned, "SOCIALFEED_POST_BANNED", ""},
	SocialfeedNotTrader:     {SocialfeedNotTrader, "SOCIALFEED_NOT_TRADER", ""},
	UserSuspectWool:         {UserSuspectWool, "USER_SUSPECT_WOOL", ""},
	InternalServerError:     {InternalServerError, "INTERNAL_SERVER_ERROR", ""},
	ServerBusy:              {ServerBusy, "SERVER_BUSY", ""},
	ServerDegrade:           {ServerDegrade, "SERVER_DEGRADE", ""},
	ServerLogicalErr:        {ServerLogicalErr, "SERVER_LOGICAL_ERR", ""},
	UserBasicKYC:            {UserBasicKYC, "USER_BASIC_KYC", ""},
	UserAdvancedKYC:         {UserAdvancedKYC, "USER_ADVANCED_KYC", ""},
	RiskControlTips:         {RiskControlTips, "RISK_CONTROL_TIPS", ""},
}
