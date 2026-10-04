package web

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/lijizhong480/ark-go/core/response"
	"github.com/golang-jwt/jwt/v4"
)

type GatewayOptions struct {
	Patterns []string
	Ignore   []string
	// Secret is the Base64-encoded HMAC key expected by JJWT setSigningKey(String).
	Secret string
}

// InnerGateway checks the bare Inner-Authentication JWT on /manage/* by
// default. It accepts only HMAC signatures and validates standard time claims.
func InnerGateway(options GatewayOptions) (Middleware, error) {
	if options.Secret == "" {
		return nil, errors.New("web: empty gateway JWT secret")
	}
	secret, err := base64.StdEncoding.DecodeString(options.Secret)
	if err != nil || len(secret) == 0 {
		return nil, errors.New("web: gateway JWT secret must be Base64")
	}
	patterns := options.Patterns
	if len(patterns) == 0 {
		patterns = []string{"/manage/*"}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !matchAny(patterns, r.URL.Path) || matchAny(options.Ignore, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			token := strings.TrimSpace(r.Header.Get("Inner-Authentication"))
			if token == "" {
				_ = WriteResponse(w, http.StatusUnauthorized, response.Failure(response.Unauthorized, "innerAuthentication is empty"))
				return
			}
			parsed, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, errors.New("unexpected JWT signing method")
				}
				return secret, nil
			})
			if err != nil || !parsed.Valid {
				_ = WriteResponse(w, http.StatusUnauthorized, response.Failure(response.Unauthorized, "innerAuthentication jwt parse fail"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}
