package apperror

import (
	"fmt"
	"strconv"

	"github.com/lijizhong480/ark-go/core/response"
)

// Error is the Go equivalent of AppException. Message is the client-facing
// message; Error() preserves Java's `code:<number>:<message>` text.
type Error struct {
	Code        response.Code
	Message     string
	Attach      any
	ExtraParams []string
	Format      []string
	Cause       error
}

func New(code response.Code, message string) *Error { return &Error{Code: code, Message: message} }

func FromDefinition(d response.Definition) *Error { return New(d.Code, d.Description) }

func Parse(code, message string) (*Error, error) {
	n, err := strconv.Atoi(code)
	if err != nil {
		return nil, fmt.Errorf("invalid response code %q: %w", code, err)
	}
	return New(response.Code(n), message), nil
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("code:%d:%s", e.Code, e.Message)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
