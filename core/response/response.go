package response

import (
	"encoding/json"
	"time"
)

// Response uses the JSON field names of Java ServerResponse. Optional fields
// are omitted when absent; timestamp is Unix milliseconds.
type Response[T any] struct {
	Code      Code    `json:"code"`
	ID        *string `json:"id,omitempty"`
	Msg       *string `json:"msg,omitempty"`
	Timestamp int64   `json:"timestamp"`
	DataID    *int64  `json:"dataId,omitempty"`
	DataType  *string `json:"dataType,omitempty"`
	Data      *T      `json:"data,omitempty"`
}

func New[T any](code Code, message string) Response[T] {
	return Response[T]{Code: code, Msg: &message, Timestamp: time.Now().UnixMilli()}
}

// NewCode matches Java's constructor that leaves msg null.
func NewCode[T any](code Code) Response[T] {
	return Response[T]{Code: code, Timestamp: time.Now().UnixMilli()}
}

func NewWithData[T any](code Code, message string, data T) Response[T] {
	r := New[T](code, message)
	r.Data = &data
	return r
}

func OK[T any](data T) Response[T] {
	r := NewCode[T](Success)
	r.Data = &data
	return r
}

func (r *Response[T]) SetID(id string)             { r.ID = &id }
func (r *Response[T]) SetDataType(dataType string) { r.DataType = &dataType }

func Failure(code Code, message string) Response[any] { return New[any](code, message) }

func FromDefinition[T any](d Definition) Response[T] { return New[T](d.Code, d.Description) }

func (r Response[T]) IsSuccess() bool { return r.Code == Success }

// MarshalJSON keeps a nil data pointer absent, including for the zero-value
// response. It uses the ordinary encoder so no protocol-specific escaping is lost.
func (r Response[T]) MarshalJSON() ([]byte, error) {
	type wire Response[T]
	return json.Marshal(wire(r))
}
