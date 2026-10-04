package xxl

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	hessian "github.com/apache/dubbo-go-hessian2"
)

const rpcRequestClass = "com.xxl.rpc.remoting.net.params.XxlRpcRequest"
const rpcExecutorClass = "com.xxl.job.core.biz.ExecutorBiz"

type wireResponse struct {
	RequestID string  `hessian:"requestId"`
	ErrorMsg  *string `hessian:"errorMsg"`
	Result    any     `hessian:"result"`
}

func (wireResponse) JavaClassName() string { return "com.xxl.rpc.remoting.net.params.XxlRpcResponse" }

type wireReturn struct {
	Code    int32   `hessian:"code"`
	Msg     *string `hessian:"msg"`
	Content any     `hessian:"content"`
}

func (wireReturn) JavaClassName() string { return "com.xxl.job.core.biz.model.ReturnT" }

type wireLog struct {
	FromLineNum int32  `hessian:"fromLineNum"`
	ToLineNum   int32  `hessian:"toLineNum"`
	LogContent  string `hessian:"logContent"`
	IsEnd       bool   `hessian:"isEnd"`
}

func (wireLog) JavaClassName() string { return "com.xxl.job.core.biz.model.LogResult" }
func init()                           { hessian.RegisterPOJOs(&wireResponse{}, &wireReturn{}, &wireLog{}) }

// RPCHandler implements the Hessian2 XXL-RPC executor transport used by
// xxl-job-core 2.1.2. Mount it at / (the Java Netty server accepts any URI).
func (e *Executor) RPCHandler(accessToken string) http.Handler {
	return e.rpcHandler(accessToken, time.Now)
}
func (e *Executor) rpcHandler(accessToken string, now func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
		if err != nil || len(raw) == 0 || len(raw) >= 2<<20 {
			http.Error(w, "invalid RPC body", http.StatusBadRequest)
			return
		}
		decoded, err := hessian.NewDecoder(raw).Decode()
		if err != nil {
			http.Error(w, "invalid Hessian request", http.StatusBadRequest)
			return
		}
		request, ok := decoded.(map[string]any)
		if !ok || request["_class"] != rpcRequestClass {
			http.Error(w, "invalid RPC request class", http.StatusBadRequest)
			return
		}
		requestID, _ := request["requestId"].(string)
		if strings.EqualFold(requestID, "BEAT_PING_PONG") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		reply := wireResponse{RequestID: requestID}
		if request["className"] != rpcExecutorClass || request["version"] != "" {
			message := "The serviceKey[" + fmt.Sprint(request["className"]) + "] not found."
			reply.ErrorMsg = &message
		} else if difference := now().UnixMilli() - asInt64(request["createMillisTime"]); difference > 180000 {
			message := "The timestamp difference between admin and executor exceeds the limit."
			reply.ErrorMsg = &message
		} else if accessToken != "" && request["accessToken"] != accessToken {
			message := "The access token is wrong."
			reply.ErrorMsg = &message
		} else {
			reply.Result, err = e.invokeRPC(request)
			if err != nil {
				message := err.Error()
				reply.ErrorMsg = &message
			}
		}
		encoded := hessian.NewEncoder()
		if err := encoded.Encode(&reply); err != nil {
			http.Error(w, "Hessian response failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(encoded.Buffer())
	})
}
func (e *Executor) invokeRPC(request map[string]any) (any, error) {
	method, _ := request["methodName"].(string)
	parameters := sliceValues(request["parameters"])
	switch method {
	case "beat":
		return wireString(e.Beat()), nil
	case "idleBeat":
		if len(parameters) != 1 {
			return nil, fmt.Errorf("xxl: idleBeat needs jobId")
		}
		return wireString(e.IdleBeat(int(asInt64(parameters[0])))), nil
	case "kill":
		if len(parameters) != 1 {
			return nil, fmt.Errorf("xxl: kill needs jobId")
		}
		return wireString(e.Kill(int(asInt64(parameters[0])))), nil
	case "run":
		if len(parameters) != 1 {
			return nil, fmt.Errorf("xxl: run needs TriggerParam")
		}
		mapped, ok := parameters[0].(map[string]any)
		if !ok || mapped["_class"] != "com.xxl.job.core.biz.model.TriggerParam" {
			return nil, fmt.Errorf("xxl: invalid TriggerParam")
		}
		data, err := json.Marshal(mapped)
		if err != nil {
			return nil, err
		}
		var trigger TriggerParam
		if err := json.Unmarshal(data, &trigger); err != nil {
			return nil, err
		}
		return wireString(e.Run(trigger)), nil
	case "log":
		if len(parameters) != 3 {
			return nil, fmt.Errorf("xxl: log needs date, id and line")
		}
		result := e.Log(LogParam{LogDateTime: asInt64(parameters[0]), LogID: asInt64(parameters[1]), FromLineNum: int(asInt64(parameters[2]))})
		return &wireReturn{Code: int32(result.Code), Msg: nullable(result.Msg), Content: &wireLog{FromLineNum: int32(result.Content.FromLineNum), ToLineNum: int32(result.Content.ToLineNum), LogContent: result.Content.LogContent, IsEnd: result.Content.IsEnd}}, nil
	default:
		return nil, fmt.Errorf("xxl: unknown method %q", method)
	}
}
func wireString(value ReturnT[string]) *wireReturn {
	return &wireReturn{Code: int32(value.Code), Msg: nullable(value.Msg), Content: nullable(value.Content)}
}
func nullable(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func sliceValues(value any) []any {
	v := reflect.ValueOf(value)
	if !v.IsValid() || v.Kind() != reflect.Slice {
		return nil
	}
	out := make([]any, v.Len())
	for i := range out {
		out[i] = v.Index(i).Interface()
	}
	return out
}
func asInt64(value any) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int32:
		return int64(number)
	case int64:
		return number
	case float64:
		return int64(number)
	case json.Number:
		parsed, _ := number.Int64()
		return parsed
	case string:
		parsed, _ := strconv.ParseInt(number, 10, 64)
		return parsed
	default:
		return 0
	}
}
