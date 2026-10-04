package xxl

import (
	"encoding/json"
	"net/http"
)

// JSONHandler implements the JSON executor endpoints used by newer XXL-JOB
// releases. xxl-job-core 2.1.2 uses Hessian-serialized XXL-RPC instead;
// JSONHandler is not a drop-in endpoint for that version.
func (e *Executor) JSONHandler(accessToken string) http.Handler {
	mux := http.NewServeMux()
	handle := func(path string, fn func(*http.Request) any) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if accessToken != "" && r.Header.Get("XXL-JOB-ACCESS-TOKEN") != accessToken {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json;charset=UTF-8")
			_ = json.NewEncoder(w).Encode(fn(r))
		})
	}
	handle("/beat", func(*http.Request) any { return e.Beat() })
	handle("/idleBeat", func(r *http.Request) any {
		var p JobIDParam
		if err := decode(r, &p); err != nil {
			return failure[string](err.Error())
		}
		return e.IdleBeat(p.JobID)
	})
	handle("/kill", func(r *http.Request) any {
		var p JobIDParam
		if err := decode(r, &p); err != nil {
			return failure[string](err.Error())
		}
		return e.Kill(p.JobID)
	})
	handle("/run", func(r *http.Request) any {
		var p TriggerParam
		if err := decode(r, &p); err != nil {
			return failure[string](err.Error())
		}
		return e.Run(p)
	})
	handle("/log", func(r *http.Request) any {
		var p LogParam
		if err := decode(r, &p); err != nil {
			return failure[LogResult](err.Error())
		}
		return e.Log(p)
	})
	return mux
}
func decode(r *http.Request, target any) error {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	return json.NewDecoder(r.Body).Decode(target)
}
