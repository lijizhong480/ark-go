package xxl

const (
	SuccessCode = 200
	FailCode    = 500
)

type ReturnT[T any] struct {
	Code    int    `json:"code"`
	Msg     string `json:"msg,omitempty"`
	Content T      `json:"content,omitempty"`
}

func success[T any](content T) ReturnT[T]      { return ReturnT[T]{Code: SuccessCode, Content: content} }
func failure[T any](message string) ReturnT[T] { return ReturnT[T]{Code: FailCode, Msg: message} }

type TriggerParam struct {
	JobID                 int    `json:"jobId"`
	ExecutorHandler       string `json:"executorHandler"`
	ExecutorParams        string `json:"executorParams"`
	ExecutorBlockStrategy string `json:"executorBlockStrategy"`
	ExecutorTimeout       int    `json:"executorTimeout"`
	LogID                 int64  `json:"logId"`
	LogDateTime           int64  `json:"logDateTime"`
	GlueType              string `json:"glueType"`
	GlueSource            string `json:"glueSource"`
	GlueUpdatetime        int64  `json:"glueUpdatetime"`
	BroadcastIndex        int    `json:"broadcastIndex"`
	BroadcastTotal        int    `json:"broadcastTotal"`
}
type RegistryParam struct {
	RegistryGroup string `json:"registryGroup"`
	RegistryKey   string `json:"registryKey"`
	RegistryValue string `json:"registryValue"`
}
type CallbackParam struct {
	LogID         int64           `json:"logId"`
	LogDateTim    int64           `json:"logDateTim"`
	ExecuteResult ReturnT[string] `json:"executeResult"`
}
type LogResult struct {
	FromLineNum int    `json:"fromLineNum"`
	ToLineNum   int    `json:"toLineNum"`
	LogContent  string `json:"logContent"`
	IsEnd       bool   `json:"isEnd"`
}
type LogParam struct {
	LogDateTime int64 `json:"logDateTime"`
	LogID       int64 `json:"logId"`
	FromLineNum int   `json:"fromLineNum"`
}
type JobIDParam struct {
	JobID int `json:"jobId"`
}
