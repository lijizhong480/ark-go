package xxl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type jobLogContext struct{}
type jobLogRef struct {
	executor *Executor
	key      logKey
}

// AppendLog writes a handler log line to the current XXL job log.
func AppendLog(ctx context.Context, line string) {
	if ref, ok := ctx.Value(jobLogContext{}).(jobLogRef); ok {
		ref.executor.appendJobLog(ref.key, line)
	}
}

// SetLogDir enables Java-compatible date/logId file layout and survives executor restarts.
// Call it before Start. Script sources are cached under logDir/gluesource.
func (e *Executor) SetLogDir(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("xxl: log directory required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started || e.starting || e.closed {
		return errors.New("xxl: set log directory before start")
	}
	if err := os.MkdirAll(filepath.Join(path, "gluesource"), 0o700); err != nil {
		return err
	}
	e.logDir = path
	return nil
}

func logFile(root string, key logKey) string {
	return filepath.Join(root, time.UnixMilli(key.date).In(time.Local).Format("2006-01-02"), fmt.Sprintf("%d.log", key.id))
}

func (e *Executor) appendJobLog(key logKey, line string) {
	e.mu.Lock()
	if state := e.logs[key]; state != nil {
		state.lines = append(state.lines, line)
	}
	root := e.logDir
	e.mu.Unlock()
	if root == "" {
		return
	}
	path := logFile(root, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	_, _ = file.WriteString(line + "\r\n")
	_ = file.Close()
}

func (e *Executor) readFileLog(param LogParam) (LogResult, bool) {
	e.mu.Lock()
	root := e.logDir
	e.mu.Unlock()
	if root == "" {
		return LogResult{}, false
	}
	file, err := os.Open(logFile(root, logKey{param.LogID, param.LogDateTime}))
	if err != nil {
		return LogResult{FromLineNum: param.FromLineNum, ToLineNum: 0, LogContent: "readLog fail, logFile not exists", IsEnd: true}, true
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	lines := []string{}
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	start := param.FromLineNum
	if start < 1 {
		start = 1
	}
	content := ""
	if start <= len(lines) {
		content = strings.Join(lines[start-1:], "\n") + "\n"
	}
	return LogResult{FromLineNum: param.FromLineNum, ToLineNum: len(lines), LogContent: content, IsEnd: false}, true
}
