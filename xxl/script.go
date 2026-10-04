package xxl

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type scriptSpec struct{ command, suffix string }

var scriptTypes = map[string]scriptSpec{
	"GLUE_SHELL":      {"bash", ".sh"},
	"GLUE_PYTHON":     {"python", ".py"},
	"GLUE_PHP":        {"php", ".php"},
	"GLUE_NODEJS":     {"node", ".js"},
	"GLUE_POWERSHELL": {"powershell", ".ps1"},
}

func (e *Executor) scriptHandler(trigger TriggerParam) Handler {
	return func(ctx context.Context, param string) ReturnT[string] {
		spec := scriptTypes[trigger.GlueType]
		e.mu.Lock()
		root := e.logDir
		if root == "" {
			if e.scriptDir == "" {
				dir, err := os.MkdirTemp("", "ark-go-xxl-glue-")
				if err != nil {
					e.mu.Unlock()
					return failure[string](err.Error())
				}
				e.scriptDir = dir
			}
			root = e.scriptDir
		}
		e.mu.Unlock()
		sourceDir := filepath.Join(root, "gluesource")
		if err := os.MkdirAll(sourceDir, 0o700); err != nil {
			return failure[string](err.Error())
		}
		path := filepath.Join(sourceDir, fmt.Sprintf("%d_%d%s", trigger.JobID, trigger.GlueUpdatetime, spec.suffix))
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			_, err = file.WriteString(trigger.GlueSource)
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil && !os.IsExist(err) {
			return failure[string](err.Error())
		}
		key := logKey{trigger.LogID, trigger.LogDateTime}
		e.appendJobLog(key, "----------- script file: "+path+" -----------")
		cmd := exec.CommandContext(ctx, spec.command, path, param, strconv.Itoa(trigger.BroadcastIndex), strconv.Itoa(trigger.BroadcastTotal))
		output, runErr := cmd.CombinedOutput()
		if len(output) > 0 {
			e.appendJobLog(key, strings.TrimRight(string(output), "\r\n"))
		}
		if runErr == nil {
			return success("")
		}
		if exit, ok := runErr.(*exec.ExitError); ok {
			return failure[string](fmt.Sprintf("script exit value(%d) is failed", exit.ExitCode()))
		}
		return failure[string](runErr.Error())
	}
}

func (e *Executor) cleanScriptDir() {
	e.mu.Lock()
	dir := e.scriptDir
	e.scriptDir = ""
	e.mu.Unlock()
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}
