package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

func spawnService(bus string) error {
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	command := []string{exe, "--bus", bus}
	if runtime := os.Getenv("RESONANCE_RUNTIME_DIR"); runtime != "" {
		command = append(command, "--runtime-dir", runtime)
	}
	command = append(command, "run")
	quoted := []string{}
	for _, s := range command {
		quoted = append(quoted, syscall.EscapeArg(s))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := exec.CommandContext(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-Command", "$r=Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=$env:RESONANCE_SERVICE_COMMAND}; if($r.ReturnValue -ne 0){exit 1}")
	p.Env = append(os.Environ(), "RESONANCE_SERVICE_COMMAND="+strings.Join(quoted, " "))
	p.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if p.Run() != nil {
		return errors.New("service_unavailable")
	}
	return nil
}
