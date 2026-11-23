//go:build unix

package main

import (
	"os/exec"
	"syscall"
)

func configureDetached(p *exec.Cmd) { p.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }
