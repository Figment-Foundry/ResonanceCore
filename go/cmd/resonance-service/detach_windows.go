//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

func configureDetached(p *exec.Cmd) {
	p.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x208}
}
