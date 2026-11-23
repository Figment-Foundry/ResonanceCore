//go:build unix

package main

import (
	"os"
	"os/exec"
)

func spawnService(bus string) error {
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	p := exec.Command(exe, "--bus", bus, "run")
	configureDetached(p)
	if e = p.Start(); e != nil {
		return e
	}
	go p.Wait()
	return nil
}
