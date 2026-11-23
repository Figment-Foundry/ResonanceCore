//go:build windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestStopWaitsForDatabaseRelease(t *testing.T) {
	root := t.TempDir()
	bus := filepath.Join(root, "bus.db")
	runtimeDir := filepath.Join(root, "run")
	command := exec.Command(os.Args[0], "-test.run=TestServiceProcessHelper")
	command.Env = append(os.Environ(),
		"RESONANCE_TEST_SERVICE=1",
		"RESONANCE_TEST_BUS="+bus,
		"RESONANCE_RUNTIME_DIR="+runtimeDir)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := request(bus, stateMap("ping"), 200*time.Millisecond); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("helper service did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := stopService(bus); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(bus); err != nil {
		t.Fatal("stop returned before SQLite released the bus:", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
}

func stateMap(operation string) map[string]any { return map[string]any{"op": operation} }

func TestServiceProcessHelper(t *testing.T) {
	if os.Getenv("RESONANCE_TEST_SERVICE") != "1" {
		return
	}
	if err := serve(os.Getenv("RESONANCE_TEST_BUS")); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}
