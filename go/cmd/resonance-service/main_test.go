package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestIsolatedServiceLifecycle(t *testing.T) {
	bus := filepath.Join(t.TempDir(), "bus.db")
	t.Setenv("RESONANCE_RUNTIME_DIR", filepath.Join(t.TempDir(), "run"))
	done := make(chan error, 1)
	go func() { done <- serve(bus) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := call(bus, "status"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := call(bus, "stop"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop")
	}
}
