//go:build unix

package transport

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestListenPreservesExistingFileAndLiveEndpoint(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "service.sock")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if listener, err := Listen(path); err == nil {
		listener.Close()
		t.Fatal("replaced ordinary file")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "keep" {
		t.Fatal("deleted ordinary file")
	}
	os.Remove(path)
	l, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if second, err := Listen(path); err == nil {
		second.Close()
		t.Fatal("replaced live service")
	}
}

func TestPrivateUnixSocketVerifiesBothPeers(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "service.sock")
	l, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan error, 1)
	go func() {
		c, e := l.Accept()
		if e == nil {
			defer c.Close()
			e = VerifyPeer(c, true)
		}
		done <- e
	}()
	c, err := Dial(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
