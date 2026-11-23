//go:build windows

package transport

import (
	"fmt"
	"testing"
	"time"
)

func TestOwnerPipeVerifiesBothPeers(t *testing.T) {
	path := fmt.Sprintf(`\\.\pipe\resonance-transport-test-%d`, time.Now().UnixNano())
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
	c, err := Dial(path, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
