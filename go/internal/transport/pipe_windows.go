//go:build windows

package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"time"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func Endpoint(bus string) (string, error) {
	h := sha256.Sum256([]byte(bus))
	return `\\.\pipe\resonance-service-` + hex.EncodeToString(h[:8]), nil
}

var (
	kernel32     = windows.NewLazySystemDLL("kernel32.dll")
	getClientPID = kernel32.NewProc("GetNamedPipeClientProcessId")
	getServerPID = kernel32.NewProc("GetNamedPipeServerProcessId")
)

type handleConn interface{ Fd() uintptr }

func currentSID() (string, error) {
	t, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return "", err
	}
	defer t.Close()
	u, err := t.GetTokenUser()
	if err != nil {
		return "", err
	}
	return u.User.Sid.String(), nil
}

func processSID(pid uint32) (string, error) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(h)
	var t windows.Token
	if err = windows.OpenProcessToken(h, windows.TOKEN_QUERY, &t); err != nil {
		return "", err
	}
	defer t.Close()
	u, err := t.GetTokenUser()
	if err != nil {
		return "", err
	}
	return u.User.Sid.String(), nil
}

func peerPID(c net.Conn, server bool) (uint32, error) {
	h, ok := c.(handleConn)
	if !ok {
		return 0, fmt.Errorf("pipe handle unavailable")
	}
	var pid uint32
	p := getServerPID
	if server {
		p = getClientPID
	}
	okCall, _, err := p.Call(h.Fd(), uintptr(unsafe.Pointer(&pid)))
	if okCall == 0 {
		return 0, err
	}
	return pid, nil
}

func PeerProcessID(c net.Conn, server bool) (uint32, error) {
	return peerPID(c, server)
}

func VerifyPeer(c net.Conn, server bool) error {
	want, err := currentSID()
	if err != nil {
		return err
	}
	pid, err := peerPID(c, server)
	if err != nil {
		return err
	}
	got, err := processSID(pid)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("peer SID mismatch")
	}
	return nil
}

func Listen(path string) (net.Listener, error) {
	sid, err := currentSID()
	if err != nil {
		return nil, err
	}
	return winio.ListenPipe(path, &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;" + sid + ")", MessageMode: true, InputBufferSize: 65536, OutputBufferSize: 1048576})
}

func Dial(path string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := winio.DialPipeContext(ctx, path)
	if err != nil {
		return nil, err
	}
	if err = VerifyPeer(c, false); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
