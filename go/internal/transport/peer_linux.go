//go:build linux

package transport

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func VerifyPeer(c net.Conn, _ bool) error {
	u, ok := c.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("unix socket required")
	}
	raw, err := u.SyscallConn()
	if err != nil {
		return err
	}
	var cred *unix.Ucred
	var probe error
	err = raw.Control(func(fd uintptr) { cred, probe = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	if err != nil {
		return err
	}
	if probe != nil {
		return probe
	}
	if cred.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("peer uid mismatch")
	}
	return nil
}
