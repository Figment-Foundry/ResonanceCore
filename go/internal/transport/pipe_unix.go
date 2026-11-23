//go:build unix

package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func privateDirectory(path string, create bool) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) && create {
		if err = os.MkdirAll(path, 0o700); err != nil {
			return "", err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || stat.Uid != uint32(os.Getuid()) || info.Mode().Perm()&0o077 != 0 {
		return "", os.ErrPermission
	}
	return path, nil
}

func Endpoint(bus string) (string, error) {
	root := os.Getenv("RESONANCE_RUNTIME_DIR")
	if root == "" {
		root = os.Getenv("XDG_RUNTIME_DIR")
	}
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		root = filepath.Join(home, ".local", "state", "resonancecore", "run")
	}
	root, err := privateDirectory(root, true)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(bus))
	return filepath.Join(root, "service-"+hex.EncodeToString(h[:8])+".sock"), nil
}

func Listen(path string) (net.Listener, error) {
	if _, err := privateDirectory(filepath.Dir(path), false); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, os.ErrExist
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	old := unix.Umask(0o077)
	l, err := net.Listen("unix", path)
	unix.Umask(old)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

func Dial(path string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	if err = VerifyPeer(c, false); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
