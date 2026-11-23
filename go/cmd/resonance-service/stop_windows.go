//go:build windows

package main

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"resonancecore/internal/state"
	"resonancecore/internal/transport"
	"time"

	"golang.org/x/sys/windows"
)

func stopService(bus string) error {
	var wait func(time.Duration) bool
	result, err := requestConnected(bus, state.Map{"op": "stop"}, time.Second, func(connection net.Conn) error {
		pid, err := transport.PeerProcessID(connection, false)
		if err != nil {
			return err
		}
		wait, err = processExitWaiter(pid)
		return err
	})
	if err != nil {
		return err
	}
	defer wait(0)
	if !wait(5 * time.Second) {
		return errors.New("service_unavailable")
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

// processExitWaiter opens the process before requesting shutdown, so PID reuse
// cannot make stop report completion for a different service incarnation.
func processExitWaiter(pid uint32) (func(time.Duration) bool, error) {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, pid)
	if err != nil {
		return nil, err
	}
	closed := false
	return func(timeout time.Duration) bool {
		if timeout <= 0 {
			if !closed {
				windows.CloseHandle(handle)
				closed = true
			}
			return true
		}
		milliseconds := uint32(timeout / time.Millisecond)
		result, err := windows.WaitForSingleObject(handle, milliseconds)
		return err == nil && result == windows.WAIT_OBJECT_0
	}, nil
}
