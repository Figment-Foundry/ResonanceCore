//go:build windows

package state

import (
	"golang.org/x/sys/windows"
	"strconv"
)

func ProcessIdentity(pid int) []string {
	if pid <= 0 {
		return nil
	}
	h, e := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if e != nil {
		return nil
	}
	defer windows.CloseHandle(h)
	status, e := windows.WaitForSingleObject(h, 0)
	if e != nil || status != uint32(windows.WAIT_TIMEOUT) {
		return nil
	}
	var creation, exit, kernel, user windows.Filetime
	if windows.GetProcessTimes(h, &creation, &exit, &kernel, &user) != nil {
		return nil
	}
	return []string{"windows", strconv.FormatUint(uint64(creation.HighDateTime)<<32|uint64(creation.LowDateTime), 10)}
}
