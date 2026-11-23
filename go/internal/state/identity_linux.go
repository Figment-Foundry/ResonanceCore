package state

import (
	"fmt"
	"os"
	"strings"
)

func ProcessIdentity(pid int) []string {
	if pid <= 0 {
		return nil
	}
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return nil
	}
	end := strings.LastIndex(string(b), ")")
	if end < 0 {
		return nil
	}
	fields := strings.Fields(string(b)[end+1:])
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
		return nil
	}
	boot, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return nil
	}
	return []string{strings.TrimSpace(string(boot)), fields[19]}
}
