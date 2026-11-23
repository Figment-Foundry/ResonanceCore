//go:build unix && !linux

package transport

import (
	"fmt"
	"net"
)

func VerifyPeer(net.Conn, bool) error {
	return fmt.Errorf("peer credentials unsupported on this Unix platform")
}
