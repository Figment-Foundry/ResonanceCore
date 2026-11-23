//go:build !windows

package main

import (
	"encoding/json"
	"os"
	"resonancecore/internal/state"
	"time"
)

func stopService(bus string) error {
	result, err := request(bus, state.Map{"op": "stop"}, time.Second)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
