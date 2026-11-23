package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"resonancecore/internal/state"
	"resonancecore/internal/transport"
	"strings"
	"sync"
	"syscall"
	"time"
)

func request(bus string, message state.Map, timeout time.Duration) (state.Map, error) {
	return requestConnected(bus, message, timeout, nil)
}

func requestConnected(bus string, message state.Map, timeout time.Duration, connected func(net.Conn) error) (state.Map, error) {
	path, e := transport.Endpoint(bus)
	if e != nil {
		return nil, e
	}
	payload, e := json.Marshal(message)
	if e != nil || len(payload)+1 > 65536 {
		return nil, errors.New("request_limit")
	}
	c, e := transport.Dial(path, timeout)
	if e != nil {
		return nil, errors.New("service_unavailable")
	}
	defer c.Close()
	if connected != nil {
		if e = connected(c); e != nil {
			return nil, e
		}
	}
	c.SetDeadline(time.Now().Add(timeout))
	if _, e = c.Write(append(payload, '\n')); e != nil {
		return nil, errors.New("service_unavailable")
	}
	line, e := bufio.NewReader(io.LimitReader(c, 1048577)).ReadBytes('\n')
	if e != nil || len(line) > 1048576 {
		return nil, errors.New("response_limit")
	}
	var response struct {
		OK     bool      `json:"ok"`
		Error  string    `json:"error"`
		Result state.Map `json:"result"`
	}
	if json.Unmarshal(line, &response) != nil {
		return nil, errors.New("invalid_response")
	}
	if !response.OK {
		return nil, errors.New(response.Error)
	}
	return response.Result, nil
}
func call(bus, op string) error {
	result, e := request(bus, state.Map{"op": op}, time.Second)
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func serve(bus string) error {
	path, e := transport.Endpoint(bus)
	if e != nil {
		return e
	}
	listener, e := transport.Listen(path)
	if e != nil {
		return e
	}
	defer listener.Close()
	service, e := state.NewService(bus)
	if e != nil {
		return e
	}
	defer service.Store.Close()
	stop := make(chan struct{})
	var once sync.Once
	shutdown := func() { once.Do(func() { close(stop); listener.Close() }) }
	defer shutdown()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	var tasks sync.WaitGroup
	defer tasks.Wait()
	tasks.Add(1)
	go func() {
		defer tasks.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-signals:
				shutdown()
				return
			case <-ticker.C:
				service.Tick()
			}
		}
	}()
	tasks.Add(1)
	go func() {
		defer tasks.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				service.PumpT3()
			}
		}
	}()

	for {
		c, e := listener.Accept()
		if e != nil {
			select {
			case <-stop:
				return nil
			default:
				return e
			}
		}
		tasks.Add(1)
		go func(c net.Conn) {
			defer tasks.Done()
			defer c.Close()
			c.SetDeadline(time.Now().Add(10 * time.Second))
			response := state.Map{"ok": false, "error": "invalid_request"}
			var message state.Map
			if transport.VerifyPeer(c, true) != nil {
				response["error"] = "peer_refused"
			} else {
				line, e := bufio.NewReader(io.LimitReader(c, 65537)).ReadBytes('\n')
				if e != nil || len(line) > 65536 {
					response["error"] = "request_limit"
				} else if json.Unmarshal(line, &message) == nil && message != nil {
					result, e := service.Dispatch(message)
					if e != nil {
						response["error"] = state.ErrorCode(e)
					} else {
						response = state.Map{"ok": true, "result": result}
					}
				}
			}
			b, _ := json.Marshal(response)
			if len(b)+1 > 1048576 {
				b = []byte(`{"ok":false,"error":"response_limit"}`)
			}
			c.Write(append(b, '\n'))
			if response["ok"] == true && message["op"] == "stop" {
				shutdown()
			}
		}(c)
	}
}
func exactBus(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("exact_bus_path_required")
	}
	parent := filepath.Dir(path)
	for {
		if _, e := os.Lstat(parent); e == nil {
			resolved, e := filepath.EvalSymlinks(parent)
			if e != nil || resolved != parent {
				return "", errors.New("exact_bus_path_required")
			}
			break
		}
		next := filepath.Dir(parent)
		if next == parent {
			break
		}
		parent = next
	}
	return path, nil
}
func start(bus string) error {
	if _, e := request(bus, state.Map{"op": "ping"}, 200*time.Millisecond); e == nil {
		return nil
	}
	if e := spawnService(bus); e != nil {
		return e
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := request(bus, state.Map{"op": "ping"}, 200*time.Millisecond); e == nil {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("service_unavailable")
}
func main() {
	if e := mainRun(); e != nil {
		fmt.Fprintln(os.Stderr, "error: "+state.ErrorCode(e))
		os.Exit(1)
	}
}
func mainRun() error {
	args := os.Args[1:]
	bus := ""
	runtimeDir := ""
	for i := 0; i < len(args); {
		if (args[i] == "--bus" || args[i] == "--runtime-dir") && i+1 < len(args) {
			if args[i] == "--bus" {
				bus = args[i+1]
			} else {
				runtimeDir = args[i+1]
			}
			args = append(args[:i], args[i+2:]...)
		} else {
			i++
		}
	}
	if runtimeDir != "" {
		os.Setenv("RESONANCE_RUNTIME_DIR", runtimeDir)
	}
	if len(args) == 0 || args[0] == "--help" {
		fmt.Println("resonance-service --bus ABSOLUTE_DB {run|start|status|stop|request|send|drain|list}")
		return nil
	}
	if bus == "" {
		bus = os.Getenv("AGENT_MSG_DB")
	}
	if bus == "" {
		home, _ := os.UserHomeDir()
		bus = filepath.Join(home, ".local", "state", "resonancecore", "bus.db")
	}
	var e error
	bus, e = exactBus(bus)
	if e != nil {
		return e
	}
	op := args[0]
	if op == "run" {
		return serve(bus)
	}
	if op == "start" {
		if e = start(bus); e != nil {
			return e
		}
		return call(bus, "ping")
	}
	if op == "stop" {
		return stopService(bus)
	}
	if op == "status" {
		return call(bus, op)
	}
	message := state.Map{"op": op}
	if op == "request" {
		line, e := io.ReadAll(io.LimitReader(os.Stdin, 65537))
		if e != nil {
			return e
		}
		if len(line) > 65536 {
			return errors.New("request_limit")
		}
		if json.Unmarshal(line, &message) != nil || message == nil {
			return errors.New("invalid_request")
		}
	} else {
		flags := flag.NewFlagSet(op, flag.ContinueOnError)
		var sender, recipient, session, typ string
		var reply int64
		flags.StringVar(&sender, "from", os.Getenv("AGENT_MSG_ID"), "sender")
		flags.StringVar(&recipient, "to", "", "recipient")
		flags.StringVar(&recipient, "as", os.Getenv("AGENT_MSG_ID"), "inbox recipient")
		flags.StringVar(&session, "session", os.Getenv("AGENT_MSG_SESSION"), "exact session")
		flags.StringVar(&typ, "type", "notify", "message type")
		flags.Int64Var(&reply, "reply-to", 0, "original message ID")
		if e = flags.Parse(args[1:]); e != nil {
			return e
		}
		if op != "send" && op != "drain" && op != "list" {
			return errors.New("unsupported_operation")
		}
		message["sender"] = sender
		message["recipient"] = recipient
		message["session"] = session
		message["type"] = typ
		if op == "send" {
			message["body"] = strings.Join(flags.Args(), " ")
			if reply != 0 {
				message["reply_to"] = reply
			}
		}
	}
	if _, e = request(bus, state.Map{"op": "ping"}, 200*time.Millisecond); e != nil {
		if op == "request" && message["event"] == "SessionEnd" {
			return e
		}
		if e = start(bus); e != nil {
			return e
		}
	}
	result, e := request(bus, message, 10*time.Second)
	if e != nil {
		return e
	}
	if e = json.NewEncoder(os.Stdout).Encode(result); e != nil {
		return e
	}
	if op == "drain" {
		ids := []any{}
		if messages, ok := result["messages"].([]any); ok {
			for _, value := range messages {
				if m, ok := value.(map[string]any); ok {
					ids = append(ids, m["id"])
				}
			}
		}
		_, e = request(bus, state.Map{"op": "ack", "recipient": message["recipient"], "session": message["session"], "message_ids": ids}, 10*time.Second)
		return e
	}
	return nil
}
