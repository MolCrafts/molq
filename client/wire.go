// Package client implements the public RPC wire shared by Go clients.
// It has no registry, Scheduler, Transport or serve dependencies.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

const maxFrame = 1048576

// Options selects an explicit endpoint or a temporary stdio serve process.
type Options struct {
	Endpoint, Token, Registry, Config, Executable string
	Timeout                                       time.Duration
}

// Wire is one serialized RPC session. Failed sessions never reconnect implicitly.
type Wire struct {
	options  Options
	gate     chan struct{}
	process  *exec.Cmd
	in       io.WriteCloser
	stdout   io.ReadCloser
	out      *bufio.Reader
	waitOnce sync.Once
	waitDone chan struct{}
	waitErr  error
	sequence int
	hello    bool
	failed   bool
}

// New constructs a lazy wire without starting a process or accessing storage.
func New(options Options) *Wire {
	if options.Timeout <= 0 {
		options.Timeout = 130 * time.Second
	}
	return &Wire{options: options, gate: make(chan struct{}, 1), waitDone: make(chan struct{})}
}

// Call sends one public request with a finite deadline and no automatic resend.
func (c *Wire) Call(method string, params map[string]any) (any, error) {
	return c.CallContext(context.Background(), method, params)
}

// CallContext honors the caller's deadline, including stdio reads and writes.
func (c *Wire) CallContext(ctx context.Context, method string, params map[string]any) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, connectionError(false)
	}
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return nil, connectionError(false)
	}
	if c.failed {
		return nil, protocol.Fail("RUNTIME_UNAVAILABLE", "RPC session is closed", nil)
	}
	if method != "molq.hello" && !c.hello {
		hello, err := c.request(ctx, "molq.hello", map[string]any{})
		if err != nil {
			return nil, err
		}
		if err := c.verifyHello(hello); err != nil {
			return nil, err
		}
	}
	result, err := c.request(ctx, method, params)
	if err == nil && method == "molq.hello" {
		err = c.verifyHello(result)
	}
	return result, err
}

func (c *Wire) verifyHello(value any) error {
	hello, _ := value.(map[string]any)
	registryID, ok := hello["registry_id"].(string)
	if !ok || registryID == "" {
		return protocol.Fail("RUNTIME_UNAVAILABLE", "Invalid RPC handshake", nil)
	}
	c.hello = true
	return nil
}

func (c *Wire) start() error {
	if c.process != nil {
		return nil
	}
	executable := c.options.Executable
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return err
		}
	}
	args := []string{"serve", "--stdio"}
	if c.options.Config != "" {
		args = append(args, "--config", c.options.Config)
	}
	if c.options.Registry != "" {
		args = append(args, "--registry", c.options.Registry)
	}
	cmd := exec.Command(executable, args...)
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		in.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		in.Close()
		out.Close()
		return err
	}
	c.process, c.in, c.stdout, c.out = cmd, in, out, bufio.NewReader(out)
	return nil
}

func (c *Wire) request(ctx context.Context, method string, params map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, connectionError(false)
	}
	c.sequence++
	id := c.sequence
	payload, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil || len(payload) > maxFrame {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid or oversized RPC frame", nil)
	}
	mutation := isMutation(method)
	var frame []byte
	if c.options.Endpoint != "" {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.options.Endpoint, "/")+"/rpc", bytes.NewReader(payload))
		if err != nil {
			return nil, protocol.Fail("INVALID_INPUT", "Invalid RPC endpoint", nil)
		}
		request.Header.Set("Authorization", "Bearer "+c.options.Token)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, connectionError(mutation)
		}
		defer response.Body.Close()
		frame, err = io.ReadAll(io.LimitReader(response.Body, maxFrame+1))
		if err != nil {
			return nil, connectionError(mutation)
		}
	} else {
		if err := c.start(); err != nil {
			return nil, connectionError(false)
		}
		type answer struct {
			frame []byte
			err   error
		}
		ready := make(chan answer, 1)
		go func() {
			if _, err := c.in.Write(append(payload, '\n')); err != nil {
				ready <- answer{err: err}
				return
			}
			for {
				line, err := c.readFrame()
				if err != nil {
					ready <- answer{err: err}
					return
				}
				var envelope map[string]any
				if json.Unmarshal(line, &envelope) == nil && envelope["method"] == "events" && envelope["id"] == nil {
					continue
				}
				ready <- answer{frame: line}
				return
			}
		}()
		select {
		case result := <-ready:
			if result.err != nil {
				c.abort()
				return nil, connectionError(mutation)
			}
			frame = result.frame
		case <-ctx.Done():
			c.abort()
			<-ready
			return nil, connectionError(mutation)
		}
	}
	if len(frame) == 0 || len(frame) > maxFrame {
		c.abort()
		return nil, connectionError(mutation)
	}
	var envelope map[string]any
	if json.Unmarshal(frame, &envelope) != nil || envelope["jsonrpc"] != "2.0" {
		c.abort()
		return nil, connectionError(mutation)
	}
	failure, hasError := envelope["error"].(map[string]any)
	if envelope["id"] != float64(id) && !(hasError && envelope["id"] == nil) {
		c.abort()
		return nil, connectionError(mutation)
	}
	if hasError {
		data, _ := failure["data"].(map[string]any)
		kind, _ := data["kind"].(string)
		message, _ := data["message"].(string)
		outcome, _ := data["outcome"].(string)
		context, _ := data["context"].(map[string]any)
		if kind == "" {
			return nil, connectionError(mutation)
		}
		return nil, protocol.FailOutcome(kind, message, outcome, context)
	}
	result, ok := envelope["result"]
	if !ok {
		c.abort()
		return nil, connectionError(mutation)
	}
	return result, nil
}

func (c *Wire) readFrame() ([]byte, error) {
	frame := make([]byte, 0, 4096)
	for {
		chunk, err := c.out.ReadSlice('\n')
		if err == nil {
			chunk = chunk[:len(chunk)-1]
		}
		if len(frame)+len(chunk) > maxFrame {
			return nil, fmt.Errorf("oversized RPC response")
		}
		frame = append(frame, chunk...)
		if err != bufio.ErrBufferFull {
			return frame, err
		}
	}
}

func connectionError(mutation bool) error {
	if mutation {
		return protocol.FailOutcome("OUTCOME_UNKNOWN", "RPC connection failed", "unknown", nil)
	}
	return protocol.Fail("RUNTIME_UNAVAILABLE", "RPC connection failed", nil)
}

func isMutation(method string) bool {
	switch method {
	case "clusters.register", "clusters.update", "clusters.remove", "config.set", "config.reload", "presets.set", "jobs.submit", "jobs.cancel", "jobs.cancel_many", "jobs.purge", "files.transfer", "files.write", "files.mkdir", "files.remove", "files.rename", "files.touch":
		return true
	default:
		return false
	}
}

func (c *Wire) abort() {
	c.failed = true
	if c.process != nil && c.process.Process != nil {
		_ = c.process.Process.Kill()
	}
	if c.in != nil {
		_ = c.in.Close()
	}
	if c.stdout != nil {
		_ = c.stdout.Close()
	}
	c.reap()
}

func (c *Wire) reap() {
	if c.process == nil {
		return
	}
	c.waitOnce.Do(func() { go func() { c.waitErr = c.process.Wait(); close(c.waitDone) }() })
}

// Close drains the temporary serve process within a bound; accepted jobs survive.
func (c *Wire) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)
	defer cancel()
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return connectionError(false)
	}
	c.failed = true
	if c.process == nil {
		return nil
	}
	_ = c.in.Close()
	c.reap()
	timer := time.NewTimer(c.options.Timeout)
	defer timer.Stop()
	select {
	case <-c.waitDone:
		return c.waitErr
	case <-timer.C:
		_ = c.process.Process.Kill()
		if c.stdout != nil {
			_ = c.stdout.Close()
		}
		<-c.waitDone
		return c.waitErr
	}
}
