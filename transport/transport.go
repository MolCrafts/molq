// Package transport runs target commands and file I/O. It does not know schedulers.
package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	posixpath "path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

const outputLimit = 1048576
const fileChunkLimit = 262144
const fileBlockSize = 65536

// Result is one bounded command result.
type Result struct {
	ReturnCode int
	Stdout     []byte
	Stderr     []byte
}

// Text decodes stdout without treating stderr as state.
func (r Result) Text() string { return strings.ToValidUTF8(string(r.Stdout), "\uFFFD") }

// Transport is target I/O with deadlines. input is sent only when hasInput is set.
type Transport interface {
	Run(ctx context.Context, argv []string, timeout time.Duration, input []byte, hasInput bool) (Result, error)
	Read(ctx context.Context, path string, offset, limit int) ([]byte, error)
	Write(ctx context.Context, path string, data []byte, offset *int) error
}

// Local runs commands on this machine without a shell.
type Local struct{}

// SSH invokes system OpenSSH. Connection setup stays in OpenSSH.
type SSH struct{ Alias string }

// Create selects the configured transport.
func Create(definition map[string]any) Transport {
	if definition["kind"] == "ssh" {
		alias, _ := definition["alias"].(string)
		return SSH{Alias: alias}
	}
	return Local{}
}

func (Local) Run(ctx context.Context, argv []string, timeout time.Duration, input []byte, hasInput bool) (Result, error) {
	if len(argv) == 0 {
		return Result{}, protocol.Fail("TRANSPORT_UNAVAILABLE", "Cannot start target command", map[string]any{"command": "", "dispatched": false})
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	configureCommand(cmd)
	cmd.WaitDelay = 2 * time.Second
	var stdout, stderr bytes.Buffer
	var exceeded atomic.Bool
	cmd.Stdout = limitWriter{buf: &stdout, exceeded: &exceeded, cancel: cancel}
	cmd.Stderr = limitWriter{buf: &stderr, exceeded: &exceeded, cancel: cancel}
	if hasInput {
		cmd.Stdin = bytes.NewReader(input)
	}
	err := cmd.Run()
	dispatched := cmd.Process != nil
	if errors.Is(err, exec.ErrWaitDelay) {
		// The command exited, but a helper still owns an inherited output pipe.
		// Stop only this command's process group; detached allocations own theirs.
		_ = cmd.Cancel()
	}
	if exceeded.Load() {
		return Result{}, protocol.Fail("PARTIAL_FAILURE", "Command output exceeds bounded frame size", map[string]any{"command": argv[0], "limit": outputLimit, "dispatched": dispatched})
	}
	if runCtx.Err() != nil {
		message := "Target command cancelled"
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			message = "Target command deadline exceeded"
		}
		return Result{}, protocol.Fail("TRANSPORT_UNAVAILABLE", message, map[string]any{"command": argv[0], "dispatched": dispatched})
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return Result{ReturnCode: exitErr.ExitCode(), Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, nil
	}
	if err != nil {
		if _, ok := err.(*protocol.Error); ok {
			return Result{}, err
		}
		return Result{}, protocol.Fail("TRANSPORT_UNAVAILABLE", "Target command failed", map[string]any{"command": argv[0], "dispatched": dispatched})
	}
	return Result{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, nil
}

func (s SSH) Run(ctx context.Context, argv []string, timeout time.Duration, input []byte, hasInput bool) (Result, error) {
	remote := shellJoin(argv)
	args, err := SSHCommand(s.Alias)
	if err != nil {
		return Result{}, err
	}
	args = append(args, "--", s.Alias, remote)
	result, err := (Local{}).Run(ctx, args, timeout, input, hasInput)
	if err == nil && result.ReturnCode == 255 {
		failure := sshFailure(result.Stderr)
		failure.Context["dispatched"] = failure.Kind != "AUTH_REQUIRED"
		return Result{}, failure
	}
	return result, err
}

func (s SSH) Read(ctx context.Context, path string, offset, limit int) ([]byte, error) {
	if offset < 0 || limit < 0 || limit > fileChunkLimit {
		return nil, protocol.Fail("INVALID_INPUT", "Invalid target file range", nil)
	}
	if limit == 0 {
		return []byte{}, nil
	}
	// Read an aligned window using only portable dd operands. Slice the
	// leading/trailing blocks locally to retain exact byte offset semantics.
	blockSize := min(fileBlockSize, limit)
	prefix := offset % blockSize
	blocks := (prefix + limit + blockSize - 1) / blockSize
	result, err := s.Run(ctx, []string{"dd", "if=" + path, fmt.Sprintf("bs=%d", blockSize), fmt.Sprintf("skip=%d", offset/blockSize), fmt.Sprintf("count=%d", blocks)}, 30*time.Second, nil, false)
	if err != nil {
		return nil, err
	}
	if result.ReturnCode != 0 {
		return nil, protocol.Fail("FILE_UNAVAILABLE", "Cannot read remote target file", map[string]any{"path": path})
	}
	start := min(prefix, len(result.Stdout))
	end := min(start+limit, len(result.Stdout))
	return result.Stdout[start:end], nil
}

func (s SSH) Write(ctx context.Context, path string, data []byte, offset *int) error {
	parent := posixpath.Dir(path)
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	temp := path + "." + hex.EncodeToString(nonce[:]) + ".tmp"
	q := func(value string) string { return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'" }
	script := fmt.Sprintf("umask 077; mkdir -p -- %s && cat > %s && mv -f -- %s %s", q(parent), q(temp), q(temp), q(path))
	if offset != nil {
		if *offset < 0 {
			return protocol.Fail("INVALID_INPUT", "Invalid target file offset", nil)
		}
		blockSize := 1
		if *offset%fileBlockSize == 0 {
			blockSize = fileBlockSize
		}
		script = fmt.Sprintf("umask 077; mkdir -p -- %s && dd of=%s bs=%d seek=%d conv=notrunc", q(parent), q(path), blockSize, *offset/blockSize)
	}
	result, err := s.Run(ctx, []string{"sh", "-c", script}, 30*time.Second, data, true)
	if err != nil {
		if failure, ok := err.(*protocol.Error); ok && failure.Context["dispatched"] == true {
			return protocol.FailOutcome("OUTCOME_UNKNOWN", "Target file write outcome is uncertain", "unknown", failure.Context)
		}
		return err
	}
	if result.ReturnCode != 0 {
		return protocol.FailOutcome("FILE_UNAVAILABLE", "Cannot write remote target file", "unknown", map[string]any{"path": path})
	}
	return nil
}

func shellJoin(argv []string) string {
	parts := make([]string, len(argv))
	for i, arg := range argv {
		parts[i] = "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
	}
	return strings.Join(parts, " ")
}

type limitWriter struct {
	buf      *bytes.Buffer
	exceeded *atomic.Bool
	cancel   context.CancelFunc
}

func (w limitWriter) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > outputLimit {
		w.exceeded.Store(true)
		w.cancel()
		return 0, protocol.Fail("PARTIAL_FAILURE", "Command output exceeds bounded frame size", map[string]any{"limit": outputLimit})
	}
	return w.buf.Write(p)
}
