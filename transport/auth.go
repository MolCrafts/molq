package transport

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

// SSHCommand is also used by rsync. An alias that already has a ControlPath
// keeps that OpenSSH master; molq does not override it and does not close it.
// Without one, authentication and later I/O share a private ControlPath.
func SSHCommand(alias string) ([]string, error) {
	args := []string{"ssh", "-T", "-o", "BatchMode=yes"}
	if userControlsMultiplexing(alias) {
		return args, nil
	}
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("molq-ssh-%d", os.Getuid()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return nil, protocol.Fail("TRANSPORT_UNAVAILABLE", "Unsafe SSH control directory", nil)
	}
	digest := sha256.Sum256([]byte(alias))
	socket := filepath.Join(dir, fmt.Sprintf("%x", digest[:12]))
	return append(args, "-o", "ControlMaster=auto", "-o", "ControlPersist=600", "-o", "ControlPath="+socket), nil
}

// userControlsMultiplexing reports whether OpenSSH already has a ControlPath
// for alias. A failed probe keeps the private master rather than failing the
// operation before the real command runs.
func userControlsMultiplexing(alias string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := (Local{}).Run(ctx, []string{"ssh", "-G", "--", alias}, 3*time.Second, nil, false)
	if err != nil || result.ReturnCode != 0 {
		return false
	}
	for _, line := range strings.Split(result.Text(), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || !strings.EqualFold(key, "controlpath") {
			continue
		}
		value = strings.TrimSpace(value)
		return value != "" && !strings.EqualFold(value, "none")
	}
	return false
}

func (s SSH) Authenticate(ctx context.Context, code string) error {
	if code == "" || len(code) > 4096 || strings.ContainsAny(code, "\r\n\x00") {
		return protocol.Fail("INVALID_INPUT", "Expected one authentication response", nil)
	}
	dir, err := os.MkdirTemp("", "molq-auth-")
	if err != nil {
		return protocol.Fail("TRANSPORT_UNAVAILABLE", "Cannot create authentication helper", nil)
	}
	defer os.RemoveAll(dir)
	response := filepath.Join(dir, "response")
	if err := os.WriteFile(response, []byte(code+"\n"), 0o600); err != nil {
		return err
	}
	askpass := filepath.Join(dir, "askpass")
	script := "#!/bin/sh\n" + shellJoin([]string{"cat", response}) + "\n" + shellJoin([]string{"rm", "-f", "--", response}) + "\n"
	if err := os.WriteFile(askpass, []byte(script), 0o700); err != nil {
		return err
	}
	args, err := SSHCommand(s.Alias)
	if err != nil {
		return err
	}
	// Put the interactive override first: OpenSSH uses the first option value.
	args = append([]string{"ssh", "-o", "BatchMode=no", "-o", "NumberOfPasswordPrompts=1"}, args[1:]...)
	args = append(args, "--", s.Alias, "true")
	args = append([]string{"env", "SSH_ASKPASS=" + askpass, "SSH_ASKPASS_REQUIRE=force", "DISPLAY=:0"}, args...)
	authCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	result, err := (Local{}).Run(authCtx, args, 60*time.Second, nil, false)
	if err != nil {
		if errors.Is(authCtx.Err(), context.DeadlineExceeded) {
			return protocol.Fail("CONNECTION_TIMEOUT", "Authentication connection timed out", nil)
		}
		return protocol.Fail("TRANSPORT_UNAVAILABLE", "Authentication connection failed", nil)
	}
	if result.ReturnCode != 0 {
		failure := sshFailure(result.Stderr)
		// OpenSSH diagnostics can explain local setup failures as well as network
		// failures. Redact the response before bounding or exposing that text.
		diagnostic := strings.TrimSpace(strings.ReplaceAll(string(result.Stderr), code, "[redacted]"))
		if len(diagnostic) > 2048 {
			diagnostic = diagnostic[:2048]
		}
		failure.Context["diagnostic"] = strings.ToValidUTF8(diagnostic, "\uFFFD")
		return failure
	}
	return nil
}

func sshFailure(stderr []byte) *protocol.Error {
	text := strings.ToLower(string(stderr))
	var failure *protocol.Error
	if strings.Contains(text, "permission denied") || strings.Contains(text, "authentication failed") || strings.Contains(text, "verification code") {
		failure = protocol.Fail("AUTH_REQUIRED", "SSH authentication was rejected", nil)
	} else {
		failure = protocol.Fail("TRANSPORT_UNAVAILABLE", "SSH connection failed", nil)
	}
	diagnostic := strings.TrimSpace(string(stderr))
	if len(diagnostic) > 2048 {
		diagnostic = diagnostic[:2048]
	}
	if diagnostic != "" {
		failure.Context["diagnostic"] = strings.ToValidUTF8(diagnostic, "\uFFFD")
	}
	return failure
}
