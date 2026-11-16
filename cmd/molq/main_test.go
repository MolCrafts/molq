package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Tests of client entry points use the same temporary RPC process as installed CLI.
func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "serve" && os.Args[2] == "--file-io" {
		if err := runServe(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	root, err := os.MkdirTemp("", "molq-cli-test-")
	if err != nil {
		panic(err)
	}
	binary := filepath.Join(root, "molq")
	command := exec.Command("go", "build", "-o", binary, ".")
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		fmt.Fprintln(os.Stderr, err, string(output))
		os.RemoveAll(root)
		os.Exit(1)
	}
	clientExecutable = func() (string, error) { return binary, nil }
	// Job directories live under the target home; keep them out of the real
	// one, while tests that build binaries keep the real Go caches.
	caches, err := exec.Command("go", "env", "GOCACHE", "GOMODCACHE", "GOPATH").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.RemoveAll(root)
		os.Exit(1)
	}
	for i, value := range strings.Split(strings.TrimSpace(string(caches)), "\n") {
		os.Setenv([]string{"GOCACHE", "GOMODCACHE", "GOPATH"}[i], value)
	}
	os.Setenv("HOME", filepath.Join(root, "home"))
	code := m.Run()
	os.RemoveAll(root)
	os.Exit(code)
}
