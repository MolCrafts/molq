package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPingHealthAndDoctor(t *testing.T) {
	root := t.TempDir()
	registryPath := filepath.Join(root, "registry.db")
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"ping", "health", "doctor"} {
		os.Stdout = writer
		err = run([]string{command, "--registry", registryPath})
		if err != nil {
			writer.Close()
			os.Stdout = stdout
			t.Fatal(command, err)
		}
	}
	writer.Close()
	os.Stdout = stdout
	var output bytes.Buffer
	_, _ = output.ReadFrom(reader)
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("output = %s", output.String())
	}
	var ping map[string]any
	if err := json.Unmarshal(lines[0], &ping); err != nil || ping["jsonrpc"] != "2.0" {
		t.Fatalf("ping = %s", lines[0])
	}
	ping = ping["result"].(map[string]any)
	if ping["ok"] != true || ping["checks"] != nil {
		t.Fatalf("ping = %#v", ping)
	}
	var health map[string]any
	if err := json.Unmarshal(lines[1], &health); err != nil || health["jsonrpc"] != "2.0" {
		t.Fatalf("health = %s", lines[1])
	}
	health = health["result"].(map[string]any)
	if health["ok"] != true {
		t.Fatalf("health = %#v", health)
	}
	if _, ok := health["checks"].([]any)[0].(map[string]any)["error"]; ok {
		t.Fatalf("health should not explain errors: %s", lines[1])
	}
	var doctor map[string]any
	if err := json.Unmarshal(lines[2], &doctor); err != nil || doctor["jsonrpc"] != "2.0" {
		t.Fatalf("doctor = %s", lines[2])
	}
	doctor = doctor["result"].(map[string]any)
	if doctor["ok"] != true {
		t.Fatalf("doctor = %#v", doctor)
	}
	first := doctor["checks"].([]any)[0].(map[string]any)
	if first["path"] != registryPath {
		t.Fatalf("doctor path = %#v", first)
	}
}

func TestHelpMentionsDoctor(t *testing.T) {
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	err = run([]string{"--help"})
	writer.Close()
	os.Stdout = stdout
	var output bytes.Buffer
	_, _ = output.ReadFrom(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output.Bytes(), []byte("molq doctor")) || !bytes.Contains(output.Bytes(), []byte("molq job submit")) || !bytes.Contains(output.Bytes(), []byte("molq job list")) {
		t.Fatalf("help = %s", output.String())
	}
	if bytes.Contains(output.Bytes(), []byte("jsonrpc")) {
		t.Fatalf("help is text, not a JSON-RPC envelope: %s", output.String())
	}
}

func TestHelpAndGlobalsDoNotConsumeJobArguments(t *testing.T) {
	args := []string{"--registry", "registry.db", "submit", "local", "--", "echo", "--help", "--registry", "payload"}
	peeled := peelGlobals(args)
	if wantsHelp(peeled) {
		t.Fatal("job argv was interpreted as CLI help")
	}
	flags, positional := splitArgs(peeled[1:])
	if first(flags["--registry"]) != "registry.db" || len(flags["--registry"]) != 1 {
		t.Fatalf("global flags = %#v", flags)
	}
	if len(positional) != 6 || positional[4] != "--registry" || positional[5] != "payload" {
		t.Fatalf("job argv = %#v", positional)
	}
	if wantsHelp([]string{"cluster", "get", "help"}) {
		t.Fatal("cluster name help was interpreted as CLI help")
	}
}

func TestVersionIsJSON(t *testing.T) {
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	err = run([]string{"--version"})
	writer.Close()
	os.Stdout = stdout
	defer reader.Close()
	var output bytes.Buffer
	_, _ = output.ReadFrom(reader)
	var envelope struct {
		Result struct {
			Version string `json:"version"`
		} `json:"result"`
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &envelope); err != nil || envelope.Result.Version != "0.9.0" || bytes.Contains(output.Bytes(), []byte("protocol_major")) {
		t.Fatalf("version = %s", output.String())
	}
}

func TestWatchOnceFailsWithOneError(t *testing.T) {
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	err = run([]string{"watch", "--once", "--cluster", "missing", "--registry", filepath.Join(t.TempDir(), "registry.db")})
	writer.Close()
	os.Stdout = stdout
	defer reader.Close()
	var output bytes.Buffer
	_, _ = output.ReadFrom(reader)
	status, ok := err.(commandExit)
	if !ok || status.code != 3 {
		t.Fatalf("exit = %v", err)
	}
	if bytes.Count(bytes.TrimSpace(output.Bytes()), []byte("\n")) != 0 {
		t.Fatalf("duplicate output = %s", output.String())
	}
	var envelope map[string]any
	if err := json.Unmarshal(output.Bytes(), &envelope); err != nil || envelope["error"] == nil {
		t.Fatalf("error output = %s", output.String())
	}
}
