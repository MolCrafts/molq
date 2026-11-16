package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/MolCrafts/molq/rpc"
	"github.com/MolCrafts/molq/serve"
)

func TestRPCWatchAndDiagnosticsUseExplicitEndpoint(t *testing.T) {
	root := t.TempDir()
	service, err := serve.Open(filepath.Join(root, "remote.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if _, err := service.Call("clusters.register", map[string]any{"name": "local", "scheduler": "shell"}); err != nil {
		t.Fatal(err)
	}
	token := "0123456789abcdef"
	handler, err := rpc.Handler(service, token)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	binary, err := clientExecutable()
	if err != nil {
		t.Fatal(err)
	}
	localRegistry := filepath.Join(root, "must-not-open.db")
	for _, args := range [][]string{
		{"rpc", "molq.hello"},
		{"watch", "--once", "--cluster", "local", "--project-root", root},
		{"ping"},
	} {
		args = append(args, "--endpoint", server.URL, "--token", token, "--registry", localRegistry)
		output, err := exec.Command(binary, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("args=%v err=%v output=%s", args, err, output)
		}
		if args[0] == "rpc" {
			var envelope map[string]any
			if err := json.Unmarshal(output, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope["result"].(map[string]any)["runtime_instance_id"] != service.ID {
				t.Fatalf("RPC used another service: %s", output)
			}
		}
		if args[0] == "watch" && len(output) != 0 {
			t.Fatalf("watch failed against selected endpoint: %s", output)
		}
	}
	if _, err := os.Stat(localRegistry); !os.IsNotExist(err) {
		t.Fatal("endpoint clients opened a local registry")
	}
}

func TestInvalidClusterDefinitionReturnsInputError(t *testing.T) {
	binary, err := clientExecutable()
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(binary, "cluster", "register", "--definition", "[]").CombinedOutput()
	if failure, ok := err.(*exec.ExitError); !ok || failure.ExitCode() != 2 {
		t.Fatalf("err=%v output=%s", err, output)
	}
	var envelope map[string]any
	if err := json.Unmarshal(output, &envelope); err != nil {
		t.Fatal(err)
	}
	data := envelope["error"].(map[string]any)["data"].(map[string]any)
	if data["kind"] != "INVALID_INPUT" {
		t.Fatal(data)
	}
}

func TestMissingConfigCanBeCreatedThroughRPC(t *testing.T) {
	binary, err := clientExecutable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	output, err := exec.Command(binary, "rpc", "config.set", "--config", configPath, "--registry", filepath.Join(root, "registry.db"), "--params", `{"key":"cache_max_age","value":10}`).CombinedOutput()
	if err != nil {
		t.Fatalf("err=%v output=%s", err, output)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document["settings"].(map[string]any)["cache_max_age"] != float64(10) {
		t.Fatal(document)
	}
}
