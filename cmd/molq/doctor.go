package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/MolCrafts/molq/client"
	"github.com/MolCrafts/molq/config"
	"github.com/MolCrafts/molq/protocol"
	"path/filepath"
)

// runPing asks the selected serve process for molq.hello and reports only whether it answered.
func runPing(args []string) error {
	probe, err := newProbe(args, "ping")
	if err != nil {
		return err
	}
	defer probe.close()
	_, err = probe.hello()
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true})
}

// runHealth reports whether the selected installation is ready. It does not explain failures.
func runHealth(args []string) error {
	return runChecks(args, "health", false)
}

// runDoctor reports each check with its path and, on failure, the error.
func runDoctor(args []string) error {
	return runChecks(args, "doctor", true)
}

func runChecks(args []string, command string, diagnose bool) error {
	probe, err := newProbe(args, command)
	if err != nil {
		return err
	}
	defer probe.close()
	checks := []map[string]any{}
	if probe.endpoint == "" {
		registryCheck := map[string]any{"name": "registry", "ok": probe.registryErr == nil}
		if diagnose {
			registryCheck["path"] = probe.registryPath
			attachError(registryCheck, probe.registryErr)
		}
		checks = append(checks, registryCheck)
	}
	if probe.configChecked {
		configCheck := map[string]any{"name": "config", "ok": probe.configErr == nil}
		if diagnose {
			configCheck["path"] = probe.configPath
			attachError(configCheck, probe.configErr)
		}
		checks = append(checks, configCheck)
	}
	_, helloErr := probe.hello()
	helloCheck := map[string]any{"name": "hello", "ok": helloErr == nil}
	if diagnose {
		attachError(helloCheck, helloErr)
	}
	checks = append(checks, helloCheck)
	ok := true
	for _, check := range checks {
		if check["ok"] != true {
			ok = false
		}
	}
	if err := printJSON(map[string]any{"ok": ok, "checks": checks}); err != nil {
		return err
	}
	if !ok {
		probe.close()
		return commandExit{1}
	}
	return nil
}

type probe struct {
	wire          *client.Wire
	helloResult   any
	helloErr      error
	endpoint      string
	token         string
	registryPath  string
	configPath    string
	configChecked bool
	registryErr   error
	configErr     error
}

func newProbe(args []string, command string) (*probe, error) {
	flags, positionals := splitArgs(args)
	if len(positionals) > 0 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq "+command, nil)
	}
	item := &probe{
		endpoint:   first(flags["--endpoint"]),
		token:      first(flags["--token"]),
		configPath: first(flags["--config"]),
	}
	file, resolved, err := resolveConfig(item.configPath)
	if resolved != "" {
		item.configPath = resolved
		item.configChecked = true
	}
	item.configErr = err
	if err != nil {
		file = config.File{}
	}
	if item.endpoint == "" {
		item.endpoint = file.Client.Endpoint
	}
	if item.token == "" {
		item.token = file.Client.Token
	}
	executable := file.Client.Executable
	if executable == "" {
		executable, err = clientExecutable()
		if err != nil {
			return nil, err
		}
	}
	item.registryPath = orString(first(flags["--registry"]), file.Client.Registry)
	if item.registryPath == "" {
		item.registryPath = filepath.Join(filepath.Dir(config.DefaultPath()), "registry.db")
	}
	validatedConfig := item.configPath
	if item.configErr != nil {
		validatedConfig = ""
	}
	item.wire = client.New(client.Options{
		Endpoint: item.endpoint, Token: item.token, Registry: item.registryPath,
		Config: validatedConfig, Executable: executable,
	})
	item.helloResult, item.helloErr = item.wire.Call("molq.hello", map[string]any{})
	if item.endpoint == "" {
		item.registryErr = item.helloErr
		if failure, ok := item.helloErr.(*protocol.Error); ok && failure.Kind == "INVALID_INPUT" && item.configChecked {
			item.configErr, item.registryErr = item.helloErr, nil
		}
	}
	return item, nil
}

func (p *probe) hello() (any, error) { return p.helloResult, p.helloErr }

func (p *probe) close() {
	if p.wire != nil {
		_ = p.wire.Close()
		p.wire = nil
	}
}

func attachError(check map[string]any, err error) {
	if err == nil {
		return
	}
	item, ok := err.(*protocol.Error)
	if !ok {
		check["error"] = map[string]any{"kind": "INTERNAL_ERROR", "message": err.Error()}
		return
	}
	check["error"] = map[string]any{"kind": item.Kind, "message": item.Message}
}

func printJSON(value any) error {
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": value})
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(encoded))
	return nil
}
