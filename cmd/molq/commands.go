package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/MolCrafts/molq/client"
	"github.com/MolCrafts/molq/protocol"
)

type caller = client.Wire

var clientExecutable = os.Executable

func runCommand(command string, args []string) error {
	flags, positionals := splitArgs(args)
	if command == "transfer" {
		flags, positionals = leadingGlobals(args)
	}
	wire, err := newCaller(flags)
	if err != nil {
		return err
	}
	defer wire.Close()
	result, err := dispatchCommand(wire, command, flags, positionals)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(encoded))
	if successful, ok := nestedBool(result, "completion", "successful"); ok && !successful {
		return commandExit{9}
	}
	if hasCoverageErrors(result) {
		return commandExit{8}
	}
	return nil
}

func dispatchCommand(client *caller, command string, flags map[string][]string, positionals []string) (any, error) {
	switch command {
	case "discover":
		return client.Call("schema.discover", map[string]any{})
	case "clusters":
		return clustersCommand(client, positionals, flags)
	case "submit":
		return submitCommand(client, positionals, flags)
	case "transfer":
		return transferCommand(client, positionals)
	case "status":
		ref, err := readJSON(required(flags, "--ref"))
		if err != nil {
			return nil, err
		}
		return client.Call("jobs.get", map[string]any{"ref": ref, "consistency": orString(first(flags["--consistency"]), "live")})
	case "cancel":
		ref, err := readJSON(required(flags, "--ref"))
		if err != nil {
			return nil, err
		}
		params := map[string]any{"ref": ref}
		if key := first(flags["--request-key"]); key != "" {
			params["request_key"] = key
		}
		return client.Call("jobs.cancel", params)
	case "logs":
		ref, err := readJSON(required(flags, "--ref"))
		if err != nil {
			return nil, err
		}
		return client.Call("logs.read", map[string]any{
			"ref": ref, "stream": orString(first(flags["--stream"]), "stdout"),
			"offset": numberOr(first(flags["--offset"]), 0), "limit": numberOr(first(flags["--limit"]), 262144),
		})
	case "wait":
		return waitCommand(client, flags)
	case "list":
		if len(positionals) != 1 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq list CLUSTER", nil)
		}
		params := map[string]any{"cluster": positionals[0]}
		if states := flags["--state"]; len(states) > 0 {
			params["states"] = anyList(states)
		}
		if name := first(flags["--name"]); name != "" {
			params["name"] = name
		}
		return client.Call("jobs.list", params)
	case "observe":
		if len(positionals) != 1 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq observe CLUSTER", nil)
		}
		params := map[string]any{"cluster": positionals[0]}
		if cursor := first(flags["--cursor"]); cursor != "" {
			params["cursor"] = cursor
		}
		return client.Call("jobs.observe", params)
	case "history":
		return historyCommand(client, positionals, flags)
	case "purge":
		if len(positionals) > 1 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq purge [CLUSTER]", nil)
		}
		params := map[string]any{}
		if len(positionals) == 1 {
			params["cluster"] = positionals[0]
		}
		return client.Call("jobs.purge", params)
	default:
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq serve|rpc|watch|discover|clusters|submit|transfer|status|cancel|logs|wait|list|observe|history|purge", nil)
	}
}

func clustersCommand(client *caller, positionals []string, flags map[string][]string) (any, error) {
	if len(positionals) == 0 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq clusters list|get|register|update|remove", nil)
	}
	switch positionals[0] {
	case "list":
		return client.Call("clusters.list", map[string]any{})
	case "get":
		if len(positionals) != 2 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq clusters get CLUSTER", nil)
		}
		return client.Call("clusters.get", map[string]any{"cluster": positionals[1]})
	case "register":
		definition, err := readJSON(required(flags, "--definition"))
		if err != nil {
			return nil, err
		}
		object, ok := definition.(map[string]any)
		if !ok {
			return nil, protocol.Fail("INVALID_INPUT", "Cluster definition must be an object", nil)
		}
		return client.Call("clusters.register", object)
	case "update":
		if len(positionals) != 2 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq clusters update CLUSTER --definition JSON --revision N", nil)
		}
		definition, err := readJSON(required(flags, "--definition"))
		if err != nil {
			return nil, err
		}
		return client.Call("clusters.update", map[string]any{
			"cluster": positionals[1], "definition": definition, "expected_revision": numberOr(required(flags, "--revision"), 0),
		})
	case "remove":
		if len(positionals) != 2 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq clusters remove CLUSTER --revision N", nil)
		}
		return client.Call("clusters.remove", map[string]any{
			"cluster": positionals[1], "expected_revision": numberOr(required(flags, "--revision"), 0),
		})
	default:
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq clusters list|get|register|update|remove", nil)
	}
}

func submitCommand(client *caller, positionals []string, flags map[string][]string) (any, error) {
	if len(positionals) == 0 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq submit CLUSTER [--spec JSON | -- ARGV]", nil)
	}
	specText := first(flags["--spec"])
	argv := []string{}
	if len(positionals) > 1 && positionals[1] == "--" {
		argv = positionals[2:]
	}
	if (specText == "") == (len(argv) == 0) {
		return nil, protocol.Fail("INVALID_INPUT", "Provide exactly one of spec or argv", nil)
	}
	name, comment := first(flags["--name"]), first(flags["--comment"])
	if specText != "" && (name != "" || comment != "") {
		return nil, protocol.Fail("INVALID_INPUT", "Name and comment belong in the spec when --spec is set", nil)
	}
	var spec any
	var err error
	if specText != "" {
		spec, err = readJSON(specText)
		if err != nil {
			return nil, err
		}
	} else {
		body := map[string]any{"execution": map[string]any{"units": []any{
			map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": anyList(argv)}},
		}}}
		if name != "" || comment != "" {
			scheduling := map[string]any{}
			if name != "" {
				scheduling["name"] = name
			}
			if comment != "" {
				scheduling["comment"] = comment
			}
			body["scheduling"] = scheduling
		}
		spec = body
	}
	params := map[string]any{"cluster": positionals[0], "spec": spec}
	if key := first(flags["--request-key"]); key != "" {
		params["request_key"] = key
	}
	return client.Call("jobs.submit", params)
}

func historyCommand(client *caller, positionals []string, flags map[string][]string) (any, error) {
	if len(positionals) != 1 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq history CLUSTER", nil)
	}
	params := map[string]any{"cluster": positionals[0]}
	if refs := first(flags["--refs"]); refs != "" {
		if len(flags["--state"]) > 0 || first(flags["--since"]) != "" || first(flags["--until"]) != "" || first(flags["--name"]) != "" || first(flags["--limit"]) != "" {
			return nil, protocol.Fail("INVALID_INPUT", "History refs are mutually exclusive with range filters", nil)
		}
		value, err := readJSON(refs)
		if err != nil {
			return nil, err
		}
		params["refs"] = value
	}
	if states := flags["--state"]; len(states) > 0 {
		params["states"] = anyList(states)
	}
	for _, name := range []string{"since", "until", "name"} {
		if value := first(flags["--"+name]); value != "" {
			params[name] = value
		}
	}
	if limit := first(flags["--limit"]); limit != "" {
		params["limit"] = numberOr(limit, 100)
	}
	return client.Call("jobs.history", params)
}

func waitCommand(client *caller, flags map[string][]string) (any, error) {
	ref, err := readJSON(required(flags, "--ref"))
	if err != nil {
		return nil, err
	}
	timeout := floatOr(first(flags["--timeout"]), 3600)
	interval := floatOr(first(flags["--interval"]), 1)
	if timeout <= 0 || interval <= 0 {
		return nil, protocol.Fail("INVALID_INPUT", "wait timeout/interval must be positive", nil)
	}
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	var cursor string
	for {
		if !time.Now().Before(deadline) {
			return nil, protocol.Fail("WAIT_TIMEOUT", "Foreground wait deadline reached", nil)
		}
		params := map[string]any{"refs": []any{ref}}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, err := client.Call("jobs.observe", params)
		if err != nil {
			return nil, err
		}
		body := result.(map[string]any)
		cursor = stringField(body["cursor"])
		if body["completion"].(map[string]any)["all_confirmed_terminal"] == true {
			return body, nil
		}
		delay := time.Duration(interval * float64(time.Second))
		if remaining := time.Until(deadline); remaining < delay {
			delay = remaining
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func newCaller(flags map[string][]string) (*caller, error) {
	file, resolved, err := resolveConfig(first(flags["--config"]))
	if err != nil {
		return nil, err
	}
	executable := file.Client.Executable
	if executable == "" {
		executable, err = clientExecutable()
		if err != nil {
			return nil, err
		}
	}
	return client.New(client.Options{
		Endpoint: orString(first(flags["--endpoint"]), file.Client.Endpoint),
		Token:    orString(first(flags["--token"]), file.Client.Token),
		Registry: orString(first(flags["--registry"]), file.Client.Registry),
		Config:   resolved, Executable: executable,
	}), nil
}

func splitArgs(args []string) (map[string][]string, []string) {
	flags := map[string][]string{}
	var positionals []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			positionals = append(positionals, args[i:]...)
			break
		}
		if strings.HasPrefix(args[i], "--") {
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				flags[args[i]] = append(flags[args[i]], args[i+1])
				i++
				continue
			}
			flags[args[i]] = append(flags[args[i]], "true")
			continue
		}
		positionals = append(positionals, args[i])
	}
	return flags, positionals
}

func readJSON(value string) (any, error) {
	var text string
	switch {
	case value == "@-":
		body, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, protocol.Fail("INVALID_INPUT", "Cannot read JSON input", nil)
		}
		text = string(body)
	case strings.HasPrefix(value, "@"):
		body, err := os.ReadFile(value[1:])
		if err != nil {
			return nil, protocol.Fail("INVALID_INPUT", "Cannot read JSON input", nil)
		}
		text = string(body)
	default:
		text = value
	}
	var parsed any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "Cannot read JSON input", nil)
	}
	return parsed, nil
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func required(flags map[string][]string, name string) string {
	return first(flags[name])
}

func orString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func numberOr(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	var number int
	if _, err := fmt.Sscan(value, &number); err != nil {
		return fallback
	}
	return number
}

func floatOr(value string, fallback float64) float64 {
	if value == "" {
		return fallback
	}
	var number float64
	if _, err := fmt.Sscan(value, &number); err != nil {
		return fallback
	}
	return number
}

func anyList(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func stringField(value any) string {
	text, _ := value.(string)
	return text
}

func mapValue(value any) map[string]any {
	item, _ := value.(map[string]any)
	if item == nil {
		return map[string]any{}
	}
	return item
}

func nestedBool(value any, key, child string) (bool, bool) {
	body, ok := value.(map[string]any)
	if !ok {
		return false, false
	}
	parent, ok := body[key].(map[string]any)
	if !ok {
		return false, false
	}
	boolean, ok := parent[child].(bool)
	return boolean, ok
}
