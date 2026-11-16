package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/client"
	"github.com/MolCrafts/molq/protocol"
)

type caller = client.Wire

var clientExecutable = os.Executable

func runCommand(command string, args []string) error {
	if message, ok := retiredCommand(command); ok {
		return protocol.Fail("INVALID_INPUT", message, nil)
	}
	var flags map[string][]string
	var positionals []string
	if command == "transfer" {
		flags, positionals = leadingGlobals(args)
	} else {
		flags, positionals = splitArgs(args)
	}
	seconds, err := transferDeadline(flags["--transfer-deadline"])
	if err != nil {
		return err
	}
	var timeout time.Duration
	if seconds != 0 {
		if command != "transfer" {
			return protocol.Fail("INVALID_INPUT", "--transfer-deadline requires molq transfer", nil)
		}
		timeout = time.Duration(seconds+15) * time.Second
	}
	wire, err := newCaller(flags, timeout)
	if err != nil {
		return err
	}
	defer wire.Close()
	result, err := dispatchCommand(wire, command, flags, positionals, seconds)
	if err != nil {
		return err
	}
	view := command
	if (command == "job" || command == "cluster") && len(positionals) > 0 {
		view = command + " " + positionals[0]
	}
	if err := present(view, result); err != nil {
		return err
	}
	if successful, ok := nestedBool(result, "completion", "successful"); ok && !successful {
		return commandExit{9}
	}
	if hasCoverageErrors(result) {
		return commandExit{8}
	}
	return nil
}

func retiredCommand(command string) (string, bool) {
	message, ok := map[string]string{
		"list":     "usage: molq job list CLUSTER [--state STATE] [--name NAME]",
		"clusters": "usage: molq cluster list|get|register|update|remove",
		"submit":   "usage: molq job submit CLUSTER --project-root PATH [--spec JSON | -- ARGV]",
		"status":   "usage: molq job status --ref JSON",
		"cancel":   "usage: molq job cancel --ref JSON",
		"requeue":  "usage: molq job requeue --ref JSON",
		"logs":     "usage: molq job logs --ref JSON",
		"wait":     "usage: molq job wait --ref JSON",
		"observe":  "usage: molq job observe CLUSTER",
		"history":  "usage: molq job history CLUSTER",
		"purge":    "usage: molq job purge [CLUSTER]",
	}[command]
	return message, ok
}

func dispatchCommand(client *caller, command string, flags map[string][]string, positionals []string, transferSeconds int) (any, error) {
	switch command {
	case "discover":
		return client.Call("schema.discover", map[string]any{})
	case "cluster":
		return clusterCommand(client, positionals, flags)
	case "job":
		return jobCommand(client, positionals, flags)
	case "transfer":
		return transferCommand(client, positionals, transferSeconds)
	default:
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq cluster|job|transfer|discover|serve|rpc|watch|ping|health|doctor", nil)
	}
}

func jobCommand(client *caller, positionals []string, flags map[string][]string) (any, error) {
	if len(positionals) == 0 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq job list|submit|preview|status|cancel|requeue|logs|wait|history|observe", nil)
	}
	verb, rest := positionals[0], positionals[1:]
	switch verb {
	case "list":
		if len(rest) != 1 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq job list CLUSTER [--state STATE] [--name NAME]", nil)
		}
		params := map[string]any{"cluster": rest[0]}
		if project := first(flags["--project-root"]); project != "" {
			params["project_root"] = project
		}
		if states := flags["--state"]; len(states) > 0 {
			params["states"] = anyList(states)
		}
		if name := first(flags["--name"]); name != "" {
			params["name"] = name
		}
		return client.Call("jobs.list", params)
	case "submit", "preview":
		params, err := jobSubmission(verb, rest, flags)
		if err != nil {
			return nil, err
		}
		return client.Call("jobs."+verb, params)
	case "status":
		ref, err := readJSON(required(flags, "--ref"))
		if err != nil {
			return nil, err
		}
		return client.Call("jobs.get", map[string]any{"ref": ref, "consistency": orString(first(flags["--consistency"]), "live")})
	case "cancel", "requeue":
		ref, err := readJSON(required(flags, "--ref"))
		if err != nil {
			return nil, err
		}
		params := map[string]any{"ref": ref}
		if key := first(flags["--request-key"]); key != "" {
			params["request_key"] = key
		}
		return client.Call("jobs."+verb, params)
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
	case "observe":
		if len(rest) != 1 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq job observe CLUSTER [--cursor TOKEN]", nil)
		}
		params := map[string]any{"cluster": rest[0]}
		if project := first(flags["--project-root"]); project != "" {
			params["project_root"] = project
		}
		if cursor := first(flags["--cursor"]); cursor != "" {
			params["cursor"] = cursor
		}
		return client.Call("jobs.observe", params)
	case "history":
		return historyCommand(client, rest, flags)

	default:
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq job list|submit|preview|status|cancel|requeue|logs|wait|history|observe", nil)
	}
}

func clusterCommand(client *caller, positionals []string, flags map[string][]string) (any, error) {
	if len(positionals) == 0 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq cluster list|get|register|update|remove", nil)
	}
	switch positionals[0] {
	case "list":
		return client.Call("clusters.list", map[string]any{})
	case "get":
		if len(positionals) != 2 {
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq cluster get CLUSTER", nil)
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
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq cluster update CLUSTER --definition JSON --revision N", nil)
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
			return nil, protocol.Fail("INVALID_INPUT", "usage: molq cluster remove CLUSTER --revision N", nil)
		}
		return client.Call("clusters.remove", map[string]any{
			"cluster": positionals[1], "expected_revision": numberOr(required(flags, "--revision"), 0),
		})
	default:
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq cluster list|get|register|update|remove", nil)
	}
}

func jobSubmission(verb string, positionals []string, flags map[string][]string) (map[string]any, error) {
	if len(positionals) == 0 || (len(positionals) > 1 && positionals[1] != "--") {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq job "+verb+" CLUSTER --project-root PATH [--spec JSON | -- ARGV]", nil)
	}
	scheduling := map[string]any{}
	resources := map[string]any{}
	for flag, values := range flags {
		if len(values) != 1 || values[0] == "" {
			return nil, protocol.Fail("INVALID_INPUT", "Expected exactly one option value", map[string]any{"option": flag})
		}
		value := values[0]
		switch flag {
		case "--project-root", "--request-key", "--spec", "--registry", "--config", "--endpoint", "--token":
		case "--name", "--comment", "--partition", "--account":
			scheduling[strings.TrimPrefix(flag, "--")] = value
		case "--memory-bytes":
			resources["memory_bytes"] = value
		case "--nodes", "--tasks", "--cpus-per-task", "--time-limit-seconds":
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return nil, protocol.Fail("INVALID_INPUT", "Expected a positive integer", map[string]any{"option": flag})
			}
			resources[strings.ReplaceAll(strings.TrimPrefix(flag, "--"), "-", "_")] = n
		default:
			return nil, protocol.Fail("INVALID_INPUT", "Unknown job submission option", map[string]any{"option": flag})
		}
	}
	specText := first(flags["--spec"])
	if (specText != "") == (len(positionals) > 2) {
		return nil, protocol.Fail("INVALID_INPUT", "Provide exactly one of spec or argv", nil)
	}
	var spec any
	if specText != "" {
		if len(positionals) != 1 || len(scheduling)+len(resources) != 0 {
			return nil, protocol.Fail("INVALID_INPUT", "Provide scheduling and resources in --spec", nil)
		}
		value, err := readJSON(specText)
		if err != nil {
			return nil, err
		}
		spec = value
	} else {
		body := map[string]any{"execution": map[string]any{"units": []any{
			map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": anyList(positionals[2:])}},
		}}}
		if len(scheduling) > 0 {
			body["scheduling"] = scheduling
		}
		if len(resources) > 0 {
			body["resources"] = resources
		}
		spec = body
	}
	params := map[string]any{"cluster": positionals[0], "spec": spec, "project_root": first(flags["--project-root"])}
	if key := first(flags["--request-key"]); key != "" {
		if verb != "submit" {
			return nil, protocol.Fail("INVALID_INPUT", "--request-key requires job submit", nil)
		}
		params["request_key"] = key
	}
	return params, nil
}

func historyCommand(client *caller, positionals []string, flags map[string][]string) (any, error) {
	if len(positionals) != 1 {
		return nil, protocol.Fail("INVALID_INPUT", "usage: molq job history CLUSTER", nil)
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

func newCaller(flags map[string][]string, timeout time.Duration) (*caller, error) {
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
		Config:   resolved, Executable: executable, Timeout: timeout,
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
			flags[args[i]] = append(flags[args[i]], "")
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
