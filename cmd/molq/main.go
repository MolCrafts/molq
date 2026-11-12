package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mattn/go-isatty"

	"github.com/MolCrafts/molq/watch"

	"github.com/MolCrafts/molq/config"
	"github.com/MolCrafts/molq/protocol"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if result, ok := err.(commandExit); ok {
			os.Exit(result.code)
		}
		if item, ok := err.(*protocol.Error); ok {
			emit(item)
			os.Exit(exitCode(item.Kind))
		}
		emit(protocol.Fail("INVALID_INPUT", err.Error(), nil))
		os.Exit(2)
	}
}

type commandExit struct{ code int }

func (e commandExit) Error() string { return fmt.Sprintf("command exit %d", e.code) }

var forceJSON bool

func peelJSON(args []string) ([]string, bool) {
	force := false
	var kept []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			kept = append(kept, args[i:]...)
			break
		}
		if args[i] == "--json" {
			force = true
			continue
		}
		kept = append(kept, args[i])
	}
	return kept, force
}

func run(args []string) error {
	args, forceJSON = peelJSON(args)
	args = peelGlobals(args)
	if wantsHelp(args) {
		printHelp(args)
		return nil
	}
	if len(args) > 0 && (args[0] == "--version" || args[0] == "version") {
		fmt.Fprintln(os.Stdout, `{"jsonrpc":"2.0","id":1,"result":{"version":"0.9.0"}}`)
		return nil
	}
	if len(args) == 0 {
		printHelp(nil)
		return nil
	}
	switch args[0] {
	case "ping":
		return runPing(args[1:])
	case "health":
		return runHealth(args[1:])
	case "doctor":
		return runDoctor(args[1:])
	}
	if args[0] == "rpc" {
		return callRPC(args[1:])
	}
	if args[0] == "watch" {
		return runWatch(args[1:])
	}
	if args[0] != "serve" {
		return runCommand(args[0], args[1:])
	}
	return runServe(args[1:])
}

func runWatch(args []string) error {
	flags := flag.NewFlagSet("watch", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "", "configuration file")
	registryPath := flags.String("registry", "", "registry database")
	endpoint := flags.String("endpoint", "", "explicit RPC endpoint")
	token := flags.String("token", "", "RPC bearer token")
	interval := flags.Float64("interval", 0, "seconds between observations")
	once := flags.Bool("once", false, "observe once and exit")
	var clusters stringsFlag
	flags.Var(&clusters, "cluster", "cluster to observe")
	if err := flags.Parse(args); err != nil {
		return protocol.Fail("INVALID_INPUT", err.Error(), nil)
	}
	file, _, err := resolveConfig(*configPath)
	if err != nil {
		return err
	}
	wire, err := newCaller(map[string][]string{
		"--config": {*configPath}, "--registry": {*registryPath},
		"--endpoint": {*endpoint}, "--token": {*token},
	})
	if err != nil {
		return err
	}
	defer wire.Close()
	names := file.Client.Watch.Clusters
	if len(clusters) > 0 {
		names = clusters
	}
	every := time.Duration(file.Client.Watch.Interval * float64(time.Second))
	if *interval > 0 {
		every = time.Duration(*interval * float64(time.Second))
	}
	err = watch.Run(context.Background(), wire, watch.Options{
		Interval: every, Clusters: names, NerveEndpoint: file.Client.Watch.NerveEndpoint,
		Once: *once, Out: os.Stdout,
	})
	var reported *watch.ReportedError
	if errors.As(err, &reported) {
		var failure *protocol.Error
		if errors.As(reported.Err, &failure) {
			return commandExit{exitCode(failure.Kind)}
		}
		return commandExit{1}
	}
	return err
}

type stringsFlag []string

func (s *stringsFlag) String() string { return strings.Join(*s, ",") }

func (s *stringsFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func peelGlobals(args []string) []string {
	globals := map[string]bool{"--registry": true, "--config": true, "--endpoint": true, "--token": true}
	var flags []string
	var positionals []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			positionals = append(positionals, args[i:]...)
			break
		}
		if globals[args[i]] && i+1 < len(args) {
			flags = append(flags, args[i], args[i+1])
			i++
			continue
		}
		positionals = append(positionals, args[i])
	}
	if len(positionals) == 0 {
		return flags
	}
	return append(append([]string{positionals[0]}, flags...), positionals[1:]...)
}

func callRPC(args []string) error {
	flags, positionals := splitArgs(args)
	if len(positionals) != 1 {
		return protocol.Fail("INVALID_INPUT", "usage: molq rpc METHOD", nil)
	}
	method := positionals[0]
	known := false
	for _, name := range protocol.Current().Methods() {
		if name == method {
			known = true
		}
	}
	if !known {
		return protocol.Fail("METHOD_NOT_FOUND", "Unknown RPC method", map[string]any{"method": method})
	}
	paramsText := first(flags["--params"])
	if paramsText == "" {
		paramsText = "{}"
	}
	parsed, err := readJSON(paramsText)
	if err != nil {
		return err
	}
	body, ok := parsed.(map[string]any)
	if !ok {
		return protocol.Fail("INVALID_INPUT", "Named RPC params must be an object", nil)
	}
	wire, err := newCaller(flags)
	if err != nil {
		return err
	}
	defer wire.Close()
	result, err := wire.Call(method, body)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(encoded))
	if hasCoverageErrors(result) {
		return commandExit{8}
	}
	return nil
}

func hasCoverageErrors(result any) bool {
	body, ok := result.(map[string]any)
	if !ok {
		return false
	}
	if body["partial_failure"] == true {
		return true
	}
	coverage, ok := body["coverage"].(map[string]any)
	if !ok {
		return false
	}
	switch errors := coverage["errors"].(type) {
	case []any:
		return len(errors) > 0
	case []map[string]any:
		return len(errors) > 0
	default:
		return false
	}
}

// resolveConfig loads --config, or ~/.molcrafts/molq/config.json.
// A missing file starts empty and can be written through RPC. An invalid file
// is an error. The returned path is what serve should validate.
func resolveConfig(explicit string) (config.File, string, error) {
	path := explicit
	if path == "" {
		path = config.DefaultPath()
	}
	file, err := config.Load(path)
	if err != nil {
		return config.File{}, path, err
	}
	return file, path, nil
}

func emit(err *protocol.Error) {
	if !forceJSON && isatty.IsTerminal(os.Stdout.Fd()) {
		fmt.Fprint(os.Stdout, formatError(err))
		return
	}
	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      nil,
		"error":   map[string]any{"code": -32000, "message": err.Error(), "data": err.Data()},
	}
	encoded, _ := json.Marshal(payload)
	fmt.Fprintln(os.Stdout, string(encoded))
}

func exitCode(kind string) int {
	codes := map[string]int{
		"INVALID_INPUT": 2, "METHOD_NOT_FOUND": 2,
		"CLUSTER_NOT_FOUND": 3, "CACHE_MISS": 3, "FILE_UNAVAILABLE": 3,
		"RUNTIME_UNAVAILABLE": 4, "SCHEDULER_UNAVAILABLE": 4, "TRANSPORT_UNAVAILABLE": 4,
		"SCHEDULER_UNSUPPORTED": 5, "TRANSFER_UNSUPPORTED": 5, "RESOURCE_REQUEST_INVALID": 5,
		"EXECUTION_RESOURCE_CONFLICT": 5, "RPC_CHANNEL_UNSUPPORTED": 5,
		"STATE_SCHEMA_INCOMPATIBLE": 5, "WAIT_TIMEOUT": 6,
		"AUTH_REQUIRED": 7, "PERMISSION_DENIED": 7, "SUBMISSION_REJECTED": 7, "CANCEL_REJECTED": 7,
		"PARTIAL_FAILURE": 8, "CONFLICT": 10, "STATE_BUSY": 10, "OUTCOME_UNKNOWN": 10,
		"REGISTRY_MISMATCH": 10, "JOB_IDENTITY_MISMATCH": 10, "JOB_IDENTITY_UNVERIFIABLE": 10,
	}
	if code, ok := codes[kind]; ok {
		return code
	}
	return 1
}
