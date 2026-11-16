package main

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/MolCrafts/molq/protocol"
)

const transferUsage = "usage: molq transfer [--transfer-deadline SECONDS] [RSYNC_OPTION...] SRC... DEST (SRC/DEST: CLUSTER:PATH or a local PATH)"

// leadingGlobals takes the connection flags peelGlobals moved to the front and
// leaves every remaining word to rsync untouched.
func leadingGlobals(args []string) (map[string][]string, []string) {
	globals := map[string]bool{"--registry": true, "--config": true, "--endpoint": true, "--token": true, "--transfer-deadline": true}
	flags := map[string][]string{}
	for len(args) >= 2 && globals[args[0]] {
		flags[args[0]] = append(flags[args[0]], args[1])
		args = args[2:]
	}
	return flags, args
}

func transferDeadline(values []string) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}
	seconds, err := strconv.Atoi(values[0])
	if len(values) != 1 || err != nil || seconds < 1 || seconds > 3600 {
		return 0, protocol.Fail("INVALID_INPUT", "--transfer-deadline requires one value from 1 to 3600 seconds", nil)
	}
	return seconds, nil
}

func transferCommand(client *caller, args []string, seconds int) (any, error) {
	params, err := transferParams(args)
	if err != nil {
		return nil, err
	}
	if seconds != 0 {
		params["timeout_seconds"] = seconds
	}
	return client.Call("files.transfer", params)
}

// transferParams reads an rsync command line. The last operand is the
// destination; CLUSTER:PATH names a registered cluster the way rsync's
// HOST:PATH names a host.
func transferParams(args []string) (map[string]any, error) {
	options, operands, err := protocol.SplitRsyncArgs(args)
	if err != nil {
		return nil, err
	}
	if len(operands) < 2 {
		return nil, protocol.Fail("INVALID_INPUT", transferUsage, nil)
	}
	for _, option := range options {
		if err := protocol.CheckRsyncOption(option); err != nil {
			return nil, err
		}
	}
	locations := make([]any, len(operands))
	for i, operand := range operands {
		location, err := transferLocation(operand)
		if err != nil {
			return nil, err
		}
		locations[i] = location
	}
	return map[string]any{
		"sources": locations[:len(locations)-1], "destination": locations[len(locations)-1],
		"options": anyList(options),
	}, nil
}

// transferLocation applies rsync's rule: a colon before the first slash makes
// the operand remote. A local path is made absolute against this shell's
// working directory, keeping a trailing slash.
func transferLocation(operand string) (map[string]any, error) {
	if strings.HasPrefix(operand, "rsync://") || strings.Contains(operand, "::") {
		return nil, protocol.Fail("INVALID_INPUT", "rsync daemon operands are not cluster paths", map[string]any{"operand": operand})
	}
	if name, rest, found := strings.Cut(operand, ":"); found && name != "" && !strings.Contains(name, "/") {
		return map[string]any{"cluster": name, "path": rest}, nil
	}
	absolute, err := filepath.Abs(operand)
	if err != nil {
		return nil, protocol.Fail("INVALID_INPUT", "Cannot resolve local transfer path", map[string]any{"operand": operand})
	}
	absolute = filepath.ToSlash(absolute)
	if strings.HasSuffix(operand, "/") && !strings.HasSuffix(absolute, "/") {
		absolute += "/"
	}
	return map[string]any{"path": absolute}, nil
}
