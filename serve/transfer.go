package serve

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

const transferTimeout = 90 * time.Second

// location is one rsync operand: a path on a registered cluster, or a path on
// the serve host when no cluster is named.
type location struct {
	clusterID string
	transport map[string]any
	path      string
	root      string
}

func (l location) ssh() bool { return l.transport["kind"] == "ssh" }

// operand spells the location the way rsync reads it from the side that runs.
func (l location) operand(remote bool) string {
	if remote {
		return stringField(l.transport["alias"]) + ":" + l.path
	}
	if l.path == "" {
		return "."
	}
	return l.path
}

func (s *Service) transfer(ctx context.Context, params map[string]any) (any, error) {
	var sources []location
	for _, item := range anySlice(params["sources"]) {
		source, err := s.location(ctx, mapValue(item))
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	if len(sources) == 0 {
		return nil, protocol.Fail("INVALID_INPUT", "Transfer requires at least one source", nil)
	}
	destination, err := s.location(ctx, mapValue(params["destination"]))
	if err != nil {
		return nil, err
	}
	args := []string{"rsync"}
	for _, option := range anySlice(params["options"]) {
		word, _ := option.(string)
		if err := protocol.CheckRsyncOption(word); err != nil {
			return nil, err
		}
		args = append(args, word)
	}
	for _, source := range sources[1:] {
		if !sameTransport(source.transport, sources[0].transport) {
			return nil, protocol.Fail("INVALID_INPUT", "All transfer sources must be on one host", nil)
		}
	}
	runner, rsh, operands, err := transferRoute(sources, destination)
	if err != nil {
		return nil, err
	}
	args = append(append(append(args, rsh...), "--"), operands...)
	release, err := s.acquireTransfer(ctx, sources[0], destination)
	if err != nil {
		return nil, err
	}
	defer release()
	for _, item := range append(append([]location{}, sources...), destination) {
		if item.root != "" {
			if err := validateScopedTransfer(ctx, transport.Create(item.transport), item.path, item.root); err != nil {
				return nil, err
			}
		}
	}
	deadline := transferTimeout
	if seconds, ok := asInt(params["timeout_seconds"]); ok {
		deadline = time.Duration(seconds) * time.Second
	}
	result, err := runner.Run(ctx, args, deadline, nil, false)
	if err != nil {
		if failure, ok := err.(*protocol.Error); ok && failure.Context["dispatched"] == true {
			return nil, protocol.FailOutcome("OUTCOME_UNKNOWN", "Target transfer outcome is uncertain", "unknown", failure.Context)
		}
		return nil, err
	}
	if result.ReturnCode != 0 {
		return nil, protocol.FailOutcome("FILE_UNAVAILABLE", "Target rsync transfer failed", "unknown", map[string]any{
			"returncode": result.ReturnCode, "stderr": strings.ToValidUTF8(string(result.Stderr), "�"),
		})
	}
	return map[string]any{"outcome": "applied", "output": result.Text()}, nil
}

// transferRoute chooses the side that runs rsync. One shared host runs it
// there with plain paths; otherwise the serve host runs it and names the SSH
// side by alias, as rsync itself allows exactly one remote side. The returned
// rsh options must precede the operand separator.
func transferRoute(sources []location, destination location) (transport.Transport, []string, []string, error) {
	operands := make([]string, 0, len(sources)+1)
	if sameTransport(sources[0].transport, destination.transport) {
		for _, source := range sources {
			operands = append(operands, source.operand(false))
		}
		if len(sources) == 1 && path.Clean(sources[0].operand(false)) == path.Clean(destination.operand(false)) {
			return nil, nil, nil, protocol.Fail("INVALID_INPUT", "Transfer source and destination are the same path", nil)
		}
		return transport.Create(sources[0].transport), nil, append(operands, destination.operand(false)), nil
	}
	if sources[0].ssh() && destination.ssh() {
		return nil, nil, nil, protocol.Fail("TRANSFER_UNSUPPORTED", "Two distinct SSH targets require explicit staging; no remote alias guessing", nil)
	}
	// Older rsync clients pass remote operands through the login shell.
	// A scoped operand must not expand into paths the scope check never saw.
	for _, item := range append(append([]location{}, sources...), destination) {
		if item.ssh() && item.root != "" && strings.ContainsAny(item.path, " \t\r\n*?[]\\'\"$`;&|<>(){}!#~") {
			return nil, nil, nil, protocol.Fail("INVALID_INPUT", "Scoped SSH transfer requires a literal shell-safe path", nil)
		}
	}
	for _, source := range sources {
		operands = append(operands, source.operand(source.ssh()))
	}
	operands = append(operands, destination.operand(destination.ssh()))
	alias := stringField(destination.transport["alias"])
	if sources[0].ssh() {
		alias = stringField(sources[0].transport["alias"])
	}
	sshArgs, err := transport.SSHCommand(alias)
	if err != nil {
		return nil, nil, nil, err
	}
	quoted := make([]string, len(sshArgs))
	for i, word := range sshArgs {
		quoted[i] = "'" + strings.ReplaceAll(word, "'", "'\"'\"'") + "'"
	}
	return transport.Local{}, []string{"--rsh=" + strings.Join(quoted, " ")}, operands, nil
}

// location resolves one wire FileLocation. A path on a cluster follows rsync's
// host:path rule and is relative to that login's home; a serve-host path is
// absolute because the serve process has no meaningful working directory.
func (s *Service) location(ctx context.Context, value map[string]any) (location, error) {
	requested, _ := value["path"].(string)
	if strings.ContainsAny(requested, "\n\r") {
		return location{}, protocol.Fail("INVALID_INPUT", "Transfer paths are single-line", nil)
	}
	name := stringField(value["cluster"])
	if name == "" {
		if !strings.HasPrefix(requested, "/") {
			return location{}, protocol.Fail("INVALID_INPUT", "Serve-host transfer paths must be absolute", map[string]any{"path": requested})
		}

		return location{transport: map[string]any{"kind": "local"}, path: requested, root: stringField(value["root"])}, checkRoot(requested)
	}
	cluster, _, err := s.fileBinding(name)
	if err != nil {
		return location{}, err
	}
	binding := clusterTransport(cluster)
	resolved, err := clusterPath(binding, requested)
	if err != nil {
		return location{}, err
	}

	return location{clusterID: stringField(cluster["id"]), transport: binding, path: resolved, root: stringField(value["root"])}, checkRoot(resolved)
}

// clusterPath makes a local-transport path absolute against the serve user's
// home, the directory an SSH login would start in.
func clusterPath(binding map[string]any, requested string) (string, error) {
	if binding["kind"] == "ssh" || strings.HasPrefix(requested, "/") {
		return requested, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", protocol.Fail("FILE_UNAVAILABLE", "Cannot resolve the home directory for a relative path", nil)
	}
	if requested == "~" {
		requested = ""
	} else {
		requested = strings.TrimPrefix(requested, "~/")
	}
	// Preserve components for the target-side root constraint, including `..`.
	resolved := strings.TrimSuffix(home, "/") + "/" + requested
	return resolved, nil
}

func checkRoot(requested string) error {
	if strings.HasPrefix(requested, "/") && path.Clean(requested) == "/" {
		return protocol.Fail("INVALID_INPUT", "Transfer of the filesystem root is rejected", nil)
	}
	return nil
}

// acquireTransfer takes the concurrency slot of the first cluster involved.
// Serve-host-only copies touch no cluster and take none.
func (s *Service) acquireTransfer(ctx context.Context, source, destination location) (func(), error) {
	for _, id := range []string{source.clusterID, destination.clusterID} {
		if id != "" {
			return s.acquire(ctx, id)
		}
	}
	return func() {}, nil
}

func clusterTransport(cluster map[string]any) map[string]any {
	binding := mapValue(cluster["transport"])
	if binding["kind"] == "ssh" {
		return binding
	}
	return map[string]any{"kind": "local"}
}

func sameTransport(left, right map[string]any) bool {
	return left["kind"] == right["kind"] && left["alias"] == right["alias"]
}
