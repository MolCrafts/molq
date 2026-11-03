package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const helpText = `molq submits, inspects, and cancels local or HPC jobs.

Start with:
  molq ping            Ask the selected serve process for one hello
  molq health          Report ready or not, without explaining failures
  molq doctor          Explain a failed check: path and error
  molq --help          Show this text
  molq <command> --help
                       Show one command
  molq discover        Print the public method catalog and schemas

Jobs:
  molq clusters list
  molq clusters get CLUSTER
  molq clusters register --definition JSON
  molq clusters update CLUSTER --definition JSON --revision N
  molq clusters remove CLUSTER --revision N
  molq submit CLUSTER -- ARGV
  molq submit CLUSTER --spec JSON
  molq transfer [RSYNC_OPTION...] SRC... DEST
  molq status --ref JSON
  molq list CLUSTER
  molq observe CLUSTER
  molq history CLUSTER
  molq cancel --ref JSON
  molq logs --ref JSON
  molq wait --ref JSON
  molq purge [CLUSTER]

Process:
  molq serve           Listen for clients. Add --stdio for one local client
  molq watch           Ask a serve process for changes on a cadence
  molq rpc METHOD      Call any public method. --params takes JSON, @file, or @-

Where data lives:
  --registry PATH      Registry database. Default: ~/.molcrafts/molq/registry.db
  --config PATH        JSON or YAML settings. Default file is optional
  --endpoint URL       Use a running serve process instead of a local registry
  --token TEXT         Bearer token for --endpoint

JSON or @file values are the public request shapes from molq discover.
Commands print JSON, including errors, help, version, and doctor.
watch prints one JSON-RPC notification or error per line.

--name and --comment apply only to argv submission. With --spec, put them in
the spec. Give argv or --spec, not both. --ref is the JobRef, not the whole
submit response. Cancellation acceptance is not proof the job has stopped.
`

var commandHelp = map[string]string{
	"ping": `molq ping
  Ask the selected serve process for molq.hello. Nothing is submitted.
  Exit 0 and {"ok":true} means it answered. Any failure is the usual molq error.
  With --endpoint, ping that process. Otherwise ping the local registry.`,
	"health": `molq health
  Say whether this install is ready to use. Prints {"ok":true|false,"checks":[...]}.
  Each check is only a name and ok. A failed check is not explained here.
  Exit 0 means registry, configuration when present, and hello all passed.`,
	"doctor": `molq doctor
  Run the same checks as health and explain each one.
  A failed check includes its path and the error kind and message.
  Exit 0 means every check passed. Nothing is submitted.`,
	"discover": `molq discover
  Print the public method catalog, request schemas, and result types.`,
	"clusters": `molq clusters list
  molq clusters get CLUSTER
  molq clusters register --definition JSON
  molq clusters update CLUSTER --definition JSON --revision N
  molq clusters remove CLUSTER --revision N

  CLUSTER is a name or id. --definition is a cluster object, or @file.
  update and remove require the revision you last read.`,
	"submit": `molq submit CLUSTER -- ARGV
  molq submit CLUSTER --spec JSON [--request-key KEY]
  molq submit CLUSTER --name TEXT --comment TEXT -- ARGV

  CLUSTER is a name or id. Use argv or --spec, not both.
  --name and --comment are only for argv; a spec already carries scheduling.
  --request-key repeats the same submit inside one serve process.`,
	"transfer": `molq transfer [RSYNC_OPTION...] SRC... DEST

  rsync syntax. SRC and DEST are CLUSTER:PATH or a local PATH, the way rsync
  reads HOST:PATH: a colon before the first slash names a registered cluster.
  CLUSTER:relative is relative to that login's home; a local PATH is resolved
  against this shell's directory. A trailing slash is passed to rsync as is.
  Options go to rsync unchanged; nothing is implied, so write -a or --delete
  yourself. -e/--rsh, --rsync-path, -M/--remote-option and daemon options are
  owned by the cluster transport and rejected. Only one side may be remote.

  molq transfer -av --delete --exclude build/ ./project/ hpc:/scratch/you/project/
  molq transfer -a hpc:runs/out.xyz .`,
	"status": `molq status --ref JSON [--consistency live|bounded|cached]
  --ref is a JobRef object, or @file. live queries the scheduler.`,
	"list": `molq list CLUSTER [--state STATE] [--name NAME]
  --state may be repeated. Lists jobs currently known to that destination.`,
	"observe": `molq observe CLUSTER [--cursor TOKEN]
  One sample and its changes. Pass the returned cursor on the next call.`,
	"history": `molq history CLUSTER [--since TIME] [--until TIME] [--state STATE] [--name NAME] [--limit N]
  molq history CLUSTER --refs JSON
  Times are RFC3339 with a timezone. --refs cannot be combined with range filters.`,
	"cancel": `molq cancel --ref JSON [--request-key KEY]
  Asks the scheduler to cancel. An accepted result is not proof the job has stopped.`,
	"logs": `molq logs --ref JSON [--stream stdout|stderr] [--offset N] [--limit N]
  Read one log stream from the job's recorded location.`,
	"wait": `molq wait --ref JSON [--timeout SECONDS] [--interval SECONDS]
  Poll until the job is confirmed finished or the timeout expires.
  The job keeps running if this command stops.`,
	"purge": `molq purge [CLUSTER]
  Delete job directories whose newest change is older than the
  job_retention_days setting (default 30; 0 disables), on each cluster target
  and in the serve host's collected/ archive. Deleted outright, not archived.
  A listening serve process also does this at start and every 24 hours.
  An unreachable cluster is reported in its result and does not stop the rest.`,
	"rpc": `molq rpc METHOD [--params JSON]
  Call one public method. --params is a JSON object, @file, or @- for stdin.
  Method names and fields come from molq discover.`,
	"serve": `molq serve [--host ADDR] [--port N]
  molq serve --stdio
  Serve clients. Without --stdio, listen for HTTP and WebSocket.
  --host and --port override the configuration for this start.
  --stdio speaks JSON-RPC on stdin and stdout.`,
	"watch": `molq watch [--cluster NAME] [--interval SECONDS] [--once]
  Periodically ask the serve process for changes.
  --cluster may be repeated. The interval and notification target otherwise
  come from the configuration file. When --config is omitted, that file is
  ~/.molcrafts/molq/config.json if it exists.`,
}

func printHelp(args []string) {
	result := map[string]any{"command": "molq", "description": strings.TrimSpace(helpText), "commands": commandHelp}
	if len(args) > 0 {
		name := args[0]
		if name == "help" && len(args) > 1 {
			name = args[1]
		}
		if text, ok := commandHelp[name]; ok && name != "help" {
			result = map[string]any{"command": name, "description": text}
		}
	}
	encoded, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	fmt.Fprintln(os.Stdout, string(encoded))
}

func wantsHelp(args []string) bool {
	if len(args) == 0 {
		return true
	}
	if args[0] == "transfer" {
		// rsync reads -h as --human-readable unless it stands alone.
		return len(args) == 1 || args[1] == "--help" || len(args) == 2 && args[1] == "-h"
	}
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return args[0] == "help"
}
