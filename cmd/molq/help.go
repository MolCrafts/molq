package main

import (
	"fmt"
	"os"
	"strings"
)

const helpText = `molq submits, inspects, and cancels local or HPC jobs.

A cluster is a registered destination. A job is one allocation on a cluster.
The SSH host in ~/.ssh/config is how that cluster is reached. List those
separately:

  molq cluster list
  molq job list CLUSTER

Start with:
  molq --help          Show this text
  molq <command> --help
                       Show one command
  molq discover        Print the public method catalog and schemas
  molq ping            Ask the selected serve process for one hello
  molq health          Report ready or not, without explaining failures
  molq doctor          Explain a failed check: path and error

Clusters:
  molq cluster list
  molq cluster get CLUSTER
  molq cluster register --definition JSON
  molq cluster update CLUSTER --definition JSON --revision N
  molq cluster remove CLUSTER --revision N

Jobs:
  molq job submit CLUSTER --project-root PATH -- ARGV
  molq job submit CLUSTER --project-root PATH --spec JSON
  molq job preview CLUSTER --project-root PATH -- ARGV
  molq job list CLUSTER [--project-root PATH] [--state STATE] [--name NAME]
  molq job status --ref JSON
  molq job history CLUSTER
  molq job observe CLUSTER
  molq job cancel --ref JSON
  molq job logs --ref JSON
  molq job wait --ref JSON

Files:
  molq transfer [--transfer-deadline SECONDS] [RSYNC_OPTION...] SRC... DEST

Process:
  molq serve           Listen for clients. Add --stdio for one local client
  molq watch           Ask a serve process for changes on a cadence
  molq rpc METHOD      Call any public method. --params takes JSON, @file, or @-

Where data lives:
  --registry PATH      Registry database. Default: ~/.molcrafts/molq/registry.db
  --config PATH        JSON or YAML settings. Default file is optional
  --endpoint URL       Use a running serve process instead of a local registry
  --token TEXT         Bearer token for --endpoint
  --json               Print the JSON object even when stdout is a terminal

On a terminal, cluster list and job list print a table. A pipe, a redirect,
or --json prints one JSON object. --help is this text. --version is JSON.
watch prints one JSON-RPC notification or error per line. Exit codes are
unchanged.

JSON or @file values are the public request shapes from molq discover.
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
	"cluster": `molq cluster list
  molq cluster get CLUSTER
  molq cluster register --definition JSON
  molq cluster update CLUSTER --definition JSON --revision N
  molq cluster remove CLUSTER --revision N

  A cluster is a destination, not an SSH host. CLUSTER is a name or id.
  --definition is a cluster object, or @file. update and remove require the
  revision you last read. On a terminal, list prints a table; --json prints
  the JSON object.`,
	"job": `molq job submit CLUSTER --project-root PATH -- ARGV
  molq job submit CLUSTER --project-root PATH --spec JSON [--request-key KEY]
  molq job preview CLUSTER --project-root PATH [--spec JSON | -- ARGV]
  molq job list CLUSTER [--project-root PATH] [--state STATE] [--name NAME]
  molq job status --ref JSON [--consistency live|bounded|cached]
  molq job history CLUSTER [--since TIME] [--until TIME] [--state STATE] [--name NAME] [--limit N]
  molq job history CLUSTER --refs JSON
  molq job observe CLUSTER [--project-root PATH] [--cursor TOKEN]
  molq job cancel --ref JSON [--request-key KEY]
  molq job requeue --ref JSON
  molq job logs --ref JSON [--stream stdout|stderr] [--offset N] [--limit N]
  molq job wait --ref JSON [--timeout SECONDS] [--interval SECONDS]

  CLUSTER is a name or id. list shows jobs the scheduler currently knows.
  --state may be repeated. On a terminal, list prints a table; --json prints
  the JSON object. In argv mode, optional scheduling flags are --name,
  --comment, --partition and --account. Resource flags are --nodes, --tasks,
  --cpus-per-task, --memory-bytes and --time-limit-seconds. With --spec,
  include these fields in the spec. preview renders without submitting.
  --request-key reconciles the same submit across serve restarts.
  --ref is a JobRef object, or @file.
  History times are RFC3339 with a timezone. --refs cannot be combined with
  range filters. wait polls until the job is confirmed finished or the timeout
  expires; the job keeps running if this command stops. cancel asks the
  scheduler to cancel. An accepted result is not proof the job has stopped.`,
	"transfer": `molq transfer [--transfer-deadline SECONDS] [RSYNC_OPTION...] SRC... DEST

  rsync syntax. SRC and DEST are CLUSTER:PATH or a local PATH, the way rsync
  reads HOST:PATH: a colon before the first slash names a registered cluster.
  CLUSTER:relative is relative to that login's home; a local PATH is resolved
  against this shell's directory. A trailing slash is passed to rsync as is.
  --transfer-deadline sets the total copy limit (1–3600 seconds, default 90).
  rsync's own --timeout is passed through. -e/--rsh, --rsync-path,
  -M/--remote-option and daemon options are
  owned by the cluster transport and rejected. Only one side may be remote.

  molq transfer --transfer-deadline 600 -av ./project/ hpc:/scratch/you/project/
  molq transfer -a hpc:runs/out.xyz .`,
	"rpc": `molq rpc METHOD [--params JSON]
  Call one public method. --params is a JSON object, @file, or @- for stdin.
  Method names and fields come from molq discover.`,
	"serve": `molq serve [--host ADDR] [--port N]
  molq serve --stdio
  Serve clients. Without --stdio, listen for HTTP and WebSocket.
  --host and --port override the configuration for this start.
  --stdio speaks JSON-RPC on stdin and stdout.`,
	"watch": `molq watch [--cluster NAME] [--project-root PATH] [--interval SECONDS] [--once]
  Periodically ask the serve process for changes.
  --cluster may be repeated. The interval and notification target otherwise
  come from the configuration file. When --config is omitted, that file is
  ~/.molcrafts/molq/config.json if it exists.`,
}

func printHelp(args []string) {
	text := helpText
	if len(args) > 0 {
		name := args[0]
		if name == "help" && len(args) > 1 {
			name = args[1]
		}
		if name == "--help" || name == "-h" {
			name = ""
		}
		if specific, ok := commandHelp[name]; ok && name != "" && name != "help" {
			text = specific
		}
	}
	fmt.Fprintln(os.Stdout, strings.TrimSpace(text))
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
