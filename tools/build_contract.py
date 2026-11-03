"""Build the protocol contract from one declarative source."""

import json
from pathlib import Path


def obj(properties, required=()):
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


def ref(name):
    return {"$ref": f"#/$defs/{name}"}


string = {"type": "string", "minLength": 1, "maxLength": 4096}
identifier = {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"}
positive = {"type": "integer", "minimum": 1, "maximum": 1000000}
file_path = {"type": "string", "maxLength": 4096}
strings = {"type": "array", "items": string, "minItems": 1, "maxItems": 4096}
env = {
    "type": "object",
    "propertyNames": {"pattern": "^[A-Za-z_][A-Za-z0-9_]*$"},
    "additionalProperties": {"type": "string"},
}
command = {
    "oneOf": [
        obj(
            {
                "kind": {"const": "argv"},
                "argv": {**strings, "items": {**string, "minLength": 0}},
            },
            ["kind", "argv"],
        ),
        obj({"kind": {"const": "shell"}, "command": string}, ["kind", "command"]),
        obj(
            {
                "kind": {"const": "script"},
                "text": {"type": "string", "minLength": 1, "maxLength": 524288},
            },
            ["kind", "text"],
        ),
    ]
}
defs = {
    "Command": command,
    "Accelerator": obj(
        {
            "kind": {"enum": ["gpu", "nvidia_mps"]},
            "quantity": positive,
            "scope": {"enum": ["per_node", "total"]},
            "model": identifier,
        },
        ["kind", "quantity", "scope"],
    ),
    "Resources": obj(
        {
            "nodes": positive,
            "tasks": positive,
            "cpus_per_task": positive,
            "memory_bytes": {"type": "string", "pattern": "^[1-9][0-9]{0,19}$"},
            "time_limit_seconds": positive,
            "accelerators": {
                "type": "array",
                "items": ref("Accelerator"),
                "maxItems": 16,
            },
        }
    ),
    "FileLocation": obj({"cluster": string, "path": file_path}, ["path"]),
    "JobRef": obj(
        {
            "registry_id": string,
            "cluster_id": string,
            "native_id": {
                "type": "string",
                "pattern": "^[A-Za-z0-9][A-Za-z0-9_.;-]{0,255}$",
            },
            "incarnation": string,
        },
        ["registry_id", "cluster_id", "native_id", "incarnation"],
    ),
    "Dependency": obj(
        {
            "ref": ref("JobRef"),
            "condition": {
                "enum": ["after", "after_success", "after_failure", "after_started"]
            },
        },
        ["ref", "condition"],
    ),
    "Scheduling": obj(
        {
            "name": identifier,
            "comment": {"type": "string", "maxLength": 4094},
            "partition": identifier,
            "account": identifier,
            "qos": identifier,
            "reservation": identifier,
            "priority": {"type": "integer", "minimum": 0, "maximum": 1000000},
            "exclusive": {"type": "boolean"},
            "dependencies": {
                "type": "array",
                "items": ref("Dependency"),
                "maxItems": 256,
            },
        }
    ),
    "Launch": obj(
        {"kind": {"enum": ["direct", "mpi"]}, "ranks": positive, "threads": positive},
        ["kind"],
    ),
    "Placement": obj({"tasks": positive, "cpus_per_task": positive}),
    "ExecutionUnit": obj(
        {
            "id": identifier,
            "command": ref("Command"),
            "env": env,
            "cwd": string,
            "stdout": string,
            "stderr": string,
            "launch": ref("Launch"),
            "placement": ref("Placement"),
        },
        ["id", "command"],
    ),
    "Plan": {
        "oneOf": [
            obj({"kind": {"const": "unit"}, "ref": identifier}, ["kind", "ref"]),
            obj(
                {
                    "kind": {"enum": ["sequence", "parallel"]},
                    "children": {
                        "type": "array",
                        "items": ref("Plan"),
                        "minItems": 1,
                        "maxItems": 256,
                    },
                },
                ["kind", "children"],
            ),
        ]
    },
    "Execution": obj(
        {
            "units": {
                "type": "array",
                "items": ref("ExecutionUnit"),
                "minItems": 1,
                "maxItems": 256,
            },
            "plan": ref("Plan"),
            "env": env,
            "cwd": string,
        },
        ["units"],
    ),
    "JobSpec": obj(
        {
            "resources": ref("Resources"),
            "scheduling": ref("Scheduling"),
            "execution": ref("Execution"),
        },
        ["execution"],
    ),
    "Defaults": obj(
        {
            "resources": ref("Resources"),
            "scheduling": ref("Scheduling"),
            "env": env,
            "cwd": string,
        }
    ),
    "TransportDefinition": {
        "oneOf": [
            obj({"kind": {"const": "local"}}, ["kind"]),
            obj({"kind": {"const": "ssh"}, "alias": identifier}, ["kind", "alias"]),
        ]
    },
    "ClusterDefinition": obj(
        {
            "name": identifier,
            "scheduler": {"enum": ["shell", "slurm", "pbs", "lsf"]},
            "transport": ref("TransportDefinition"),
            "defaults": ref("Defaults"),
            "launcher": {"enum": ["srun", "mpirun", "mpiexec"]},
            "pbs_dialect": {"enum": ["openpbs"]},
        },
        ["name", "scheduler"],
    ),
}
defs.update(
    {
        "ErrorData": obj(
            {
                "kind": string,
                "message": {"type": "string"},
                "outcome": {"enum": ["not_applied", "applied", "unknown"]},
                "context": {"type": "object"},
            },
            ["kind", "message", "outcome", "context"],
        ),
        "Freshness": obj(
            {
                "status": {"enum": ["live", "cached", "stale", "unavailable"]},
                "age_seconds": {"type": "number", "minimum": 0},
            },
            ["status", "age_seconds"],
        ),
        "Completion": obj(
            {
                "all_confirmed_terminal": {"type": "boolean"},
                "successful": {"type": ["boolean", "null"]},
            },
            ["all_confirmed_terminal", "successful"],
        ),
        "JobSnapshot": obj(
            {
                "ref": ref("JobRef"),
                "state": {
                    "enum": [
                        "queued",
                        "running",
                        "succeeded",
                        "failed",
                        "cancelled",
                        "timed_out",
                        "unknown",
                    ]
                },
                "terminal": {"type": "boolean"},
                "observed_at": string,
                "freshness": ref("Freshness"),
                "source": {
                    "enum": [
                        "queue",
                        "accounting",
                        "shell_receipt",
                        "absent",
                        "unavailable",
                    ]
                },
                "raw_state": {"type": "string"},
                "exit_code": {"type": ["integer", "null"]},
                **{
                    field: {"type": ["string", "null"]}
                    for field in (
                        "name",
                        "comment",
                        "submitted_at",
                        "started_at",
                        "ended_at",
                    )
                },
                "error": ref("ErrorData"),
                "last_known": ref("JobSnapshot"),
            },
            [
                "ref",
                "state",
                "terminal",
                "observed_at",
                "freshness",
                "source",
                "raw_state",
                "exit_code",
            ],
        ),
        "Coverage": obj(
            {
                "complete": {"type": "boolean"},
                "errors": {"type": "array", "items": ref("ErrorData")},
            },
            ["complete", "errors"],
        ),
        "ObservedChange": obj(
            {
                "cursor": string,
                "observed_at": string,
                "ref": ref("JobRef"),
                "previous_state": {"type": ["string", "null"]},
                "state": string,
                "terminal": {"type": "boolean"},
            },
            ["cursor", "observed_at", "ref", "previous_state", "state", "terminal"],
        ),
        "ObservationResult": obj(
            {
                "query": obj(
                    {
                        "since": string,
                        "until": string,
                        "time_field": {"const": "ended_at"},
                        "account_scope": {"const": "current_target_user"},
                        "source": {"const": "accounting"},
                        "observed_at": string,
                        "freshness": ref("Freshness"),
                        "limit": {"type": "integer"},
                    },
                    [
                        "since",
                        "until",
                        "time_field",
                        "account_scope",
                        "source",
                        "observed_at",
                        "freshness",
                        "limit",
                    ],
                ),
                "snapshots": {"type": "array", "items": ref("JobSnapshot")},
                "coverage": ref("Coverage"),
                "completion": ref("Completion"),
                "changes": {"type": "array", "items": ref("ObservedChange")},
                "cursor": string,
                "resync": {"type": "boolean"},
            },
            ["snapshots", "coverage", "completion"],
        ),
        "ClusterRecord": obj(
            {
                **defs["ClusterDefinition"]["properties"],
                "id": string,
                "revision": positive,
            },
            ["name", "scheduler", "id", "revision"],
        ),
    }
)
defs["Resources"]["properties"]["memory_bytes"]["description"] = (
    "Minimum memory per allocated node, decimal bytes; Scheduler rounds upward to its native granularity."
)

consistency = {"enum": ["live", "bounded", "cached"]}
query = {
    "refs": {"type": "array", "items": ref("JobRef"), "minItems": 1, "maxItems": 1024},
    "consistency": consistency,
    "max_age": {"type": "number", "minimum": 0, "maximum": 86400},
}
params = {
    "molq.hello": obj({}),
    "schema.discover": obj({}),
    "clusters.list": obj({}),
    "clusters.get": obj({"cluster": string}, ["cluster"]),
    "clusters.register": ref("ClusterDefinition"),
    "clusters.update": obj(
        {
            "cluster": string,
            "definition": ref("ClusterDefinition"),
            "expected_revision": positive,
        },
        ["cluster", "definition", "expected_revision"],
    ),
    "clusters.remove": obj(
        {"cluster": string, "expected_revision": positive},
        ["cluster", "expected_revision"],
    ),
    "jobs.validate": obj(
        {"cluster": string, "spec": ref("JobSpec")}, ["cluster", "spec"]
    ),
    "jobs.preview": obj(
        {"cluster": string, "spec": ref("JobSpec")}, ["cluster", "spec"]
    ),
    "jobs.submit": obj(
        {"cluster": string, "spec": ref("JobSpec"), "request_key": identifier},
        ["cluster", "spec"],
    ),
    "jobs.get": obj(
        {
            "ref": ref("JobRef"),
            "consistency": consistency,
            "max_age": {"type": "number", "minimum": 0, "maximum": 86400},
        },
        ["ref"],
    ),
    "jobs.get_many": obj(query, ["refs"]),
    "jobs.observe": obj({**query, "cluster": string, "cursor": string}),
    "jobs.list": obj(
        {
            "cluster": string,
            "states": {
                "type": "array",
                "items": defs["JobSnapshot"]["properties"]["state"],
                "minItems": 1,
                "uniqueItems": True,
            },
            "name": string,
        },
        ["cluster"],
    ),
    "jobs.history": {
        "oneOf": [
            obj({"cluster": string, "refs": query["refs"]}, ["cluster", "refs"]),
            obj(
                {
                    "cluster": string,
                    "since": string,
                    "until": string,
                    "states": {
                        "type": "array",
                        "items": {
                            "enum": ["succeeded", "failed", "cancelled", "timed_out"]
                        },
                        "minItems": 1,
                        "uniqueItems": True,
                    },
                    "name": string,
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1024},
                },
                ["cluster"],
            ),
        ]
    },
    "jobs.cancel": obj({"ref": ref("JobRef"), "request_key": identifier}, ["ref"]),
    "jobs.cancel_many": obj({"refs": query["refs"]}, ["refs"]),
    "jobs.purge": obj({"cluster": string}),
    "logs.read": obj(
        {
            "ref": ref("JobRef"),
            "stream": {"enum": ["stdout", "stderr"]},
            "offset": {"type": "integer", "minimum": 0},
            "limit": {"type": "integer", "minimum": 1, "maximum": 262144},
        },
        ["ref"],
    ),
    "files.transfer": obj(
        {
            "sources": {
                "type": "array",
                "items": ref("FileLocation"),
                "minItems": 1,
                "maxItems": 64,
            },
            "destination": ref("FileLocation"),
            "options": {
                "type": "array",
                "items": {**string, "pattern": "^-"},
                "maxItems": 256,
            },
        },
        ["sources", "destination"],
    ),
    "files.stat": obj({"cluster": string, "path": file_path}, ["cluster", "path"]),
    "files.list": obj({"cluster": string, "path": file_path}, ["cluster", "path"]),
    "files.read": obj(
        {
            "cluster": string,
            "path": file_path,
            "offset": {"type": "integer", "minimum": 0},
            "limit": {"type": "integer", "minimum": 0, "maximum": 262144},
        },
        ["cluster", "path"],
    ),
    "files.write": obj(
        {
            "cluster": string,
            "path": file_path,
            "data": {"type": "string", "maxLength": 349528},
            "offset": {"type": "integer", "minimum": 0},
        },
        ["cluster", "path", "data"],
    ),
    "files.mkdir": obj(
        {
            "cluster": string,
            "path": file_path,
            "parents": {"type": "boolean"},
            "exist_ok": {"type": "boolean"},
            "mode": {"type": "integer", "minimum": 0, "maximum": 4095},
        },
        ["cluster", "path"],
    ),
    "files.remove": obj(
        {"cluster": string, "path": file_path, "directory": {"type": "boolean"}},
        ["cluster", "path"],
    ),
    "files.rename": obj(
        {"cluster": string, "path": file_path, "target": file_path},
        ["cluster", "path", "target"],
    ),
    "files.touch": obj(
        {"cluster": string, "path": file_path, "exist_ok": {"type": "boolean"}},
        ["cluster", "path"],
    ),
    "config.reload": obj({}),
    "config.get": obj({"key": identifier}, ["key"]),
    "config.set": obj({"key": identifier, "value": {}}, ["key", "value"]),
    "presets.list": obj({}),
    "presets.get": obj({"key": identifier}, ["key"]),
    "presets.set": obj({"key": identifier, "value": ref("Defaults")}, ["key", "value"]),
    "events.subscribe": obj({"cursor": string}),
}
# Generic target I/O: no workspace vocabulary or scheduler initialization.
for _name, _schema in params.items():
    if _name.startswith("files.") and _name != "files.transfer":
        _schema["properties"]["root"] = file_path
params["files.list"]["properties"].update(
    {
        "with_stat": {"type": "boolean"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 512},
        "after": string,
    }
)
params["files.remove"]["properties"]["recursive"] = {"type": "boolean"}
params["files.chmod"] = obj(
    {
        "cluster": string,
        "path": file_path,
        "root": file_path,
        "mode": {"type": "integer", "minimum": 0, "maximum": 4095},
    },
    ["cluster", "path", "mode"],
)
params["files.symlink"] = obj(
    {"cluster": string, "path": file_path, "root": file_path, "target": file_path},
    ["cluster", "path", "target"],
)
params["files.resolve"] = obj(
    {"cluster": string, "path": file_path, "root": file_path}, ["cluster", "path"]
)
params["files.read_many"] = obj(
    {
        "cluster": string,
        "root": file_path,
        "paths": {"type": "array", "items": file_path, "minItems": 1, "maxItems": 128},
        "max_bytes": {"type": "integer", "minimum": 1, "maximum": 262144},
    },
    ["cluster", "paths"],
)
params["connections.authenticate"] = obj(
    {"cluster": string, "code": {"type": "string", "minLength": 1, "maxLength": 4096}},
    ["cluster", "code"],
)
params["files.transfer"]["properties"]["timeout_seconds"] = {
    "type": "integer",
    "minimum": 1,
    "maximum": 3600,
}
defs["FileLocation"]["properties"]["root"] = file_path

contract = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://molcrafts.org/molq/protocol",
    "$defs": defs,
    "methods": params,
    "release": "0.9.0",
}
text = json.dumps(contract, indent=2) + "\n"
root = Path(__file__).resolve().parents[1]
(root / "src/molq/protocol/contract.json").write_text(text)
(root / "protocol/contract.json").write_text(text)
