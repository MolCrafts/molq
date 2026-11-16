"""Generate wire DTOs and async projection from canonical contract/projection."""

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
contract = json.loads((ROOT / "src/molq/protocol/contract.json").read_text())
py = [
    '"""Generated from protocol/contract.json; edit its declarative source."""',
    "from __future__ import annotations",
    "from typing import Any, Literal, NotRequired, TypedDict",
    "",
]
ts = ["// Generated from protocol/contract.json. Do not edit."]


def type_of(schema, name, language):
    if "$ref" in schema:
        return schema["$ref"].rsplit("/", 1)[1]
    if "const" in schema or "enum" in schema:
        values = [schema["const"]] if "const" in schema else schema["enum"]
        return (
            ("Literal[" + ", ".join(repr(v) for v in values) + "]")
            if language == "py"
            else " | ".join(json.dumps(v) for v in values)
        )
    if "oneOf" in schema:
        return " | ".join(
            type_of(s, name + str(i), language) for i, s in enumerate(schema["oneOf"])
        )
    kind = schema.get("type")
    if isinstance(kind, list):
        return " | ".join(
            "None"
            if k == "null" and language == "py"
            else "null"
            if k == "null"
            else type_of({**schema, "type": k}, name, language)
            for k in kind
        )
    if kind == "object":
        if "properties" not in schema:
            item = (
                type_of(
                    schema.get("additionalProperties", {}), name + "Value", language
                )
                if isinstance(schema.get("additionalProperties", {}), dict)
                else ("Any" if language == "py" else "unknown")
            )
            return (
                f"dict[str, {item}]" if language == "py" else f"Record<string, {item}>"
            )
        if language == "ts":
            return (
                "{ "
                + "; ".join(
                    f"{k}{'' if k in schema.get('required', []) else '?'}: {type_of(v, name + k, language)}"
                    for k, v in schema["properties"].items()
                )
                + " }"
            )
        fields = []
        for key, value in schema["properties"].items():
            field = type_of(value, name + key, language)
            fields.append(
                f"    {key}: {field if key in schema.get('required', []) else 'NotRequired[' + field + ']'}"
            )
        py.extend(
            [
                f"class {name}(TypedDict):",
                '    """Canonical wire shape."""',
                *(fields or ["    pass"]),
                "",
            ]
        )
        return name
    if kind == "array":
        item = type_of(schema["items"], name + "Item", language)
        return f"list[{item}]" if language == "py" else f"({item})[]"
    return {
        "string": ("str", "string"),
        "integer": ("int", "number"),
        "number": ("float", "number"),
        "boolean": ("bool", "boolean"),
    }.get(kind, ("Any", "unknown"))[language == "ts"]


for name, schema in contract["$defs"].items():
    p = type_of(schema, name, "py")
    if p != name:
        py.append(f"{name} = {p}\n")
    ts.append(f"export type {name} = {type_of(schema, name, 'ts')};")
for name, schema in contract["methods"].items():
    type_name = "".join(x.title() for x in name.split(".")) + "Params"
    p = type_of(schema, type_name, "py")
    if p != type_name:
        py.append(f"{type_name} = {p}\n")
    ts.append(f"export type {type_name} = {type_of(schema, type_name, 'ts')};")
ts.append(
    "export const methods = " + json.dumps(list(contract["methods"])) + " as const;"
)
(ROOT / "src/molq/protocol/types.py").write_text("\n".join(py) + "\n")
(ROOT / "sdk/typescript/src/types.ts").write_text("\n".join(ts) + "\n")
async_names = {
    "asubmit",
    "_target_project",
    "invoke",
    "__aenter__",
    "__aexit__",
    "close",
    "rpc",
    "register",
    "list",
    "history",
    "definition",
    "authenticate",
    "update",
    "remove",
    "submit",
    "transfer",
    "validate",
    "preview",
    "observe",
    "status",
    "cancel",
    "requeue",
    "wait",
    "logs",
}


class MakeAsync(ast.NodeTransformer):
    def visit_FunctionDef(self, node):
        if node.name not in async_names:
            return self.generic_visit(node)
        node = ast.AsyncFunctionDef(**{f: getattr(node, f) for f in node._fields})
        return self.generic_visit(node)

    def visit_Call(self, node):
        node = self.generic_visit(node)
        if isinstance(node.func, ast.Attribute) and node.func.attr in {
            "rpc",
            "definition",
            "_target_project",
            "call",
            "connect",
            "close",
            "wait",
            "observe",
            "submit",
            "sleep",
        }:
            if node.func.attr == "sleep":
                node.func.value = ast.Name(id="asyncio", ctx=ast.Load())
            return ast.Await(value=node)
        return node


projection_paths = []
for name in ("objects", "jobs"):
    source = (
        (ROOT / f"src/molq/client/{name}.py")
        .read_text()
        .replace("molq.client.wire", "molq.client.async_wire")
        .replace("molq.client.jobs", "molq.client.async_jobs")
        .replace("molq.client.objects", "molq.client.async_objects")
        .replace("def __enter__", "def __aenter__")
        .replace("def __exit__", "def __aexit__")
        .replace("import time", "import time\nimport asyncio")
    )
    for old, new in [
        ("ClusterRegistry", "AsyncClusterRegistry"),
        ("JobCollection", "AsyncJobCollection"),
        ("Molq", "AsyncMolq"),
        ("Cluster", "AsyncCluster"),
        ("Job", "AsyncJob"),
        ("Wire", "AsyncWire"),
    ]:
        source = re.sub(r"\b" + old + r"\b", new, source)
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "submit":
            node.name = "asubmit"
            node.body[0].value.value = node.body[0].value.value.replace(
                'local = submit("dev", "local",',
                'local = await asubmit("dev", "local",',
            )
    tree.body[
        0
    ].value.value = f"Generated async OOP projection; edit {name}.py and regenerate."
    out = ast.unparse(ast.fix_missing_locations(MakeAsync().visit(tree)))
    generated = ROOT / f"src/molq/client/async_{name}.py"
    generated.write_text(out + "\n")
    projection_paths.append(str(generated))

paths = [str(ROOT / "src/molq/protocol/types.py"), *projection_paths]
subprocess.run([sys.executable, "-m", "ruff", "check", "--fix", *paths], check=True)
subprocess.run([sys.executable, "-m", "ruff", "format", *paths], check=True)
