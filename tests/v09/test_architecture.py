"""Enforce service independence for every Python client module."""

import ast
from pathlib import Path


def test_python_clients_do_not_import_service_implementation():
    source = Path(__file__).resolve().parents[2] / "src" / "molq"
    forbidden = {
        "molq.serve",
        "molq.runtime",
        "molq.scheduler",
        "molq.transport",
        "molq.registry",
        "molq.rpc",
    }
    violations = []
    for path in source.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports = [item.name for item in node.names]
            elif isinstance(node, ast.ImportFrom):
                package = ".".join(path.relative_to(source.parent).parts[:-1])
                if node.level:
                    package = ".".join(package.split(".")[: 1 - node.level or None])
                    module = ".".join(filter(None, [package, node.module]))
                else:
                    module = node.module or ""
                imports = [module, *(f"{module}.{item.name}" for item in node.names)]
            else:
                continue
            for imported in imports:
                if any(
                    imported == service or imported.startswith(service + ".")
                    for service in forbidden
                ):
                    violations.append(f"{path.name}:{node.lineno} imports {imported}")
    assert not violations, "\n".join(violations)
