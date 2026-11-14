"""Fail if canonical generation changes any checked-in projection."""

import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
paths = [
    root / "src/molq/protocol/contract.json",
    root / "src/molq/protocol/types.py",
    root / "src/molq/client/async_objects.py",
    root / "src/molq/client/async_jobs.py",
    root / "sdk/typescript/src/types.ts",
]
before = {path: path.read_bytes() for path in paths}
for script in ("build_contract.py", "generate_clients.py"):
    subprocess.run([sys.executable, str(root / "tools" / script)], check=True)
subprocess.run(["npm", "run", "build"], cwd=root / "sdk/typescript", check=True)
changed = [
    str(path.relative_to(root)) for path in paths if path.read_bytes() != before[path]
]
if changed:
    raise SystemExit("Generated drift: " + ", ".join(changed))
print("Canonical schema, DTOs and async projection match.")
