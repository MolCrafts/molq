"""Canonical argv, explicit shell and inline script constructors."""

import json

from molq import ExecutionUnit

argv = ExecutionUnit.argv("argv", ["echo", "literal $HOME"])
shell = ExecutionUnit.shell("shell", 'printf "%s\\n" "$HOME"')
script = ExecutionUnit.script("script", "printf 'first\\n'\ntrue\n")
for unit in (argv, shell, script):
    print(json.dumps(unit.to_wire(), ensure_ascii=False))
