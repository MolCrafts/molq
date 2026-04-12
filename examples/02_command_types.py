"""02_command_types.py — Three mutually exclusive command types.

Demonstrates:
  - argv  : structured args, never shell-interpreted (safest)
  - command: single-line shell string
  - Script.inline: multi-line script text
  - Script.path  : reference an existing file on disk
"""

import tempfile
from pathlib import Path

from molq.testing import make_submitor
from molq.types import Script

with make_submitor("demo", job_duration=0) as s:
    # 1. argv — recommended for structured commands
    h = s.submit(argv=["python", "-c", "print('argv job')"])
    print(f"argv    → {h.wait().state}")

    # 2. command — single-line shell string
    h = s.submit(command="echo 'command job' && echo done")
    print(f"command → {h.wait().state}")

    # 3. Script.inline — multi-line bash/python script
    h = s.submit(
        script=Script.inline(
            """\
#!/bin/bash
set -e
echo "step 1"
echo "step 2"
"""
        )
    )
    print(f"inline  → {h.wait().state}")

    # 4. Script.path — reference a pre-existing file
    with tempfile.NamedTemporaryFile(suffix=".sh", delete=False, mode="w") as f:
        f.write("#!/bin/bash\necho 'path script'\n")
        script_path = Path(f.name)
    script_path.chmod(0o755)

    h = s.submit(script=Script.path(script_path))
    print(f"path    → {h.wait().state}")

    script_path.unlink()
