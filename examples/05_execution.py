"""Explicit allocation-internal parallel then sequential work."""

import json

from molq import Execution, ExecutionUnit, JobSpec, Parallel, Sequence, UnitRef

spec = JobSpec(
    execution=Execution(
        units=(
            ExecutionUnit.argv("a", ["echo", "a"]),
            ExecutionUnit.argv("b", ["echo", "b"]),
            ExecutionUnit.argv("finish", ["echo", "finished"]),
        ),
        plan=Sequence((Parallel((UnitRef("a"), UnitRef("b"))), UnitRef("finish"))),
    )
)
print(json.dumps(spec.to_wire(), ensure_ascii=False))
