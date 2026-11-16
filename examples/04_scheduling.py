"""Typed scheduling intent contains no native flags."""

import json

from molq import Scheduling

print(
    json.dumps(
        Scheduling(
            name="calculation", partition="compute", account="research"
        ).to_wire(),
        ensure_ascii=False,
    )
)
