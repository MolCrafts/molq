"""Defaults persist through RPC; merging occurs only in Runtime."""

import json

from molq import Molq

mq = Molq()
try:
    print(
        json.dumps(
            mq.rpc(
                "presets.set",
                key="small",
                value={"resources": {"tasks": 2, "cpus_per_task": 1}},
            ),
            ensure_ascii=False,
        )
    )
finally:
    mq.close()
