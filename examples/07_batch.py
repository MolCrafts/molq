"""Observe collections in one RPC scope instead of one worker per Job."""

import json
import sys

from molq import Molq

if __name__ == "__main__":
    with open(sys.argv[1]) as file:
        refs = json.load(file)
    mq = Molq()
    try:
        print(json.dumps(mq.jobs(refs).status(), ensure_ascii=False))
    finally:
        mq.close()
