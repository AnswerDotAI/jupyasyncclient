#!/usr/bin/env python
"Called by jupygate's cargo spec with the public OpenAPI JSON on stdin."
import json, sys
from pathlib import Path
from jupyasyncclient.core import build_spec

build_spec(nm=Path(__file__).parent.parent/'jupyasyncclient'/'jg_spec.py', spec=json.load(sys.stdin))
