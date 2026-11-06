#!/usr/bin/env python
"Called by rustygate's cargo spec with the public OpenAPI JSON on stdin."
import json, sys
from pathlib import Path
from jupyasyncclient.core import build_spec

build_spec(nm=Path(__file__).parent.parent/'jupyasyncclient'/'rg_spec.py', spec=json.load(sys.stdin))
