#!/usr/bin/env python
"Regenerate jupyasyncclient/rg_spec.py from a running rustygate (default http://localhost:8787)."
from pathlib import Path
from jupyasyncclient.core import build_spec

build_spec(nm=Path(__file__).parent.parent/'jupyasyncclient'/'rg_spec.py')
