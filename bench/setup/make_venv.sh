#!/bin/bash
# Create the Python venv every langX job uses (Python 3.12.3 from the module),
# with the pinned runtime, build and test requirements. Run from the
# repository root on the login node, which has network access:
#   bash bench/setup/make_venv.sh
# Optional: LANGX_VENV (default from rostam_env.sh).

HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/rostam_env.sh"   # before set -u: Lmod is not -u safe
set -euo pipefail

if [ ! -f "$LANGX_VENV/bin/activate" ]; then
  python -m venv "$LANGX_VENV"
fi
. "$LANGX_VENV/bin/activate"
python -m pip install -q --upgrade pip
python -m pip install -q -r requirements.txt -r requirements-build.txt \
  -r requirements-dev.txt
python --version
python -m pip list 2>/dev/null | grep -iE '^(langgraph|pybind11|pytest) '
