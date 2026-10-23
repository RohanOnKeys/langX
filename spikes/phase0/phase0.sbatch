#!/bin/bash
#SBATCH --job-name=langx-p0
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=01:30:00
#SBATCH --output=p0_%j.out
#
# Phase 0 spikes A', B, F (plan section 16) on one node.
# Submit from the spike directory:
#   HPX_INSTALL=... P0_VENV=... sbatch phase0.sbatch
# Optional: P0_POLLS (default 250,500,1000), P0_REPS (default 5),
#           P0_SKIP_BUILD=1 to reuse ./build.

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL to the supervision-enabled HPX install}"
: "${P0_VENV:?set P0_VENV to a venv with pybind11}"
POLLS=${P0_POLLS:-250,500,1000}
REPS=${P0_REPS:-5}
SPIKE_DIR=${SLURM_SUBMIT_DIR:-$PWD}
cd "$SPIKE_DIR"

echo "=== identity ==="
echo "job=${SLURM_JOB_ID:-none} node=$(hostname) nproc=$(nproc) date=$(date -Is)"
uname -r

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$P0_VENV/bin/activate"
python --version
python -c "import pybind11; print('pybind11', pybind11.__version__)"
gcc --version | head -1
git -C "$HPX_INSTALL/../../source/hpx" rev-parse HEAD 2>/dev/null || true

if [ "${P0_SKIP_BUILD:-0}" != "1" ]; then
  echo "=== build ==="
  rm -rf build
  cmake -S native -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DPYBIND11_FINDPYTHON=ON \
    -DCMAKE_PREFIX_PATH="$HPX_INSTALL;$(python -m pybind11 --cmakedir)" \
    -DPython_EXECUTABLE="$(which python)" || { echo "CONFIGURE FAILED"; exit 11; }
  cmake --build build -j"$(nproc)" || { echo "BUILD FAILED"; exit 12; }
fi
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"
ldd build/p0_root | grep -i hpx
python -c "import sys; sys.path.insert(0,'build'); import _p0; print(_p0.version_info())" \
  || { echo "IMPORT FAILED"; exit 13; }

OUT=_runs/${SLURM_JOB_ID:-local}
mkdir -p "$OUT"

echo "=== smoke: one island, spike F only ==="
python run_phase0.py --build-dir build --out "$OUT/smoke" --fd-poll-ms 500 \
  --reps 1 --spikes F --island-timeout-s 120
SMOKE=$(python -c "import json; s=json.load(open('$OUT/smoke/summary.json')); print(s['500']['coord_ok'])")
if [ "$SMOKE" != "1" ]; then
  echo "SMOKE FAILED: look at $OUT/smoke/poll500_rep0/*.log and coord_result.json"
  exit 14
fi

# Optional shutdown check: no-kill islands for each disconnect mode and
# pause between supervision::finalize and hpx::disconnect.
SHUTDOWN_REPS=${P0_SHUTDOWN_REPS:-0}
if [ "$SHUTDOWN_REPS" -gt 0 ]; then
  for MODE in ${P0_SHUTDOWN_MODES:-post direct}; do
    for WAIT in ${P0_SHUTDOWN_WAITS:-0}; do
      echo "=== shutdown: mode=$MODE wait=${WAIT}ms reps=$SHUTDOWN_REPS, spike F only ==="
      P0_DISCONNECT_MODE=$MODE P0_POST_FINALIZE_WAIT_MS=$WAIT python run_phase0.py \
        --build-dir build --out "$OUT/shutdown_${MODE}_wait${WAIT}" --fd-poll-ms 500 \
        --reps "$SHUTDOWN_REPS" --spikes F --island-timeout-s 120
    done
  done
fi

echo "=== sweep: polls=$POLLS reps=$REPS mode=${P0_SWEEP_MODE:-post} ==="
P0_DISCONNECT_MODE=${P0_SWEEP_MODE:-post} python run_phase0.py --build-dir build \
  --out "$OUT/sweep" --fd-poll-ms "$POLLS" --reps "$REPS"

echo "=== bundle ==="
tar czf "p0_${SLURM_JOB_ID:-local}.tgz" "$OUT"
echo "bundle: $SPIKE_DIR/p0_${SLURM_JOB_ID:-local}.tgz"
echo "=== done ==="
