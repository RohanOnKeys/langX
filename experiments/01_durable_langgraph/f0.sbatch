#!/bin/bash
#SBATCH --job-name=langx-f0
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=langx_f0_%j.out
#
# F0 for research_agent on one node: baseline vs HPX run. Needs the build
# from bench/rostam_test.sbatch. Submit from the repository root:
#   HPX_INSTALL=<hpx install> LANGX_VENV=<venv with langgraph> \
#     sbatch experiments/01_durable_langgraph/f0.sbatch
# Optional: F0_ARGS, e.g. F0_ARGS="--reps 10 --width 16 --loops 3"

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL}"
: "${LANGX_VENV:?set LANGX_VENV}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$LANGX_VENV/bin/activate"
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"
python -c "import importlib.metadata as m; print('langgraph', m.version('langgraph'))"
python experiments/01_durable_langgraph/run_f0.py \
  --out "experiments/01_durable_langgraph/_runs/${SLURM_JOB_ID:-local}" ${F0_ARGS:-}
echo "=== done: rc=$?"
