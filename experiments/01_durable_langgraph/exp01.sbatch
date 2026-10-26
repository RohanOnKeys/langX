#!/bin/bash
#SBATCH --job-name=langx-exp01
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=03:00:00
#SBATCH --output=langx_exp01_%j.out
#
# Failure matrix F1 to F9 on one node, then the gates. Needs the build from
# bench/rostam_test.sbatch. Submit from the repository root:
#   HPX_INSTALL=<hpx install> LANGX_VENV=<venv with langgraph> \
#     sbatch experiments/01_durable_langgraph/exp01.sbatch
# Optional: TRIALS (default 10), SCENARIOS (default all, e.g. "F2 F4"),
# EXP_ARGS (extra run_exp01.py flags for every scenario).

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL}"
: "${LANGX_VENV:?set LANGX_VENV}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$LANGX_VENV/bin/activate"
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"
python -c "import importlib.metadata as m; print(*(f'{p} {m.version(p)}' for p in ('langgraph', 'langgraph-checkpoint-sqlite')))"
X=experiments/01_durable_langgraph
OUT=$X/_runs/${SLURM_JOB_ID:-local}
N=${TRIALS:-10}
for S in ${SCENARIOS:-F1 F2 F3 F4 F5 F6 F7 F8 F9}; do
  case $S in
    F3) python $X/run_exp01.py --scenario F3 --tier-b naive --trials "$N" --out "$OUT" ${EXP_ARGS:-}
        python $X/run_exp01.py --scenario F3 --tier-b keyed --trials "$N" --out "$OUT" ${EXP_ARGS:-} ;;
    *)  python $X/run_exp01.py --scenario "$S" --trials "$N" --out "$OUT" ${EXP_ARGS:-} ;;
  esac
done
python $X/analyze_exp01.py "$OUT/exp01.jsonl"
echo "=== done: rc=$?"
