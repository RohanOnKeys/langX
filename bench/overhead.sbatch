#!/bin/bash
#SBATCH --job-name=langx-overhead
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=langx_overhead_%j.out
#
# Dispatch overhead split: bench/dispatch_overhead.py, REPS times, on one
# node. Needs the build from bench/rostam_test.sbatch. Submit from the
# repository root:
#   HPX_INSTALL=<hpx install> LANGX_VENV=<venv> sbatch bench/overhead.sbatch
# The HPX-only floor is: CASES=rtt sbatch bench/hpx_repro/repro.sbatch

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL}"
: "${LANGX_VENV:?set LANGX_VENV}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$LANGX_VENV/bin/activate"
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"
for r in $(seq 1 "${REPS:-3}"); do
  python bench/dispatch_overhead.py --n "${N:-2000}"
done
echo "=== done"
