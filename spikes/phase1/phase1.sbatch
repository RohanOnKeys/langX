#!/bin/bash
#SBATCH --job-name=langx-p1
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=langx_p1_%j.out
#
# Phase 1 survivor fencing diagnostic (docs/results/phase1.md, job 216785):
# kill w2 or w1, with workers joining their peers at init (5000 ms) and not
# (0 ms). Needs the build from bench/rostam_test.sbatch. Submit from the
# repository root:
#   sbatch spikes/phase1/phase1.sbatch
# Optional: REPS (default 2: each rep kills w2, then w1).

cd "${SLURM_SUBMIT_DIR:-$PWD}"
. bench/setup/rostam_env.sh
set -uo pipefail

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"
for d in 5000 0; do
  for r in $(seq 1 "${REPS:-2}"); do
    for k in w2 w1; do
      python spikes/phase1/diag_survivor_fence.py --kill $k --worker-discovery-ms $d
    done
  done
done
echo "=== done"
