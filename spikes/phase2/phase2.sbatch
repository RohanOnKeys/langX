#!/bin/bash
#SBATCH --job-name=langx-p2
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:40:00
#SBATCH --output=langx_p2_%j.out
#
# Phase 2 coordinator loss and worker restart (docs/results/phase2.md, jobs
# 217758 and 217788): C1 killed, C1 leaving cleanly with a 1 s and a 3 s
# pause, REPS times each. Needs the build from bench/rostam_test.sbatch.
# Submit from the repository root:
#   sbatch spikes/phase2/phase2.sbatch
# Optional: REPS (default 3).
#
# Job 217757 (in-process re-init of a fenced worker) used a --reinit option
# that was removed once re-init was shown to fail; it is not re-runnable.

cd "${SLURM_SUBMIT_DIR:-$PWD}"
. bench/setup/rostam_env.sh
set -uo pipefail

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"
for r in $(seq 1 "${REPS:-3}"); do
  python spikes/phase2/coordinator_loss.py --c1-exit kill
  python spikes/phase2/coordinator_loss.py --c1-exit clean --c1-pause-s 1.0
  python spikes/phase2/coordinator_loss.py --c1-exit clean --c1-pause-s 3.0
done
echo "=== done"
