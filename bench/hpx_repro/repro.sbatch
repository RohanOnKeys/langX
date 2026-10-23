#!/bin/bash
#SBATCH --job-name=hpx-repro
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:45:00
#SBATCH --output=hpx_repro_%j.out
#
# Build and run every supervision reproducer case on one node. Submit from
# the repository root:
#   HPX_INSTALL=<hpx install> sbatch bench/hpx_repro/repro.sbatch
# Optional: REPS (default 5).

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL to a supervision-enabled HPX install}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"
B=bench/hpx_repro/build
cmake -S bench/hpx_repro -B $B -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$HPX_INSTALL" || { echo "CONFIGURE FAILED"; exit 11; }
cmake --build $B -j"$(nproc)" || { echo "BUILD FAILED"; exit 12; }

OUT=bench/hpx_repro/_runs/${SLURM_JOB_ID:-local}
R=bench/hpx_repro/run_repro.sh
N=${REPS:-5}
bash $R $B/supervision_gaps_repro $OUT observer "$N" --r-b-join 1
bash $R $B/supervision_gaps_repro $OUT observer "$N" --r-b-join 0
bash $R $B/supervision_gaps_repro $OUT inflight "$N"
bash $R $B/supervision_gaps_repro $OUT remove "$N"
bash $R $B/supervision_gaps_repro $OUT epoch "$N"
echo "=== done"
