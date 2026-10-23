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
# Optional: REPS (default 5), CASES (default all), and HPX_INSTALL pointing at
# a patched HPX to check a fix, e.g.
#   CASES=leave HPX_INSTALL=<patched install> sbatch bench/hpx_repro/repro.sbatch

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL to a supervision-enabled HPX install}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"

echo "=== identity: job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD) hpx_install=$HPX_INSTALL"
B=bench/hpx_repro/build_$(echo "$HPX_INSTALL" | md5sum | cut -c1-8)   # one per HPX install
cmake -S bench/hpx_repro -B $B -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$HPX_INSTALL" || { echo "CONFIGURE FAILED"; exit 11; }
cmake --build $B -j"$(nproc)" || { echo "BUILD FAILED"; exit 12; }
echo "=== linked: $(ldd $B/supervision_gaps_repro | grep -o "[^ ]*libhpx_core[^ ]*" | head -1)"

OUT=bench/hpx_repro/_runs/${SLURM_JOB_ID:-local}
R=bench/hpx_repro/run_repro.sh
N=${REPS:-5}
for CASE in ${CASES:-observer inflight remove epoch leave}; do
  case $CASE in
    observer)
      bash $R $B/supervision_gaps_repro $OUT observer "$N" --r-b-join 1
      bash $R $B/supervision_gaps_repro $OUT observer "$N" --r-b-join 0 ;;
    leave)
      bash $R $B/supervision_gaps_repro $OUT leave "$N" --r-pause-ms 0
      bash $R $B/supervision_gaps_repro $OUT leave "$N" --r-pause-ms 1000 ;;
    *)
      bash $R $B/supervision_gaps_repro $OUT "$CASE" "$N" ;;
  esac
done
echo "=== done"
