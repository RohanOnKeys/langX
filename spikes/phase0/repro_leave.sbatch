#!/bin/bash
#SBATCH --job-name=langx-repro
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=repro_%j.out
#
# Pure C++ reproducer for the connector abort in hpx::stop: root, one
# stayer, one leaver per rep, for each pause between supervision::finalize
# and hpx::disconnect. Submit from the spike directory:
#   HPX_INSTALL=... P0_VENV=... sbatch repro_leave.sbatch
# Optional: R_REPS (default 5), R_PAUSES (default "0 1000").

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL}"
: "${P0_VENV:?set P0_VENV (the venv the build directory was configured with)}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$P0_VENV/bin/activate"
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"
cmake --build build -j"$(nproc)" --target repro_leave p0_root || { echo "BUILD FAILED"; exit 12; }

OUT=_runs/repro_${SLURM_JOB_ID:-local}
port() { python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
common="--hpx:bind=none --hpx:ignore-batch-env"

for PAUSE in ${R_PAUSES:-0 1000}; do
  for REP in $(seq 1 "${R_REPS:-5}"); do
    d=$OUT/pause${PAUSE}_rep$REP
    mkdir -p "$d"
    RP=$(port)
    build/p0_root --hpx:agas=localhost:$RP --hpx:hpx=localhost:$RP \
      --hpx:expect-connecting-localities --hpx:threads=2 $common \
      --p0-bootdir "$d" --p0-fd-poll-ms 500 > "$d/root.log" 2>&1 &
    ROOT=$!
    for _ in $(seq 300); do [ -f "$d/root.started" ] && break; sleep 0.1; done
    build/repro_leave --hpx:agas=localhost:$RP --hpx:hpx=localhost:$(port) \
      --hpx:threads=4 $common --r-role stayer --r-stay-s 20 > "$d/stayer.log" 2>&1 &
    STAY=$!
    sleep 2
    timeout 60 build/repro_leave --hpx:agas=localhost:$RP --hpx:hpx=localhost:$(port) \
      --hpx:threads=4 $common --r-role leaver --r-pause-ms "$PAUSE" \
      --r-leave-after-s 3 > "$d/leaver.log" 2>&1
    RC=$?
    echo "pause=${PAUSE}ms rep=$REP leaver_rc=$RC last=\"$(grep '^R:' "$d/leaver.log" | tail -1)\" joined=\"$(grep 'joined' "$d/leaver.log")\""
    kill -9 "$STAY" "$ROOT" 2>/dev/null
    wait 2>/dev/null
  done
done
echo "=== done ==="
