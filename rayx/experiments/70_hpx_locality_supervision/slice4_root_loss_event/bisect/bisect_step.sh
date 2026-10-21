#!/bin/bash
# exp70 Slice 4A HPX bisect step (EXPERIMENT-ONLY). Run by `git bisect run` inside the HPX
# bisect worktree, within the 2-node allocation of exp70_slice4_bisect.sbatch.
#
# For the checked-out HPX commit: build HPX (Asio/stdexec pinned to what that commit expected),
# rebuild exp68 against it, run the unchanged Slice 4A launcher, and classify the post-root-loss
# actor observations of the runs it produced:
#   exit 0   -> "old" behaviour: every actor probe is call_timeout (blocked to the bound)
#   exit 1   -> "new" behaviour: every actor probe is call_error_result (failed promptly)
#   exit 125 -> skip: build failure, run failure, no runs, or mixed categories
set -uo pipefail

: "${BISECT_WORK:?}" "${BISECT_STDEXEC_REPO:?}" "${BISECT_ASIO_REPO:?}" "${RAYX_REPO:?}"

SHA=$(git rev-parse HEAD)
DATE=$(git log -1 --format=%cI)
TAG=${SHA:0:10}
LOGS=$BISECT_WORK/logs
BUILD=$BISECT_WORK/hpx-build
INSTALL=$BISECT_WORK/hpx-install
EXP68=$RAYX_REPO/experiments/68_vocab_sharded_topk
E68B=$EXP68/build_rostam_bisect
S4=$RAYX_REPO/experiments/70_hpx_locality_supervision/slice4_root_loss_event
RUNS=$S4/_exp70_slice4_runs
mkdir -p "$LOGS"

log () { echo "[bisect-step $(date -u +%H:%M:%SZ) $TAG] $*"; }
record () { printf '%s\t%s\t%s\t%s\n' "$SHA" "$DATE" "$1" "$2" >> "$LOGS/results.tsv"; }
skip () { log "SKIP: $1"; record skip "$1"; exit 125; }

log "commit date $DATE"

# ---- dependency pins: what this HPX commit would have fetched on its own date ----------------
opt_default () {  # $1 = option name; prints the default value declared in CMakeLists.txt
  grep -oP "$1\s+STRING\s+\"[^\"]*\"\s+\"\K[^\"]+" CMakeLists.txt | head -1
}
SX_TAG=$(opt_default HPX_WITH_STDEXEC_TAG)
if [ -z "$SX_TAG" ] || [ "$SX_TAG" = "main" ]; then
  SX=$(git -C "$BISECT_STDEXEC_REPO" rev-list -1 --before="$DATE" origin/main)
else
  SX=$(git -C "$BISECT_STDEXEC_REPO" rev-parse --verify -q "$SX_TAG^{commit}")
fi
[ -n "$SX" ] || skip "cannot resolve stdexec for tag '$SX_TAG'"
git -C "$BISECT_STDEXEC_REPO" checkout -q -f --detach "$SX" || skip "stdexec checkout $SX failed"

ASIO_ARGS=()
ASIO_TAG=$(opt_default HPX_WITH_ASIO_TAG)
if [ -n "$ASIO_TAG" ] && git -C "$BISECT_ASIO_REPO" checkout -q -f --detach "$ASIO_TAG" 2>/dev/null; then
  ASIO_ARGS=(-DFETCHCONTENT_SOURCE_DIR_ASIO="$BISECT_ASIO_REPO")
else
  log "asio tag '$ASIO_TAG' not pinned locally; HPX will fetch it itself"
fi
log "stdexec default='${SX_TAG:-none}' -> $SX ; asio='${ASIO_TAG:-none}'"

# ---- HPX: clean configure + build + install ---------------------------------------------------
rm -rf "$BUILD" "$INSTALL"
cmake -S . -B "$BUILD" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$INSTALL" \
  -DHPX_WITH_CXX_STANDARD=20 \
  -DHPX_WITH_DISTRIBUTED_RUNTIME=ON -DHPX_WITH_NETWORKING=ON -DHPX_WITH_PARCELPORT_TCP=ON \
  -DHPX_WITH_EXAMPLES=OFF -DHPX_WITH_TESTS=OFF -DHPX_WITH_MALLOC=system \
  -DHPX_WITH_FETCH_ASIO=ON "${ASIO_ARGS[@]}" \
  -DHPX_WITH_FETCH_STDEXEC=ON -DHPX_WITH_STDEXEC_TAG="$SX" \
  -DFETCHCONTENT_SOURCE_DIR_STDEXEC="$BISECT_STDEXEC_REPO" \
  > "$LOGS/$TAG.hpx-configure.log" 2>&1 || skip "HPX configure failed"
cmake --build "$BUILD" -j"$(nproc)" --target install \
  > "$LOGS/$TAG.hpx-build.log" 2>&1 || skip "HPX build failed"
grep -q "\"$SHA\"" "$INSTALL/include/hpx/config/defines.hpp" || skip "installed HPX does not carry $SHA"
log "HPX built"

# ---- exp68 against this HPX ------------------------------------------------------------------
rm -rf "$E68B"
cmake -S "$EXP68" -B "$E68B" -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON -DPython_EXECUTABLE="$(which python)" \
  -DCMAKE_PREFIX_PATH="$INSTALL;$(python -m pybind11 --cmakedir)" \
  > "$LOGS/$TAG.exp68.log" 2>&1 \
  && cmake --build "$E68B" -j 8 >> "$LOGS/$TAG.exp68.log" 2>&1 || skip "exp68 build failed"

# ---- unchanged Slice 4A launcher -------------------------------------------------------------
STAMP=$LOGS/$TAG.stamp
touch "$STAMP"
sleep 1
RAYX_HPX_INSTALL=$INSTALL RAYX_HPX_SHA=$SHA RAYX_EXP68_BUILD=$E68B \
  bash "$S4/exp70_slice4_crossnode.sbatch" > "$LOGS/$TAG.slice4.log" 2>&1
cp "$RUNS/hardware_identity_${SLURM_JOB_ID}.txt" "$LOGS/$TAG.identity.txt" 2>/dev/null || true

mapfile -t DIRS < <(find "$RUNS" -maxdepth 1 -type d -name "crossnode_${SLURM_JOB_ID}_*" -newer "$STAMP" | sort)
[ "${#DIRS[@]}" -gt 0 ] || skip "launcher produced no run directory"

VERDICT=$(python - "${DIRS[@]}" <<'EOF'
import json, os, sys
cats, rows = set(), []
for d in sys.argv[1:]:
    a = json.load(open(os.path.join(d, "aggregate.json")))
    if a.get("overall") != "pass":
        print(f"skip run {a.get('runid')} overall={a.get('overall')}"); sys.exit(0)
    o = json.load(open(os.path.join(d, "markers.json")))["arms"]["loss"]["post_loss_actor_observations"]
    rows.append(f"{a['runid']}:" + ",".join(f"{k}={v['category']}/{v['elapsed_s']:.3f}s" for k, v in o.items()))
    cats |= {v["category"] for v in o.values()}
kind = "old" if cats == {"call_timeout"} else "new" if cats == {"call_error_result"} else "skip"
print(kind, " ".join(rows))
EOF
)
log "verdict: $VERDICT"
case "$VERDICT" in
  old*) record old "${VERDICT#old }"; exit 0 ;;
  new*) record new "${VERDICT#new }"; exit 1 ;;
  *)    skip "${VERDICT:-classifier produced nothing}" ;;
esac
