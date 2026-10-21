#!/bin/bash
# exp70 Slice 4A HPX bisect driver (EXPERIMENT-ONLY). Finds the first HPX commit (first-parent
# history, i.e. merged PRs) after which actor-hosted HPX calls fail promptly after unexpected
# root loss, instead of blocking to the observation bound.
#
# The batch step runs on the sorted-first node, which the unchanged Slice 4A launcher requires.
# Every bisect step rebuilds HPX and exp68 and runs that launcher (two live runs) inside this
# same 2-node allocation; see bisect_step.sh for the per-commit build and classification.
#
# Before bisecting, both endpoints are re-tested with the bisect's own build settings (no
# supervision options, dependency pins by commit date). The bisect only starts if BISECT_GOOD
# classifies "old" and BISECT_BAD classifies "new".
#
# Required environment (exported at submission; sbatch forwards it):
#   RAYX_REPO, RAYX_VENV        RayX checkout and venv (as for the Slice 4A launcher)
#   BISECT_HPX_CLONE            full HPX git clone containing both endpoints
#   BISECT_WORK                 scratch dir: worktree, build, install, logs/results.tsv
#   BISECT_STDEXEC_REPO         full stdexec clone (checked out per step)
#   BISECT_ASIO_REPO            full asio clone (checked out per step)
#   BISECT_GOOD, BISECT_BAD     HPX commits showing the old / new behaviour
#
#   sbatch -w medusa00,medusa01 -o <dir>/exp70s4bis_%j.out exp70_slice4_bisect.sbatch
#
#SBATCH -J exp70s4bis
#SBATCH -p medusa
#SBATCH -N 2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH -t 04:00:00

set -uo pipefail

: "${RAYX_REPO:?}" "${RAYX_VENV:?}" "${BISECT_HPX_CLONE:?}" "${BISECT_WORK:?}"
: "${BISECT_STDEXEC_REPO:?}" "${BISECT_ASIO_REPO:?}" "${BISECT_GOOD:?}" "${BISECT_BAD:?}"

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$RAYX_VENV/bin/activate"
export SLURM_EXPORT_ENV=ALL
export BISECT_WORK BISECT_STDEXEC_REPO BISECT_ASIO_REPO RAYX_REPO RAYX_VENV

STEP=$RAYX_REPO/experiments/70_hpx_locality_supervision/slice4_root_loss_event/bisect/bisect_step.sh
WT=$BISECT_WORK/hpx-worktree
mkdir -p "$BISECT_WORK/logs"

echo "=== allocation: job=$SLURM_JOB_ID nodes=$SLURM_JOB_NODELIST host=$(hostname) ==="
echo "good=$BISECT_GOOD bad=$BISECT_BAD git=$(git --version)"

if [ ! -d "$WT/.git" ] && [ ! -f "$WT/.git" ]; then
  git -C "$BISECT_HPX_CLONE" worktree add --detach "$WT" "$BISECT_BAD" || exit 2
fi
cd "$WT" || exit 2
git bisect reset >/dev/null 2>&1 || true

endpoint () {  # $1 = commit, $2 = expected step exit code
  git checkout -q -f --detach "$1" || return 1
  bash "$STEP"
  local rc=$?
  echo "endpoint $1 -> rc=$rc (expected $2)"
  [ "$rc" -eq "$2" ]
}

echo "=== endpoint check: good must be old (0), bad must be new (1) ==="
if ! endpoint "$BISECT_GOOD" 0; then echo "STOP: good endpoint is not 'old' under bisect settings"; exit 3; fi
if ! endpoint "$BISECT_BAD" 1; then echo "STOP: bad endpoint is not 'new' under bisect settings"; exit 3; fi

echo "=== bisect: $(git rev-list --first-parent --count "$BISECT_GOOD..$BISECT_BAD") first-parent commits in range ==="
git bisect start --first-parent "$BISECT_BAD" "$BISECT_GOOD"
git bisect run bash "$STEP"
BRC=$?
git bisect log > "$BISECT_WORK/logs/bisect_${SLURM_JOB_ID}.log"
echo "=== bisect log ==="
cat "$BISECT_WORK/logs/bisect_${SLURM_JOB_ID}.log"
git bisect reset >/dev/null 2>&1 || true

echo "=== per-step results (sha, commit date, verdict, detail) ==="
cat "$BISECT_WORK/logs/results.tsv"
echo "=== done rc=$BRC $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
exit $BRC
