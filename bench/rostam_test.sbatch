#!/bin/bash
#SBATCH --job-name=langx-test
#SBATCH --partition=medusa
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:45:00
#SBATCH --output=langx_test_%j.out
#
# Build _langx and langx_root against HPX, then run the unit and
# integration tests on one node. Submit from the repository root:
#   HPX_INSTALL=<hpx install> LANGX_VENV=<venv with pybind11, pytest> \
#     sbatch bench/rostam_test.sbatch

set -uo pipefail
: "${HPX_INSTALL:?set HPX_INSTALL to a supervision-enabled HPX install}"
: "${LANGX_VENV:?set LANGX_VENV to a venv with pybind11 and pytest}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"

echo "=== identity ==="
echo "job=${SLURM_JOB_ID:-none} node=$(hostname) commit=$(git rev-parse --short HEAD)"

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3
source "$LANGX_VENV/bin/activate"
python --version

echo "=== build ==="
rm -rf python/build
cmake -S python -B python/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DPYBIND11_FINDPYTHON=ON \
  -DCMAKE_PREFIX_PATH="$HPX_INSTALL;$(python -m pybind11 --cmakedir)" \
  -DPython_EXECUTABLE="$(which python)" || { echo "CONFIGURE FAILED"; exit 11; }
cmake --build python/build -j"$(nproc)" || { echo "BUILD FAILED"; exit 12; }
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"

echo "=== unit tests ==="
PYTHONPATH=python/src python -m pytest -q tests/unit || UNIT_RC=$?

echo "=== integration tests ==="
PYTHONPATH=python/src python -m pytest -v tests/integration || INT_RC=$?

echo "=== done: unit_rc=${UNIT_RC:-0} integration_rc=${INT_RC:-0} ==="
