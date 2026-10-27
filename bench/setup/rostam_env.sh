# Rostam environment for every langX build and job. Source it, do not run it:
#   . bench/setup/rostam_env.sh
# HPX_INSTALL and LANGX_VENV keep any value already set.

module purge
module load gcc/15.1.0 cmake/3.29.2 boost/1.91.0-release hwloc/2.12.0 python/3.12.3

export HPX_INSTALL=${HPX_INSTALL:-/work/$USER/apps/hpx-master-install}
export LANGX_VENV=${LANGX_VENV:-/work/$USER/venvs/langx}
export LD_LIBRARY_PATH="$HPX_INSTALL/lib64:$HPX_INSTALL/lib:${LD_LIBRARY_PATH:-}"
if [ -f "$LANGX_VENV/bin/activate" ]; then
  . "$LANGX_VENV/bin/activate"
fi
