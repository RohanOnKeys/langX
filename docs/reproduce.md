# Reproducing the results

Every result in [`docs/results/`](results/) comes from a script in this repository. This page lists, per write-up, the command that produces it and the Rostam jobs it was first run as. All commands run from the repository root on Rostam.

## One-time setup

| Step | Command | Notes |
| --- | --- | --- |
| Environment | `. bench/setup/rostam_env.sh` | modules gcc 15.1.0, cmake 3.29.2, boost 1.91.0, hwloc 2.12.0, Python 3.12.3; sets `HPX_INSTALL`, `LANGX_VENV`, `LD_LIBRARY_PATH` |
| HPX | `srun -p medusa -N1 --exclusive -t 02:00:00 bash bench/setup/build_hpx.sh` | HPX master `cc2be553` with supervision and the TCP parcelport; see the note in the script |
| Python venv | `bash bench/setup/make_venv.sh` | pinned `requirements*.txt`; run on the login node |
| langX build and tests | `sbatch bench/rostam_test.sbatch` | builds `_langx` and `langx_root`, runs unit and integration tests |

Source `bench/setup/rostam_env.sh` in the shell before `sbatch`; Slurm passes the environment on. Every job script sources `bench/setup/rostam_env.sh` or loads the same modules, and reads `HPX_INSTALL` and `LANGX_VENV` from the environment. Jobs that need the langX build depend on the last step; chain them with `sbatch -d afterok:<job>`.

Versions on Rostam when the results were taken: HPX `cc2be553`, gcc 15.1.0, Python 3.12.3, pybind11 3.1.0, pytest 9.1.1, LangGraph 1.2.16. `requirements-dev.txt` pins pytest 8.4.2 for CI.

## Per result

| Write-up | What | Command | First jobs |
| --- | --- | --- | --- |
| [`phase0.md`](results/phase0.md) | spikes A', B, F: detection, fencing, shutdown | `cd spikes/phase0 && P0_VENV=$LANGX_VENV sbatch phase0.sbatch` | 216190, 216201, 216727, 216728 |
| [`phase0.md`](results/phase0.md) | C++ reproducer of the abort in `hpx::stop` | `cd spikes/phase0 && P0_VENV=$LANGX_VENV sbatch repro_leave.sbatch` | 216729 |
| [`phase1.md`](results/phase1.md) | unit and integration tests | `sbatch bench/rostam_test.sbatch` | 216761, 216785 |
| [`phase1.md`](results/phase1.md) | a dead observer fences a surviving worker | `sbatch spikes/phase1/phase1.sbatch` | 216762, 216785 |
| [`phase2.md`](results/phase2.md) | coordinator loss and worker restart | `sbatch spikes/phase2/phase2.sbatch` | 217756, 217758, 217788 |
| [`phase3.md`](results/phase3.md) | F0: HPX run equals the LangGraph baseline | `F0_ARGS="--reps 5" sbatch experiments/01_durable_langgraph/f0.sbatch` | 218812 |
| [`phase3.md`](results/phase3.md) | unit and integration tests, F0 test included | `sbatch bench/rostam_test.sbatch` | 219462 |
| [`overhead.md`](results/overhead.md) | dispatch overhead, Python layers | `sbatch bench/overhead.sbatch` | 219491 |
| [`overhead.md`](results/overhead.md) | dispatch overhead, HPX-only floor | `CASES=rtt REPS=3 sbatch bench/hpx_repro/repro.sbatch` | 219492 |

Phase 2 job 217757 (in-process re-init of a fenced worker) used an option that was removed once re-init was shown to fail, so it is not re-runnable.

## Upstream reproducers

The C++ reproducers attached to the HPX issues are in [`bench/hpx_repro/`](../bench/hpx_repro/). They need only an HPX install, not the langX build.

| Issue | Case | Command |
| --- | --- | --- |
| [hpx#7825](https://github.com/TheHPXProject/hpx/issues/7825) | `leave` | `CASES=leave sbatch bench/hpx_repro/repro.sbatch` |
| [hpx#7834](https://github.com/TheHPXProject/hpx/issues/7834) | `observer`, `inflight`, `remove`, `epoch` | `CASES="observer inflight remove epoch" sbatch bench/hpx_repro/repro.sbatch` |

To check a fix, point `HPX_INSTALL` at the patched install; the script keeps one build directory per install.

## Outputs

Job logs land in the directory the job was submitted from (`*_<job>.out`). Raw run data goes under `_runs/` next to each script and is ignored by git. The numbers in the write-ups are copied from those logs.
