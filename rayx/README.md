# RayX experiments snapshot

This folder is a read-only reference copy of the RayX experiment write-ups and results that langX builds on. It is not maintained here and is not part of the langX code.

## Source

| Field | Value |
| --- | --- |
| Upstream project | RayX, <https://github.com/byekrang/RayX> |
| Copied from | fork <https://github.com/RohanOnKeys/RayX>, commit `0037827` (2026-10-10) |
| Contents | every tracked file under `experiments/` (exp01 to exp70), plus the local exp70 reproduction run directories |
| Authors | exp01 to exp70 and all experiment code: Bita (GitHub `byekrang`). exp70 reproduction on HPX `cc2be553` (report, curated evidence, run directories, launcher overrides, Slice 4A control and bisect driver): Rohan Pattanayak |

RayX has no license file at the copied commit. This copy is for local reference while permission or a license is being arranged. Do not publish it, and do not copy code from it into langX, until the author has agreed.

## What is included

- `experiments/NN_*/`: write-ups, curated aggregate JSON, runners, sbatch scripts, and native sources, as tracked in RayX.
- `experiments/70_hpx_locality_supervision/slice*/_exp70_slice*_runs/`: raw run directories from the exp70 reproduction on HPX master `cc2be553` (Rostam, 2026-10-10). These are gitignored in RayX.
- `experiments/70_hpx_locality_supervision/_exp70_rostam_runs/`: the Slice 0 runner and the HPX build logs for `cc2be553` (job 206520) and `20bc3d4b` (jobs 206556, 206557). The original `.tgz` bundles were left out because their contents are already extracted into the run directories above.

The per-experiment `.gitignore` files from RayX were removed from this copy, because they would hide the run results above. They are RayX development hygiene rules, not experiment content.

Links in the copied write-ups that point outside `experiments/` (for example to RayX `docs/` or `readme.md`) are broken here. Read them in the RayX repository.

## Most relevant to langX

| Experiment | Why it matters here |
| --- | --- |
| `66_hpx_runtime_inside_ray_actor` | HPX runtime hosted in-process inside a Python process over the TCP parcelport |
| `67_two_ray_actors_shared_hpx` | Two Ray-hosted localities in one shared HPX runtime, bidirectional actions |
| `68_vocab_sharded_topk` | Exactness methodology: distributed results checked bit for bit against an oracle |
| `70_hpx_locality_supervision` | Lifecycle supervision. Slice 3B is the native `supervision_dispatch` harness: SIGKILL detection, `target_fenced`, `force_disconnect`, replacement join. See `reproduction_hpx_cc2be553.md` for the current-master results |
