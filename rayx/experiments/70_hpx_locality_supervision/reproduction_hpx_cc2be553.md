# exp70 reproduction on current HPX master (cc2be553)

**Date:** 2026-10-10. **Cluster:** Rostam, partition `medusa`. **HPX:** master
`cc2be553fac9b1f2a372f4277d7c708e70bf7eb4` (committed 2026-10-09).

Curated evidence: [`reproduction_hpx_cc2be553_curated_evidence.json`](reproduction_hpx_cc2be553_curated_evidence.json).
Raw run directories stay untracked under each slice's `_exp70_slice*_runs/`.

## Summary

exp70 was rerun from a fresh checkout, by a second person, against the latest HPX master
instead of the pinned commits used for the recorded evidence (`20bc3d4b` for Slices 1 to 4,
`bec10f39` for Slice 3B).

* **Slices 1, 2A, 3A and 4A reproduce.** Each passed 2/2 cross-node runs with the same
  classifications as the recorded evidence.
* **Slice 3B reproduces.** It passed 16/16 gates in 4/4 runs on four different nodes. The root
  still hangs at teardown in every run, so shutdown stays INCONCLUSIVE. In one of the four runs
  the hang happened earlier than recorded, apparently inside `hpx::supervision::finalize()`
  rather than in `hpx::finalize()`.
* **One recorded observation did not reproduce.** After unexpected root loss (Slice 4A), the
  actor-hosted HPX calls failed within about 6 to 10 ms with `Connection reset by peer`. In the
  recorded runs the same calls blocked until the 15 s observation bound. This contradicts exp70
  key finding 1. A same-node control shows the change follows the HPX build: on medusa00/01 with
  everything else identical, HPX `20bc3d4b` still blocks for 15 s and HPX `cc2be553` fails in
  milliseconds.
* **Not run:** Slice 0, and the local (single-machine) phases of Slices 1 to 4.

## Setup

| Item | Value |
|---|---|
| HPX source | `TheHPXProject/hpx` master at `cc2be553` |
| HPX options ON | `DISTRIBUTED_RUNTIME`, `NETWORKING`, `PARCELPORT_TCP`, `SUPERVISION`, `FORCE_DISCONNECT` |
| HPX options OFF | `EXAMPLES`, `TESTS` |
| Other HPX settings | Release, C++20, Ninja, `MALLOC=system`, Asio and stdexec fetched at configure time |
| Toolchain | gcc 15.1.0, cmake 3.29.2, boost 1.91.0, hwloc 2.12.0 (Rostam modules) |
| Python stack | Python 3.12.3, Ray 2.55.1, pybind11 3.1.0 (fresh venv) |
| OS | Linux 5.14.0-570.37.1.el9_6 x86_64, glibc 2.34 |
| Network | `10.42.5.x` (`eno16`) |
| Repository | fork `RohanOnKeys/RayX`, runs made at `be358b9` |

One HPX install served every slice. The recorded evidence used two separate installs (a plain
one for Slices 1 to 4 and a supervision-enabled one for Slice 3B). The supervision options only
add components, so the single install is a superset of both.

**HPX build** (Slurm job 206520): medusa00, 40 cores, 537 build steps, 155 s, exit code 0. The
installed `hpx/config/defines.hpp` defines `HPX_HAVE_GIT_COMMIT "cc2be553..."`,
`HPX_HAVE_SUPERVISION` and `HPX_HAVE_FORCE_DISCONNECT`, and the install contains
`libhpx_supervision` and `libhpx_supervision_dispatch`.

**exp68 build** (the workload every slice reuses): built cleanly against the new install with no
source changes, so the HPX API that exp68 uses is unchanged between `20bc3d4b` and `cc2be553`.

**Launcher changes needed to run it:** the exp68 and exp70 launchers hardcoded the original
author's repo, venv and HPX paths. Commits `be358b9` and `bdfe157` make them read optional
`RAYX_REPO`, `RAYX_VENV`, `RAYX_HPX_INSTALL`, `RAYX_HPX_SHA` (and `RAYX_HPX_SRC` for Slice 3B)
overrides, with unchanged defaults. Run manifests now record the HPX install and SHA actually
used. The Slice 3B launcher no longer moves an existing install's source checkout and refuses to
run if the source is not at `RAYX_HPX_SHA`. The `#SBATCH -o` log path cannot be overridden from
inside the script, so every submission passed `-o` explicitly.

## Selftests

| Slice | Result |
|---|---|
| 1 | 60/60 |
| 2A | 73/73 |
| 3A | 96/96 |
| 3B | ok, no failures |
| 4A | 115/115 |

## Results by slice

### Slice 1: whole-island replacement

Job 206548, medusa00 (controller, Ray head, root, actor A) and medusa01 (actor B, the victim).

| Run | Overall | Gate groups | Post-loss membership |
|---|---|---|---|
| 20261010T085432Z | pass | 19/19 (121 checks) | `membership_stale` |
| 20261010T085449Z | pass | 19/19 (121 checks) | `membership_stale` |

Matches the recorded evidence. The Ray `worker died ... SYSTEM_ERROR` line in the job log is the
intentional SIGKILL of actor B. No Ray orphans on either node.

### Slice 2A: explicit completion

Job 206549, medusa00 and medusa01. Former fixed serving window 3 s, configured idle 6 s.

| Run | Overall | Gate groups |
|---|---|---|
| 20261010T085929Z | pass | 15/15 (109 checks) |
| 20261010T085947Z | pass | 15/15 (109 checks) |

Matches the recorded evidence.

### Slice 3A: classified connector departure vs loss

Job 206550, medusa11 and medusa12.

| Run | Overall | Gate groups | Graceful arm | Loss arm | Loss-arm membership |
|---|---|---|---|---|---|
| 20261010T085930Z | pass | 28/28 (176 checks) | `graceful_departure` | `unexpected_loss` | `membership_stale` |
| 20261010T085945Z | pass | 28/28 (176 checks) | `graceful_departure` | `unexpected_loss` | `membership_stale` |

Matches the recorded evidence. The graceful arm's membership shrank after departure in both runs
(`membership_shrank`). That is observational and not gated, and the curated record of the
original runs does not include it, so there is nothing to compare it with.

### Slice 4A: explicit root completion vs suspected root loss

Job 206551, medusa13 and medusa14. Suspicion bound 5.0 s, expected external witness refresh
0.2 s.

| Run | Overall | Gate groups | Normal arm | Loss arm | Observed silence |
|---|---|---|---|---|---|
| 20261010T085932Z | pass | 31/31 (205 checks) | `explicit_completion` | `suspected_root_loss` | 5.014 s |
| 20261010T090001Z | pass | 31/31 (205 checks) | `explicit_completion` | `suspected_root_loss` | 5.014 s |

The classification result matches the recorded evidence. The post-loss actor observations do
not (see the next section).

### Slice 3B: HPX-native connector loss classification and recovery

Four single-node runs, chained so they did not share the native build directory at the same
time. The two later runs were moved off medusa02 and medusa03, which were busy, onto medusa08
and medusa11.

| Run | Job | Node | Gates | Final stale dispatch | `force_disconnect` rc | Root exit | `root.finish_note` |
|---|---|---|---|---|---|---|---|
| 20261010T140358Z | 206552 | medusa00 | 16/16 | `rejected_fenced` | 0 | `hung_killed_by_harness` | written |
| 20261010T141311Z | 206553 | medusa01 | 16/16 | `rejected_fenced` | 0 | `hung_killed_by_harness` | written |
| 20261010T141747Z | 206554 | medusa08 | 16/16 | `rejected_fenced` | 0 | `hung_killed_by_harness` | written |
| 20261010T141859Z | 206555 | medusa11 | 16/16 | `rejected_fenced` | 0 | `hung_killed_by_harness` | **missing** |

In every run:

* connector B was killed with SIGKILL, with no graceful disconnect;
* zero failure events were published by the application, so HPX classified the failure itself;
* the stale incarnation was fenced (`rejected_fenced`);
* after `hpx::force_disconnect`, AGAS resolution of the departed locality failed, membership
  shrank to 2, and a root dispatch to it failed as `not_found`;
* replacement connector C joined as a distinct incarnation and ran verified work.

This matches the recorded 16/16 result on `bec10f39`, now across four nodes instead of one.

**Shutdown.** Every run ended with the harness killing the root after 30 s, as recorded. The
point where the root hung is not the same in every run:

* In runs 1 to 3, the root wrote `root.finish_note` (`{"rc":0,"note":"slice3b sequence
  complete"}`). The harness writes that file immediately after `hpx::supervision::finalize()`
  returns, so these runs hung later, in `hpx::finalize()` / `hpx::stop()`. All four recorded runs
  on `bec10f39` behaved this way.
* In run 4 (medusa11), the root had already written its verified result, but `root.finish_note`
  was never written. The most likely reading is that this run hung inside
  `hpx::supervision::finalize()` itself.

This is one occurrence, and the same confound applies as before: the harness waits for the root
to exit before stopping connectors A and C, so the console finalizes while two connect-mode
localities are still attached. Neither hang point is attributable to HPX from this evidence.

## Finding: post-root-loss actor calls now fail promptly

exp70 key finding 1 says: after unexpected root loss, actor-hosted HPX calls blocked rather than
failing promptly, and every post-loss actor probe hit its bound.

| | Recorded (HPX `20bc3d4b`, medusa00/01) | This reproduction (HPX `cc2be553`, medusa13/14) |
|---|---|---|
| Actor A | `call_timeout` after 15.001 s | `call_error_result` after 0.010 s and 0.007 s |
| Actor B | `call_timeout` after 15.001 s | `call_error_result` after 0.009 s and 0.006 s |
| Returned value | none (bound reached) | `{"ok": false, "error": "RuntimeError: Connection reset by peer: "}` |

Both runs agree with each other, and every probe stayed inside its 15 s bound, so all observation
gates pass. The slice is designed to record whichever category occurs. This is a change in
observed behaviour, not a test failure.

**What it suggests:** on this build and these nodes, an actor-hosted HPX locality notices that
the root's TCP connection is gone and fails the call immediately, instead of blocking.

The external suspected-root-loss classification is unaffected. It still needed the full 5 s of
witness silence. An HPX connection error is not a classified lifecycle event.

### Control: same nodes, two HPX builds

Three things differed between the first reproduction and the recorded run: the HPX commit, the
nodes (medusa13/14 vs medusa00/01) and the pybind11 version. To separate them, Slice 4A was run
twice more on **medusa00/01**, the recorded node pair, from the same venv (Ray 2.55.1, pybind11
3.1.0) and the same repo commit (`90a2f59`). Only the HPX install, and the exp68 build linked
against it, differed between the two jobs. The jobs were chained so they did not overlap.

| Job | HPX | Run | Actor A | Actor B | Overall |
|---|---|---|---|---|---|
| 206558 | `20bc3d4b` | 20261010T110316Z | `call_timeout`, 15.002 s | `call_timeout`, 15.001 s | pass |
| 206558 | `20bc3d4b` | 20261010T110416Z | `call_timeout`, 15.002 s | `call_timeout`, 15.002 s | pass |
| 206559 | `cc2be553` | 20261010T110522Z | `call_error_result`, 0.008 s | `call_error_result`, 0.007 s | pass |
| 206559 | `cc2be553` | 20261010T110552Z | `call_error_result`, 0.006 s | `call_error_result`, 0.006 s | pass |

Root loss was classified as `suspected_root_loss` at 5.013 s of silence in all four runs.

**Result:** on the recorded nodes, the recorded HPX commit reproduces the recorded behaviour
exactly, and the current HPX commit fails promptly, as it also did on medusa13/14. The change
follows the HPX build. The nodes and the pybind11 version are ruled out.

**How the old HPX was built:** HPX `20bc3d4b` with the same options as the main build, except
the supervision options, which that commit predates (`HPX_WITH_DISTRIBUTED_RUNTIME`,
`HPX_WITH_NETWORKING`, `HPX_WITH_PARCELPORT_TCP` ON; examples and tests OFF; Release, C++20,
`MALLOC=system`). Build job 206557 on medusa00 succeeded. The installed `defines.hpp` carries
`20bc3d4b`, and the old exp68 build was checked with `ldd` to link that install's
`libhpx.so.2`.

At this commit stdexec is mandatory and is fetched from the stdexec `main` branch. The first
build attempt (job 206556) fetched stdexec `main` as of 2026-10-09 (`5eee1768`), which no longer
declares `stdexec::tag_invoke` and related names, so HPX failed to compile. The stdexec checkout
was therefore pinned to `711da597` (2026-07-12), the last stdexec `main` commit before the HPX
commit date (2026-07-13).

**What it still does not establish:**

* which HPX commit between `20bc3d4b` and `cc2be553` made the difference;
* whether the change is in HPX itself or in a dependency fetched with it. The two builds use
  different stdexec versions (pinned July version vs October `main`, exact commit not recorded),
  and Asio may also differ. A TCP connection-reset behaviour points to HPX's networking layer
  rather than to stdexec, but this control does not prove that.

## Not run

* **Slice 0** (the two-process HPX-only loopback reproducer). Its runner was retargeted
  (`bdfe157`) but not executed. Its case 1 gates hard-code the original error shape
  (`std::system_error`, code 1, thrown at the `async` call). HPX #7384, found in this slice, has
  since been fixed upstream, so that shape may differ on current HPX.
* **Local phases** of Slices 1 to 4. Only their Rostam cross-node phases were run. Slice 3B has
  only a local phase (`run_slice3b.py --phase local`), which is what ran on each Rostam node.

## Claims and non-claims

Supported:

> On HPX master `cc2be553`, exp70 Slices 1, 2A, 3A and 4A reproduce cross-node on Rostam (2/2 runs
> each), and Slice 3B reproduces 16/16 in 4/4 runs on four nodes, with shutdown still
> INCONCLUSIVE. After unexpected root loss, the actor-hosted HPX calls failed promptly with a
> connection reset, instead of blocking to the 15 s bound. A same-node control on medusa00/01
> shows this follows the HPX build: HPX `20bc3d4b` still blocks to the bound there, and HPX
> `cc2be553` fails in milliseconds.

Not claimed:

* no performance, production or fault-tolerance claim;
* no claim that the Slice 3B shutdown hang is an HPX defect, or that it is fixed;
* no attribution of the Slice 4A behaviour change to a specific HPX commit inside
  `20bc3d4b..cc2be553`, and no separation of HPX from its fetched dependencies;
* no claim that prompt failure is guaranteed for other failure modes, topologies or networks;
* no Ray versus HPX comparison.

## Roadmap impact

**Roadmap changed.** The external lifecycle contract and the Slice 3B native path both hold on
current HPX. Key finding 1 (post-root-loss calls block) is specific to HPX `20bc3d4b`. On current
master the surviving actor-hosted localities fail calls promptly after root loss, so the
supervisor no longer depends on its call bounds just to avoid being stranded in this scenario.
The external bounded root-loss classification is still needed, because a connection error is not
a classified HPX lifecycle event.

## Next step

Bisect `20bc3d4b..cc2be553` on Slice 4A at medusa00/01, keeping the venv and nodes fixed, to find
the HPX commit after which post-root-loss actor calls fail promptly.
