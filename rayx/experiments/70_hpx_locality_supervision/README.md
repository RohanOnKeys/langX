# exp70: HPX locality lifecycle supervision

**Status:** the A-path is complete and hardware-verified on two-node Rostam hardware. As of
2026-08-18, the B-path is no longer uniformly "no HPX API exists": upstream landed
`components/supervision_dispatch` + `hpx::force_disconnect` (HPX #7390/#7441, merged PR #7447).
Slice 3B is now **complete and PASSES 16/16 on hardware** (2026-09-08, job 192305, medusa00)
against upstream HPX `bec10f39`, after the AGAS symbol-routing defect that blocked the 08-18 run
was fixed upstream; Slice 4B's blocking reasons **narrowed but did not close** (recovery via
`force_disconnect` is structurally excluded for the console/root locality); Slice 2B's status is
unchanged from the 07-30 assessment (unblocked in principle, no native harness built yet). See
[`native_backend_gap_matrix.md`](native_backend_gap_matrix.md) for the full, dated,
non-destructive chronology.

## Question

When Ray actors host HPX connect-mode localities in-process, who is allowed to say that a
locality is *done*, and who is allowed to say that a locality is *gone*?

HPX can admit dynamically connecting localities, but it does not expose either fact as a
classified runtime lifecycle event. exp70 asks how far an **external** supervisor can close that
gap, and exactly where the external approach stops.

## Architecture

```text
HPX:
    distributed C++ actions, futures, composition, and locality membership

RayX / Ray:
    placement, process and actor supervision, lifecycle policy,
    whole-island failure classification, and whole-island replacement
```

The completed experiments do **not** claim transparent HPX recovery.

Concretely:

```text
Ray actors host HPX connect-mode localities in-process
        ↓
a separately supervised work-free HPX root anchors the runtime
        ↓
external lifecycle backends classify completion or bounded suspicion
        ↓
RayX treats unexpected loss as whole-island failure
        ↓
Ray replaces the complete island
```

The work-free root is a separately supervised **process**, never a Ray actor. It runs no
application work; it anchors the runtime and observes membership.

### The lifecycle states nobody reports

A connector observing the root needs to distinguish:

```text
root alive but idle
root completed normally
root disappeared unexpectedly
```

A root or supervisor observing a connector needs to distinguish:

```text
connector alive but idle
connector departed gracefully
connector disappeared unexpectedly
```

Current HPX exposes **none** of these as classified runtime lifecycle facts. Membership can be
polled, but polling cannot separate "idle" from "gone", and — as exp70 measured — membership can
still contain a departed locality at the observation point.

## Why fixed connector lifetimes fail

Before exp70, connectors used a fixed serving window: serve for N seconds, then leave. That
encodes a guess about how long work will keep arriving. Slice 0 reduces the failure to two
processes: a connector with a fixed 3 s window departs, the root is then idle for 6 s, and a
later *valid* dispatch to the departed locality fails.

The lifetime was never the real problem. The real problem is that "no further work will be sent"
was never expressible, so it had to be guessed.

## External lifecycle contract

Every slice implements a **backend-neutral** contract so a future HPX-native backend can be
substituted without changing the state machine, the gates, or the event vocabulary. Each slice
already selftests a substitute backend passing its identical gates.

| Slice | Acceptance surface |
|---|---|
| 2A | `publish_complete(epoch, connectors, payload)` / `observe_completion(epoch, connector, bound)` |
| 3A | `publish_event(epoch, connector, kind, digest, classification)` / `observe_events(epoch, observer, bound)` |
| 4A | `observe_root_event(epoch, root_identity, bound)` / `publish_completion(epoch, root_identity, payload)` |

### External witness variants — be precise

The two witness mechanisms are **not** the same, and neither is an HPX-native heartbeat:

* **Slice 0** — `root.alive` is **dispatch-driven activity evidence**: the root bumps it
  immediately *before* it dispatches. It is **not periodic**. The connector's deadman therefore
  fires only on silence longer than its bound.
* **Slices 1–4** — the separately supervised work-free root refreshes an **external
  root-liveness witness periodically** (~200 ms). It is periodic, but it remains **outside HPX**.

Throughout this experiment, "heartbeat" is always written as **external** heartbeat unless it
refers to a hypothetical future HPX-native facility.

## Experiment roadmap

| Slice | Question | Status |
|---|---|---|
| 0 | Why fixed connector lifetimes fail | Complete |
| 1 | Can Ray replace the complete failed island? | Complete |
| 2A | Can completion be explicit and testable? | Complete |
| 2B | HPX-native completion backend | Unblocked in principle (2026-07-30); no native harness implemented yet |
| 3A | Can connector loss be classified externally? | Complete |
| 3B | HPX-native connector departure/loss event + recovery | **Complete — 16/16 gates on Rostam hardware (2026-09-08, HPX `bec10f39`). The 2026-08-18 `discover_and_join()` block was an upstream AGAS routing defect, fixed upstream. Shutdown separately INCONCLUSIVE — see gap matrix** |
| 4A | Can root loss be classified externally? | Complete |
| 4B | HPX-native root completion/loss event | Narrowed (2026-08-18): detection plausible, recovery structurally excluded — see gap matrix |

The A slices are **executable external reference implementations and acceptance harnesses**. The
B slices must be satisfied by **real HPX-native runtime facts**. Moving the existing external
observations behind a class named "native" would **not** satisfy a B slice.

## Slice results

### Slice 0 — reduced lifecycle gap and external workaround

Two-process loopback topology: one root, one connector. Demonstration timings — **not defaults**
— are a 3 s former serving window, a 6 s idle interval, and a 15 s external connector deadman
measured on the connector's own steady clock.

Case 1 reproduces the failure: the connector's fixed window expires, and a later valid dispatch
to the departed locality fails. Case 2 is the workaround: a dispatch-driven `root.alive` witness
plus an explicit `root.done` completion marker lets the connector stay for an idle interval
longer than case 1's entire lifetime and then serve a valid dispatch.

While reducing this, a separate defect was found in `addressing_service::resolve_locality`
during **stale-target dispatch**: a manual `unlock()` on a lock already released by
`unlock_guard` masked the intended `bad_parameter` error. That is HPX issue **#7384**; it is
fixed and its regression-test PR is approved. **#7384 fixes error reporting, not lifecycle
supervision** — the lifecycle gap this experiment is about is untouched by it.

Reproducer: [`upstream_reproducer/`](upstream_reproducer/README.md) (public).

### Slice 1 — Ray-supervised whole-island replacement

Actor-hosted HPX shared island: a separately supervised work-free root plus actor A and actor B
each hosting an HPX locality **in-process**. One remote connector is killed unexpectedly.
Classification is observation-only and injection-blind. The complete old island — including the
root and the *surviving* connector — is discarded, a fresh island is constructed, and the same
deterministic workload passes again on the replacement.

Post-loss membership remained `membership_stale` in the accepted runs. This was **observational,
not required**.

> Unexpected loss of one remotely placed actor-hosted HPX locality caused the RayX supervisor to
> classify the complete island as failed, discard the old root and surviving connector,
> construct a fresh cross-node island, and verify the same deterministic distributed HPX
> workload on the replacement.

[`slice1_actor_hosted_island_restart/`](slice1_actor_hosted_island_restart/README.md)

### Slice 2A — explicit completion

Backend-neutral explicit-completion contract. The first workload is verified; the island then
sits idle for longer than the former fixed serving window and both connectors **remain
available**; a *distinct* second workload is verified; only then is completion published. A
duplicate publication is rejected, and a post-completion controller dispatch is rejected
**before it could reach HPX**. Both connectors observe epoch-matched completion, depart through
the validated disconnect/stop sequence, and the root returns to membership one and finalizes
cleanly.

> In a two-node actor-hosted HPX island, both connectors remained available across an idle
> interval longer than the former fixed serving window, accepted later valid distributed work,
> and departed cleanly only after explicit completion was published through the external backend.

[`slice2_explicit_completion/`](slice2_explicit_completion/README.md)

### Slice 3A — classified connector departure vs loss

One shared backend-neutral connector-lifecycle schema drives two arms: a graceful-control arm
and an unexpected-loss arm. The decisive design point is that a **graceful HPX departure is
distinct from host-process death** — after a graceful stop the hosting actor stays answerable,
while a lost connector takes its host with it.

The classifier reads a strict evidence allow-list and **cannot read injection metadata**
(`BlindnessViolation` is raised structurally). The graceful arm produces `graceful_departure`;
the loss arm produces `unexpected_loss`. Post-loss HPX membership remained stale in the accepted
runs but was **not gated**.

> The external connector-lifecycle backend classified unexpected loss of a remotely placed
> actor-hosted locality distinctly from normal graceful departure and recorded the bounded HPX
> membership behavior visible at the observation point.

[`slice3_connector_loss_event/`](slice3_connector_loss_event/README.md)

### Slice 3B — native validation against upstream supervision_dispatch + force_disconnect

Added 2026-08-18 after upstream HPX landed `components/supervision_dispatch` +
`hpx::force_disconnect` (issues #7390/#7441, merged PR #7447). Reuses Slice 3's exact topology
(one standalone work-free root, Ray-actor-hosted connectors) but replaces the external evidence
backend with the real upstream API for a single silent-crash-then-recover scenario: root
observes native classification (`failure_detection_loop()`, zero application code — the harness
never calls `publish_event(event::failed)` itself), observes fencing
(`hpx::supervision::check_admission()`), then **explicitly** invokes `hpx::force_disconnect`
before a replacement connector joins as a provably distinct incarnation.

**Status (2026-09-08): PASS — 16/16 gates, 32/32 selftest, Rostam jobs 192301–192305, medusa00,
upstream HPX `bec10f395059738193a1d53d1e8f02acc01f4ce2`.** HPX itself classified the SIGKILLed
connector as `event::failed` with zero application-published failure events; the stale
incarnation was fenced via `check_admission()` → `rejected_fenced`; `force_disconnect` from the
console produced AGAS, membership, root-dispatch and third-party-connector cleanup evidence; and
a replacement connector joined as a distinct incarnation and ran verified work. Deterministic
across four runs. `shutdown_result` is separately **INCONCLUSIVE** and not attributed to
upstream: `supervision::finalize()` returned, `hpx::finalize()`/`hpx::stop()` then hung, but the
harness's teardown ordering confounds attribution.

*Superseded status (2026-08-18): native harness implemented, run on Rostam hardware
(job 185466-185474, medusa00) — blocked before any gate.* A fresh isolated HPX install with
`-DHPX_WITH_SUPERVISION=ON` built and linked correctly; root and both connectors started and
each connector's own `supervision_init()` succeeded. The run then consistently failed at
`hpx::supervision::discover_and_join()`, which found zero cross-process peers in both directions
across five consecutive attempts, despite each side's own registry name self-resolving
successfully. That was traced to `symbol_namespace_locality()` bounding explicit
`/<locality-id>/` routing by `get_initial_num_localities()` (which stays 1 in a late-connect
topology) and **fixed upstream** in commit `d5af2db3` via PR #7471. The diagnostic trail is kept
in the Slice 3B section below as provenance for that defect report.

[`slice3_connector_loss_event/README.md`](slice3_connector_loss_event/README.md) (Slice 3B
section) · [`native_backend_gap_matrix.md`](native_backend_gap_matrix.md) (gate-by-gate mapping)

### Slice 4A — explicit root completion vs bounded suspected root loss

One backend-neutral root-lifecycle schema, two arms, and the **same classifier fingerprint in
every accepted arm**. Strict evidence allow-list; the root-kill injection record is stored
outside the evidence and is inaccessible to the classifier.

Suspicion bound 5 s against a 0.2 s expected external-witness refresh. Healthy pre-bound silence
does **not** classify loss — a healthy refreshing root observed below the bound yields only an
observation timeout. Root death does **not** bypass the configured policy bound. Both loss arms
classified `suspected_root_loss`, and the poisoned island was discarded.

> In a two-node actor-hosted HPX island, the external root-lifecycle backend distinguished
> explicit root completion from bounded suspicion after unexpected loss of the separately
> supervised work-free root, while the supervisor discarded the poisoned island.

[`slice4_root_loss_event/`](slice4_root_loss_event/README.md)

## Key findings

1. **After unexpected root loss, actor-hosted HPX calls blocked rather than failing promptly.
   The supervisor required bounded observations to avoid becoming stranded.** Every post-loss
   actor probe hit its bound instead of returning an error. This is not generalized beyond the
   tested HPX build and topology.
2. Membership polling is not a loss signal: after an ungraceful departure the membership
   snapshot still contained the departed locality at the observation point.
3. Graceful departure is separable from loss using only observable evidence — but only because
   a gracefully departed connector's *host* stays answerable.
4. The external ceiling is **bounded suspicion, never certainty**. Silence cannot be
   distinguished from a stalled-but-live locality without a runtime liveness guarantee.

## Recovery boundary

RayX owns whole-island replacement. HPX is asked only to expose lifecycle facts.

For **root/console loss**, and for connector loss on builds without
`components/supervision_dispatch` + `hpx::force_disconnect`, unexpected loss poisons the island:
exp51/exp53 and Slice 1 all show that the safe recovery boundary is discarding the complete
island — root and survivors — and constructing a fresh one. The A-slices add no partial-island
continuation and no AGAS repair.

**Slice 3B is a scoped exception, validated 2026-09-08 on HPX `bec10f39`.** For the silent loss
of a *late-connected, non-console* locality, the island survived without a whole-island restart:
HPX runtime-classified the stale incarnation as `failed` and fenced it, the surviving console
explicitly called `hpx::force_disconnect`, AGAS/membership/connection-cache cleanup was observed
(including from a third-party connector), surviving connector A stayed useful, and replacement
connector C joined and executed verified work.

That exception does **not** extend to root/console loss, to arbitrary locality classes, or to
automatic recovery, and it is not a generic fault-tolerance claim. `force_disconnect` remains an
explicit orchestration action taken by the console after observing fencing — HPX never chains it
from a fencing event — and it cannot target the console/root at all.

## Native HPX gaps

```text
Slice 2B requires: HPX-native explicit-completion publication and observation
                    -- unblocked in principle since 2026-07-30; no native harness built yet
Slice 3B requires: HPX-native connector departure/loss classification + recovery
                    -- SATISFIED on current upstream HPX bec10f39 (2026-09-08, Rostam job
                       192305, medusa00): semantic result 16/16 PASS over the full path --
                       late join -> useful work -> hard crash (SIGKILL) -> runtime-native
                       failed classification -> specific fencing -> explicit root-side
                       hpx::force_disconnect -> observable cleanup -> replacement join ->
                       verified work. (Previously blocked on 7b88345, 2026-08-18, because
                       cross-process supervision-registry lookup failed in AGAS; that defect
                       was fixed upstream in d5af2db3 / PR #7471. The 08-18 evidence is kept
                       as provenance.) Shutdown is tracked separately and remains
                       INCONCLUSIVE -- see native_backend_gap_matrix.md
Slice 4B requires: HPX-native root completion/loss or runtime liveness notification
                    -- narrowed 2026-08-18: detection plausible via the same
                       failure_detection_loop() mechanism, which the 2026-09-08 run showed
                       DOES work cross-process for a non-console locality. Recovery stays
                       structurally excluded regardless: hpx::force_disconnect can never
                       target the console/root locality
```

Gate-by-gate mapping from each A-slice acceptance gate to the upstream runtime fact its B
equivalent needs, plus the full 2026-08-18 update: [`native_backend_gap_matrix.md`](native_backend_gap_matrix.md).

## Supported claims

> exp70 demonstrates an executable external lifecycle contract for dynamically connected
> actor-hosted HPX localities: explicit completion, classified connector departure or loss,
> classified root completion or bounded suspected loss, and supervisor-owned whole-island
> replacement.

Per-slice claims are quoted verbatim in the slice sections above and in
[`evidence_index.md`](evidence_index.md).

## Non-claims

exp70 does **not** demonstrate:

* an HPX-native heartbeat;
* HPX-native completion;
* HPX-native connector-loss notification;
* HPX-native root-loss notification;
* authoritative failure certainty;
* transparent HPX recovery;
* partial-island continuation;
* automatic AGAS repair;
* application-state restoration;
* any performance improvement;
* universal behavior across all HPX builds, networks, or failure modes.

## Reproducing the tests

The selftests are pure logic checks — no Ray, no HPX, no Slurm — and run anywhere:

```bash
cd experiments/70_hpx_locality_supervision
python3 slice1_actor_hosted_island_restart/run_slice1.py --selftest
python3 slice2_explicit_completion/run_slice2.py --selftest
python3 slice3_connector_loss_event/run_slice3.py --selftest
python3 slice3_connector_loss_event/run_slice3b.py --selftest
python3 slice4_root_loss_event/run_slice4.py --selftest
```

The local live phase needs Ray plus a built exp68 (`experiments/68_vocab_sharded_topk`):

```bash
python3 slice2_explicit_completion/run_slice2.py --phase local
python3 slice3_connector_loss_event/run_slice3.py --phase local
python3 slice4_root_loss_event/run_slice4.py --phase local
```

Slice 3B's local-live phase additionally needs `slice3_connector_loss_event/native/` built
against an HPX install configured with `-DHPX_WITH_SUPERVISION=ON` — not the same exp68 build
the other slices use:

```bash
python3 slice3_connector_loss_event/run_slice3b.py --phase local
```

The cross-node phase skips cleanly with no Slurm submission when run off-cluster. On a cluster
it is driven by the per-slice launchers `exp70_slice1_crossnode.sbatch`,
`exp70_slice2_crossnode.sbatch`, `exp70_slice3_crossnode.sbatch` and
`exp70_slice4_crossnode.sbatch`. Those launchers encode a specific site layout: reproducing the
hardware evidence requires an equivalent Slurm, Ray, HPX and network setup, not just the scripts.

## Evidence and artifacts

Per-slice records — runners, launchers, selftest counts, jobs, node pairs, run ids, curated
evidence and hash verification — are in [`evidence_index.md`](evidence_index.md).

Curated evidence JSON files are tracked. Raw run directories, copy-back trees, build trees and
logs are deliberately untracked (see `.gitignore`); they remain on disk for local inspection.

An independent reproduction against HPX master `cc2be553` (2026-10-10) is in
[`reproduction_hpx_cc2be553.md`](reproduction_hpx_cc2be553.md). Slices 1 to 4 and 3B reproduce
there, but after root loss the actor-hosted HPX calls failed promptly instead of blocking, so
key finding 1 above did not reproduce. A same-node control ties that change to the HPX build:
HPX `20bc3d4b` still blocks, HPX `cc2be553` fails in milliseconds.

## Relationship to upstream HPX work

* **HPX #7384** — found during stale-target dispatch in Slice 0; posted, fixed, regression-test
  PR approved. It concerns **error reporting**, not lifecycle supervision. No further work is
  planned on it unless a maintainer asks.
* **Lifecycle feature request (#7390)** — asks HPX to expose completion and classified
  departure/loss events for dynamically connected localities. The A-slices are the executable
  external reference tests it cites.
* **[`upstream_acceptance_contract.md`](upstream_acceptance_contract.md)** — the
  maintainer-facing companion to #7390: how each completed A-slice becomes an unchanged
  acceptance test for a future HPX-native lifecycle API, plus the suggested contribution
  sequence. Start there if you are reviewing this work from the HPX side.

## Future work

* Slice 3B's hardware evidence is **done** (2026-09-08, jobs 192301–192305, HPX `bec10f39`,
  16/16). The concrete next step is Slice 2B (explicit completion), which is now implementable
  against the same `hpx::supervision` API without a new upstream ask — the discovery mechanism
  it depends on is hardware-proven by 3B.
* Two narrower follow-ups, both smaller than a slice: a teardown-ordering control (stop
  connectors A/C *before* waiting on root exit) to decide whether Slice 3B's shutdown hang is
  the upstream shutdown issue or purely harness ordering; and a successful (non-fenced)
  cross-binary `dispatch_work<Action>()` dispatch, which the 3B run did not exercise because the
  fence short-circuits before the wrapped action is invoked.
* Slice 4B has no equivalent next step: `hpx::force_disconnect` cannot target the console/root by
  design, so root-loss recovery stays genuinely upstream-blocked regardless of further local
  work. Root-loss *detection* narrowed (see the gap matrix) but was not independently prototyped
  here, per this update's explicit scope ("do not try to solve root loss").
