"""Gates G1 to G5 and metrics for exp01 (plan section 14).

Reads the trial records that ``run_exp01.py`` appended to ``exp01.jsonl``,
groups them by tag (scenario, plus the Tier B mode for F3 and the durability
for F7), and writes ``aggregate.json``. Prints one table row per tag and
exits non-zero if a gate fails.

  G1  the run completed and its final state hash equals the baseline
  G2  outbox keys equal the baseline's, no key applied twice
  G3  no commit accepted from a worker after it was seen fenced (gated in
      F4, F6 and F9, reported everywhere)
  G4a the run completed within the trial's run timeout
  G5  every fence the coordinator recorded came from HPX state or an HPX
      dispatch error, never from the harness
  G4b exit paths are recorded, not gated

    python experiments/01_durable_langgraph/analyze_exp01.py \\
        experiments/01_durable_langgraph/_runs/exp01/exp01.jsonl
"""

import argparse
import collections
import json
import pathlib
import sys

HPX_FENCE_REASONS = {"target_fenced", "dispatch_failed", "fenced_at_commit"}
G3_GATED = {"F4", "F6", "F9"}


def stats(xs):
    """Median, p95 (nearest rank), min and max, or None without data."""
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    rank = lambda q: xs[min(len(xs) - 1, max(0, -(-len(xs) * q // 100) - 1))]  # noqa: E731
    return {"n": len(xs), "median": rank(50), "p95": rank(95),
            "min": xs[0], "max": xs[-1]}


def gates(r):
    """Per-trial gate results."""
    hpx = r.get("hpx", {})
    ob = r.get("outbox", {})
    return {
        "G1": bool(hpx.get("completed") and hpx.get("hash_equal")),
        "G2": ob.get("duplicates") == 0 and bool(ob.get("keys_equal")),
        "G3": r.get("stale_commits") == 0,
        "G4a": bool(hpx.get("completed")) and not r.get("trial_timed_out"),
        "G5": set(r.get("fence_reasons", {}).values()) <= HPX_FENCE_REASONS,
    }


def aggregate(records):
    by_tag = collections.defaultdict(list)
    for r in records:
        by_tag[r.get("tag", r.get("scenario"))].append(r)
    out = {}
    for tag, rs in sorted(by_tag.items()):
        scenario = rs[0].get("scenario")
        per = [gates(r) for r in rs]
        g = {k: sum(p[k] for p in per) for k in per[0]}
        failed = [k for k, v in g.items() if v < len(rs)
                  and (k != "G3" or scenario in G3_GATED)]
        signaled = [r for r in rs if r.get("signals")]
        detected = [r for r in signaled
                    if {str(s["locality"]) for s in r["signals"]}
                    <= set(r.get("detection_ms", {}))]
        exits = collections.Counter()
        for r in rs:
            ex = r.get("exit", {})
            for name, path in (ex.get("workers") or {}).items():
                exits[f"worker:{path}"] += 1
            c = ex.get("coordinator")
            for path in (c.values() if isinstance(c, dict) else [c]):
                exits[f"coordinator:{path}"] += 1
            exits[f"root:{ex.get('root')}"] += 1
        a = {
            "scenario": scenario, "trials": len(rs), "params": rs[0].get("params"),
            "gates": {k: f"{v}/{len(rs)}" for k, v in g.items()},
            "gates_failed": failed,
            "detection_ms": stats([v for r in rs
                                   for v in r.get("detection_ms", {}).values()]),
            "detected": f"{len(detected)}/{len(signaled)}" if signaled else None,
            "recovery_ms": stats([r.get("recovery_ms") for r in rs]),
            "wall_ratio": stats([round(r["hpx"]["wall_s"] / r["baseline"]["wall_s"], 2)
                                 for r in rs if r.get("hpx", {}).get("wall_s")
                                 and r.get("baseline", {}).get("wall_s")]),
            "retries": stats([len(r.get("retries", [])) for r in rs]),
            "rejected": sum(len(r.get("rejected", [])) for r in rs),
            "late_replies": sum(len(r.get("late_replies", [])) for r in rs),
            "first_attempt_committed": sum(len(r.get("first_attempt_committed", []))
                                           for r in rs),
            "trials_with_unsignaled_fence": sum(bool(r.get("unsignaled_fenced"))
                                                for r in rs),
            "exit_paths": dict(sorted(exits.items())),
        }
        if scenario == "F3":
            a["tier_b_duplicates"] = stats([(r.get("tier_b") or {}).get("duplicates")
                                            for r in rs])
            a["tier_b_baseline_duplicates"] = stats(
                [(r.get("baseline", {}).get("tier_b") or {}).get("duplicates")
                 for r in rs])
        if scenario == "F6":
            a["replacement_commits"] = stats([r.get("replacement_commits")
                                              for r in rs])
            a["replace_errors"] = [r["notes"]["replace_error"] for r in rs
                                   if r.get("notes", {}).get("replace_error")]
        if scenario == "F7":
            a["c2_ready_ms"] = stats([r.get("c2_ready_ms") for r in rs])
            a["rerun_after_commit"] = stats([r.get("rerun_after_commit") for r in rs])
            a["root_alive"] = f"{sum(bool(r.get('root_alive')) for r in rs)}/{len(rs)}"
        if scenario == "F8":
            a["false_positive_rate"] = round(
                sum(bool(r.get("detection_ms")) for r in rs) / len(rs), 3)
        out[tag] = a
    return out


def fmt(s):
    return "-" if not s else f"{s['median']} [{s['min']}, {s['max']}]"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("records", nargs="+", help="exp01.jsonl files")
    ap.add_argument("--out", default="", help="aggregate.json path; default "
                    "next to the first records file")
    a = ap.parse_args()
    records = []
    for path in a.records:
        records += [json.loads(x) for x in pathlib.Path(path).read_text().splitlines()
                    if x.strip()]
    agg = aggregate(records)
    out = pathlib.Path(a.out or pathlib.Path(a.records[0]).with_name("aggregate.json"))
    out.write_text(json.dumps(agg, indent=1, sort_keys=True))
    print("| tag | trials | G1 | G2 | G3 | G4a | G5 | detection ms | recovery ms |")
    print("|---|---|---|---|---|---|---|---|---|")
    for tag, x in agg.items():
        g = x["gates"]
        print(f"| {tag} | {x['trials']} | {g['G1']} | {g['G2']} | {g['G3']} | "
              f"{g['G4a']} | {g['G5']} | {fmt(x['detection_ms'])} | "
              f"{fmt(x['recovery_ms'])} |")
    failed = {t: x["gates_failed"] for t, x in agg.items() if x["gates_failed"]}
    print(json.dumps({"gates_failed": failed, "aggregate": str(out)}))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
