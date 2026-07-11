#!/usr/bin/env python3
"""
merge_mv.py

Merge per-vantage JSONL results from v6probe_mv.py into a single per-host
dataset, compute the multi-vantage statistics, and (optionally) compare
against the published March 2026 single-vantage baseline.

Usage:

  python3 merge_mv.py results-*.jsonl \
      --baseline docs/ipv6-results-100k.json \
      --output merged-100k.json \
      --summary summary-100k.json

Outputs:
  * merged JSON: one record per host with per-vantage sub-records
  * summary JSON: all computed statistics
  * a human-readable report on stdout
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from itertools import combinations


def load_results(paths: list[str]) -> tuple[dict, list[str]]:
    """Return ({host: {vantage: record}}, ordered vantage names)."""
    by_host: dict[str, dict] = defaultdict(dict)
    vantages: list[str] = []
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                v = rec["vantage"]
                if v not in vantages:
                    vantages.append(v)
                by_host[rec["host"]][v] = rec
    return by_host, vantages


def load_baseline(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    return {r["host"]: r for r in data}


def v6_reachable(rec: dict | None) -> bool:
    return bool(rec and rec.get("v6") and rec["v6"].get("reachable"))


def v4_reachable(rec: dict | None) -> bool:
    return bool(rec and rec.get("v4") and rec["v4"].get("reachable"))


def median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    mid = len(s) // 2
    return round(s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2, 1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("results", nargs="+", help="Per-vantage JSONL files")
    p.add_argument("--baseline", help="Published ipv6-results JSON for delta analysis")
    p.add_argument("--control-vantage", default="rf",
                   help="Vantage matching the baseline's location (default: rf)")
    p.add_argument("--output", default="merged.json", help="Merged per-host output")
    p.add_argument("--summary", default="summary.json", help="Summary statistics output")
    p.add_argument("--examples", type=int, default=15,
                   help="Example hosts to list per finding, lowest rank first")
    args = p.parse_args()

    by_host, vantages = load_results(args.results)
    n_vantages = len(vantages)
    print(f"Loaded {len(by_host)} hosts across {n_vantages} vantages: "
          f"{', '.join(vantages)}\n")

    baseline = load_baseline(args.baseline) if args.baseline else {}

    merged: list[dict] = []
    summary: dict = {"vantages": vantages, "hosts": len(by_host)}

    # ------------------------------------------------------------------
    # Per-vantage headline numbers (each vantage as its own mini-survey)
    # ------------------------------------------------------------------
    per_vantage: dict[str, dict] = {}
    for v in vantages:
        recs = [by_host[h].get(v) for h in by_host]
        recs = [r for r in recs if r is not None]
        n = len(recs)
        n_aaaa = sum(1 for r in recs if r.get("has_aaaa"))
        n_reach = sum(1 for r in recs if v6_reachable(r))
        v6_ms = [r["v6"]["connect_ms"] for r in recs
                 if v6_reachable(r) and r["v6"].get("connect_ms") is not None]
        v4_ms = [r["v4"]["connect_ms"] for r in recs
                 if v4_reachable(r) and r["v4"].get("connect_ms") is not None]
        per_vantage[v] = {
            "hosts_probed": n,
            "has_aaaa": n_aaaa,
            "has_aaaa_pct": round(n_aaaa / n * 100, 1) if n else None,
            "v6_reachable": n_reach,
            "v6_reachable_pct": round(n_reach / n * 100, 1) if n else None,
            "median_v6_connect_ms": median(v6_ms),
            "median_v4_connect_ms": median(v4_ms),
        }
    summary["per_vantage"] = per_vantage

    # ------------------------------------------------------------------
    # Per-host merge + consensus classification
    # ------------------------------------------------------------------
    # Only hosts probed from ALL vantages take part in consensus stats.
    complete_hosts = [h for h in by_host if len(by_host[h]) == n_vantages]
    summary["hosts_probed_from_all_vantages"] = len(complete_hosts)

    reach_dist = Counter()          # reachable from k of N vantages
    aaaa_presence_dist = Counter()  # AAAA present at k of N vantages
    dns_divergent = []              # AAAA everywhere, but different record sets
    partial_reach = []              # reachable from some but not all vantages
    broken_everywhere = []          # AAAA somewhere, reachable nowhere
    dead_hosts = []                 # AAAA somewhere, unreachable v6 AND v4 everywhere

    for host in by_host:
        recs = by_host[host]
        rank = min(r["rank"] for r in recs.values())
        entry = {
            "rank": rank,
            "host": host,
            "vantage_count": len(recs),
            "per_vantage": {
                v: {
                    "has_aaaa": r.get("has_aaaa"),
                    "aaaa_records": r.get("aaaa_records", []),
                    "v6_reachable": v6_reachable(r),
                    "v6_http_code": (r.get("v6") or {}).get("http_code"),
                    "v6_connect_ms": (r.get("v6") or {}).get("connect_ms"),
                    "v6_attempts": (r.get("v6") or {}).get("attempts"),
                    "v4_reachable": v4_reachable(r) if r.get("v4") else None,
                    "v4_connect_ms": (r.get("v4") or {}).get("connect_ms"),
                    "error": r.get("error"),
                }
                for v, r in recs.items()
            },
        }

        if len(recs) == n_vantages:
            k_aaaa = sum(1 for r in recs.values() if r.get("has_aaaa"))
            k_reach = sum(1 for r in recs.values() if v6_reachable(r))
            aaaa_presence_dist[k_aaaa] += 1
            entry["aaaa_vantages"] = k_aaaa
            entry["v6_reachable_vantages"] = k_reach
            if k_aaaa > 0:
                reach_dist[k_reach] += 1
                if 0 < k_reach < n_vantages:
                    partial_reach.append(entry)
                elif k_reach == 0:
                    any_v4 = any(v4_reachable(r) for r in recs.values())
                    if any_v4:
                        broken_everywhere.append(entry)
                    else:
                        dead_hosts.append(entry)
                # geo-DNS divergence: AAAA at every vantage, differing sets
                if k_aaaa == n_vantages:
                    sets = {tuple(r["aaaa_records"]) for r in recs.values()}
                    entry["aaaa_sets_distinct"] = len(sets)
                    if len(sets) > 1:
                        dns_divergent.append(entry)

        merged.append(entry)

    merged.sort(key=lambda e: e["rank"])

    n_any_aaaa = sum(reach_dist.values())
    summary["consensus"] = {
        "hosts_with_aaaa_anywhere": n_any_aaaa,
        "aaaa_presence_distribution": {
            f"aaaa_at_{k}_of_{n_vantages}": aaaa_presence_dist.get(k, 0)
            for k in range(n_vantages + 1)
        },
        "v6_reachability_distribution": {
            f"reachable_from_{k}_of_{n_vantages}": reach_dist.get(k, 0)
            for k in range(n_vantages + 1)
        },
        "reachable_everywhere": reach_dist.get(n_vantages, 0),
        "partially_reachable": sum(reach_dist.get(k, 0) for k in range(1, n_vantages)),
        "unreachable_everywhere_but_v4_alive": len(broken_everywhere),
        "unreachable_everywhere_and_v4_dead": len(dead_hosts),
        "geo_dns_divergent": len(dns_divergent),
    }

    def example_list(entries: list[dict]) -> list[dict]:
        out = []
        for e in sorted(entries, key=lambda x: x["rank"])[: args.examples]:
            out.append({
                "rank": e["rank"],
                "host": e["host"],
                "reachable_from": [v for v, pv in e["per_vantage"].items()
                                   if pv["v6_reachable"]],
            })
        return out

    summary["examples"] = {
        "partially_reachable": example_list(partial_reach),
        "broken_v6_everywhere": example_list(broken_everywhere),
        "geo_dns_divergent": [
            {"rank": e["rank"], "host": e["host"],
             "distinct_aaaa_sets": e["aaaa_sets_distinct"]}
            for e in sorted(dns_divergent, key=lambda x: x["rank"])[: args.examples]
        ],
    }

    # Pairwise vantage disagreement on reachability (among AAAA hosts)
    pair_disagree = {}
    for va, vb in combinations(vantages, 2):
        d = n_pair = 0
        for h in complete_hosts:
            ra, rb = by_host[h][va], by_host[h][vb]
            if ra.get("has_aaaa") or rb.get("has_aaaa"):
                n_pair += 1
                if v6_reachable(ra) != v6_reachable(rb):
                    d += 1
        pair_disagree[f"{va}|{vb}"] = {
            "hosts": n_pair, "disagree": d,
            "disagree_pct": round(d / n_pair * 100, 2) if n_pair else None,
        }
    summary["pairwise_reachability_disagreement"] = pair_disagree

    # ------------------------------------------------------------------
    # Delta vs the March 2026 baseline (control vantage preferred)
    # ------------------------------------------------------------------
    if baseline:
        cv = args.control_vantage if args.control_vantage in vantages else vantages[0]
        if cv != args.control_vantage:
            print(f"NOTE: control vantage '{args.control_vantage}' not found; "
                  f"using '{cv}' for the baseline delta.\n")
        gained_aaaa, lost_aaaa, gained_reach, lost_reach = [], [], [], []
        n_common = 0
        for host, base in baseline.items():
            rec = by_host.get(host, {}).get(cv)
            if rec is None:
                continue
            n_common += 1
            now_aaaa, was_aaaa = bool(rec.get("has_aaaa")), bool(base.get("has_aaaa"))
            now_reach, was_reach = v6_reachable(rec), bool(base.get("ipv6_reachable"))
            item = {"rank": base["rank"], "host": host}
            if now_aaaa and not was_aaaa:
                gained_aaaa.append(item)
            if was_aaaa and not now_aaaa:
                lost_aaaa.append(item)
            if now_reach and not was_reach:
                gained_reach.append(item)
            if was_reach and not now_reach:
                lost_reach.append(item)
        summary["baseline_delta"] = {
            "control_vantage": cv,
            "hosts_in_common": n_common,
            "gained_aaaa": len(gained_aaaa),
            "lost_aaaa": len(lost_aaaa),
            "gained_v6_reachability": len(gained_reach),
            "lost_v6_reachability": len(lost_reach),
            "examples": {
                "gained_v6_reachability":
                    sorted(gained_reach, key=lambda x: x["rank"])[: args.examples],
                "lost_v6_reachability":
                    sorted(lost_reach, key=lambda x: x["rank"])[: args.examples],
            },
        }

    # ------------------------------------------------------------------
    # v4 vs v6 latency (hosts reachable over both, per vantage)
    # ------------------------------------------------------------------
    latency = {}
    for v in vantages:
        deltas = []
        for h in by_host:
            r = by_host[h].get(v)
            if (r and v6_reachable(r) and v4_reachable(r)
                    and r["v6"].get("connect_ms") is not None
                    and r["v4"].get("connect_ms") is not None):
                deltas.append(r["v6"]["connect_ms"] - r["v4"]["connect_ms"])
        latency[v] = {
            "dual_stack_hosts": len(deltas),
            "median_v6_minus_v4_connect_ms": median(deltas),
            "v6_faster_pct": round(
                sum(1 for d in deltas if d < 0) / len(deltas) * 100, 1
            ) if deltas else None,
        }
    summary["v6_vs_v4_latency"] = latency

    # ------------------------------------------------------------------
    # Write outputs and print report
    # ------------------------------------------------------------------
    with open(args.output, "w") as f:
        json.dump(merged, f, separators=(",", ":"))
    with open(args.summary, "w") as f:
        json.dump(summary, f, indent=2)

    c = summary["consensus"]
    print("=" * 60)
    print("Per-vantage headline numbers")
    print("=" * 60)
    for v, s in per_vantage.items():
        print(f"  {v:>8}: AAAA {s['has_aaaa_pct']:>5}%   "
              f"v6-reachable {s['v6_reachable_pct']:>5}%   "
              f"median v6 connect {s['median_v6_connect_ms']} ms")
    print()
    print("=" * 60)
    print(f"Consensus across {n_vantages} vantages "
          f"({summary['hosts_probed_from_all_vantages']} hosts complete)")
    print("=" * 60)
    print(f"  Hosts with AAAA anywhere:        {c['hosts_with_aaaa_anywhere']:>8,}")
    print(f"  Reachable from all vantages:     {c['reachable_everywhere']:>8,}")
    print(f"  Partially reachable:             {c['partially_reachable']:>8,}")
    print(f"  Broken v6 everywhere (v4 alive): {c['unreachable_everywhere_but_v4_alive']:>8,}")
    print(f"  Dead hosts (v4 dead too):        {c['unreachable_everywhere_and_v4_dead']:>8,}")
    print(f"  Geo-DNS divergent AAAA sets:     {c['geo_dns_divergent']:>8,}")
    if "baseline_delta" in summary:
        d = summary["baseline_delta"]
        print()
        print("=" * 60)
        print(f"Delta vs baseline (from vantage '{d['control_vantage']}', "
              f"{d['hosts_in_common']:,} hosts in common)")
        print("=" * 60)
        print(f"  Gained AAAA:            {d['gained_aaaa']:>8,}")
        print(f"  Lost AAAA:              {d['lost_aaaa']:>8,}")
        print(f"  Gained v6 reachability: {d['gained_v6_reachability']:>8,}")
        print(f"  Lost v6 reachability:   {d['lost_v6_reachability']:>8,}")
    print(f"\nMerged data: {args.output}\nSummary:     {args.summary}")


if __name__ == "__main__":
    main()
