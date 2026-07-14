#!/usr/bin/env python3
"""
merge_mv.py  (v2.2, memory-slim)

Merge per-vantage JSONL results from v6probe_mv.py (or combine_passes.py)
into a single per-host dataset, compute multi-vantage statistics, and
optionally compare against a published single-vantage baseline.

v2.2 changes vs v2.1:
  * Temporal-skew quantification:
      - per-vantage observation windows (start/end/duration) from the
        per-record "ts" field;
      - per-host max inter-vantage observation gap, emitted per host
        (max_obs_gap_s) and summarised as distributions for all complete
        hosts, the partially-reachable class, and the geo-DNS-divergent
        class.
    CAVEAT: when input files come from combine_passes.py, each record
    carries a single "ts" whose semantics depend on how combine_passes.py
    merges the two passes. Verify which pass's timestamp survives before
    citing the gap distribution, and caveat accordingly.

v2.1 changes vs v2.0:
  * Loads slim in-memory records (only the fields the analysis needs),
    with string interning, so 1M-host / 5-vantage merges fit in a few GB
    instead of tens.
  * Streams the merged per-host output as JSONL rather than building one
    giant JSON array in memory. Name the --output accordingly (.jsonl).
  * Accepts combined two-pass files from combine_passes.py transparently.

Usage:

  python3 merge_mv.py results-*.jsonl \
      --baseline docs/ipv6-results-100k.json \
      --control-vantage rf \
      --output merged.jsonl --summary summary.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations


class Slim:
    """Per-vantage per-host record, reduced to what the analysis needs."""
    __slots__ = ("rank", "has_aaaa", "aaaa", "v6", "v4", "error", "ts")

    # v6: None or (reachable, http_code, connect_ms, attempts)
    # v4: None or (reachable, connect_ms)
    # ts: None or epoch seconds (float)


def slim_record(rec: dict) -> Slim:
    s = Slim()
    s.rank = rec.get("rank", 0)
    s.has_aaaa = rec.get("has_aaaa")
    s.aaaa = tuple(sys.intern(a) for a in (rec.get("aaaa_records") or ()))
    v6 = rec.get("v6")
    s.v6 = None if v6 is None else (
        bool(v6.get("reachable")), v6.get("http_code"),
        v6.get("connect_ms"), v6.get("attempts"),
    )
    v4 = rec.get("v4")
    s.v4 = None if v4 is None else (
        bool(v4.get("reachable")), v4.get("connect_ms"),
    )
    s.error = rec.get("error")
    ts = rec.get("ts")
    try:
        s.ts = (datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                if ts else None)
    except (ValueError, AttributeError):
        s.ts = None
    return s


def v6_ok(s: "Slim | None") -> bool:
    return bool(s and s.v6 and s.v6[0])


def v4_ok(s: "Slim | None") -> bool:
    return bool(s and s.v4 and s.v4[0])


def median(values: list) -> "float | None":
    if not values:
        return None
    v = sorted(values)
    mid = len(v) // 2
    return round(v[mid] if len(v) % 2 else (v[mid - 1] + v[mid]) / 2, 1)


def gap_stats(gaps: list) -> "dict | None":
    """Distribution of inter-vantage observation gaps, reported in hours."""
    if not gaps:
        return None
    g = sorted(gaps)

    def q(p: float) -> float:
        return round(g[min(len(g) - 1, int(p * len(g)))] / 3600, 2)

    return {
        "hosts": len(g),
        "median_h": q(0.5),
        "p90_h": q(0.9),
        "p99_h": q(0.99),
        "max_h": round(g[-1] / 3600, 2),
    }


def iso_utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def load_results(paths: list) -> tuple:
    by_host: dict = defaultdict(dict)
    vantages: list = []
    for path in paths:
        n = 0
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                v = sys.intern(rec["vantage"])
                if v not in vantages:
                    vantages.append(v)
                by_host[sys.intern(rec["host"])][v] = slim_record(rec)
                n += 1
        print(f"  {path}: {n:,} records", file=sys.stderr)
    return by_host, vantages


def load_baseline(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    return {r["host"]: (r["rank"], bool(r.get("has_aaaa")),
                        bool(r.get("ipv6_reachable"))) for r in data}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("results", nargs="+", help="Per-vantage JSONL files")
    p.add_argument("--baseline", help="Published ipv6-results JSON for delta analysis")
    p.add_argument("--control-vantage", default="rf",
                   help="Vantage matching the baseline's location (default: rf)")
    p.add_argument("--output", default="merged.jsonl",
                   help="Merged per-host output (JSONL, streamed)")
    p.add_argument("--summary", default="summary.json", help="Summary statistics JSON")
    p.add_argument("--examples", type=int, default=15,
                   help="Example hosts to list per finding, lowest rank first")
    args = p.parse_args()

    by_host, vantages = load_results(args.results)
    n_vantages = len(vantages)
    print(f"Loaded {len(by_host):,} hosts across {n_vantages} vantages: "
          f"{', '.join(vantages)}\n")

    baseline = load_baseline(args.baseline) if args.baseline else {}

    summary: dict = {"vantages": vantages, "hosts": len(by_host)}

    # ------------------------------------------------------------------
    # Per-vantage headline numbers (incl. observation windows)
    # ------------------------------------------------------------------
    per_vantage: dict = {}
    for v in vantages:
        n = n_aaaa = n_reach = 0
        v6_ms: list = []
        v4_ms: list = []
        ts_min = ts_max = None
        for recs in by_host.values():
            s = recs.get(v)
            if s is None:
                continue
            n += 1
            if s.has_aaaa:
                n_aaaa += 1
            if v6_ok(s):
                n_reach += 1
                if s.v6[2] is not None:
                    v6_ms.append(s.v6[2])
            if v4_ok(s) and s.v4[1] is not None:
                v4_ms.append(s.v4[1])
            if s.ts is not None:
                if ts_min is None or s.ts < ts_min:
                    ts_min = s.ts
                if ts_max is None or s.ts > ts_max:
                    ts_max = s.ts
        per_vantage[v] = {
            "hosts_probed": n,
            "has_aaaa": n_aaaa,
            "has_aaaa_pct": round(n_aaaa / n * 100, 1) if n else None,
            "v6_reachable": n_reach,
            "v6_reachable_pct": round(n_reach / n * 100, 1) if n else None,
            "median_v6_connect_ms": median(v6_ms),
            "median_v4_connect_ms": median(v4_ms),
            "window_start_utc": iso_utc(ts_min) if ts_min is not None else None,
            "window_end_utc": iso_utc(ts_max) if ts_max is not None else None,
            "window_hours": (round((ts_max - ts_min) / 3600, 2)
                             if ts_min is not None else None),
        }
    summary["per_vantage"] = per_vantage

    # ------------------------------------------------------------------
    # Per-host merge (streamed to disk) + consensus classification
    # ------------------------------------------------------------------
    reach_dist: Counter = Counter()
    aaaa_presence_dist: Counter = Counter()
    n_complete = 0
    partial_examples: list = []
    broken_examples: list = []
    divergent_examples: list = []
    n_broken = n_dead = n_divergent = 0
    pairs = list(combinations(vantages, 2))
    pair_n: Counter = Counter()
    pair_d: Counter = Counter()
    lat_deltas: dict = {v: [] for v in vantages}
    gap_all: list = []
    gap_partial: list = []
    gap_divergent: list = []

    hosts_sorted = sorted(by_host.items(),
                          key=lambda kv: min(s.rank for s in kv[1].values()))

    with open(args.output, "w") as out:
        for host, recs in hosts_sorted:
            rank = min(s.rank for s in recs.values())
            entry = {
                "rank": rank,
                "host": host,
                "vantage_count": len(recs),
                "per_vantage": {
                    v: {
                        "has_aaaa": s.has_aaaa,
                        "aaaa_records": list(s.aaaa),
                        "v6_reachable": v6_ok(s),
                        "v6_http_code": s.v6[1] if s.v6 else None,
                        "v6_connect_ms": s.v6[2] if s.v6 else None,
                        "v6_attempts": s.v6[3] if s.v6 else None,
                        "v4_reachable": s.v4[0] if s.v4 else None,
                        "v4_connect_ms": s.v4[1] if s.v4 else None,
                        **({"error": s.error} if s.error else {}),
                    }
                    for v, s in recs.items()
                },
            }

            for v, s in recs.items():
                if (v6_ok(s) and v4_ok(s)
                        and s.v6[2] is not None and s.v4[1] is not None):
                    lat_deltas[v].append(s.v6[2] - s.v4[1])

            if len(recs) == n_vantages:
                n_complete += 1
                tss = [s.ts for s in recs.values() if s.ts is not None]
                gap = max(tss) - min(tss) if len(tss) == n_vantages else None
                if gap is not None:
                    gap_all.append(gap)
                    entry["max_obs_gap_s"] = round(gap)
                k_aaaa = sum(1 for s in recs.values() if s.has_aaaa)
                k_reach = sum(1 for s in recs.values() if v6_ok(s))
                aaaa_presence_dist[k_aaaa] += 1
                entry["aaaa_vantages"] = k_aaaa
                entry["v6_reachable_vantages"] = k_reach
                if k_aaaa > 0:
                    reach_dist[k_reach] += 1
                    if 0 < k_reach < n_vantages:
                        reach_from = tuple(v for v, s in recs.items() if v6_ok(s))
                        partial_examples.append((rank, host, reach_from))
                        if gap is not None:
                            gap_partial.append(gap)
                    elif k_reach == 0:
                        if any(v4_ok(s) for s in recs.values()):
                            n_broken += 1
                            broken_examples.append((rank, host))
                        else:
                            n_dead += 1
                    if k_aaaa == n_vantages:
                        sets = {s.aaaa for s in recs.values()}
                        entry["aaaa_sets_distinct"] = len(sets)
                        if len(sets) > 1:
                            n_divergent += 1
                            divergent_examples.append((rank, host, len(sets)))
                            if gap is not None:
                                gap_divergent.append(gap)
                    for va, vb in pairs:
                        sa, sb = recs[va], recs[vb]
                        if sa.has_aaaa or sb.has_aaaa:
                            pair_n[(va, vb)] += 1
                            if v6_ok(sa) != v6_ok(sb):
                                pair_d[(va, vb)] += 1

            out.write(json.dumps(entry, separators=(",", ":")) + "\n")

    summary["hosts_probed_from_all_vantages"] = n_complete
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
        "partially_reachable": sum(reach_dist.get(kk, 0) for kk in range(1, n_vantages)),
        "unreachable_everywhere_but_v4_alive": n_broken,
        "unreachable_everywhere_and_v4_dead": n_dead,
        "geo_dns_divergent": n_divergent,
    }

    summary["temporal_skew"] = {
        "note": ("Gaps computed from per-record ts across vantages, in hours. "
                 "If inputs came from combine_passes.py, check which pass's "
                 "ts survives combination before citing."),
        "max_inter_vantage_gap": {
            "all_complete_hosts": gap_stats(gap_all),
            "partially_reachable": gap_stats(gap_partial),
            "geo_dns_divergent": gap_stats(gap_divergent),
        },
    }

    k = args.examples
    partial_examples.sort()
    broken_examples.sort()
    divergent_examples.sort()
    summary["examples"] = {
        "partially_reachable": [
            {"rank": r, "host": h, "reachable_from": list(fr)}
            for r, h, fr in partial_examples[:k]
        ],
        "broken_v6_everywhere": [
            {"rank": r, "host": h} for r, h in broken_examples[:k]
        ],
        "geo_dns_divergent": [
            {"rank": r, "host": h, "distinct_aaaa_sets": n}
            for r, h, n in divergent_examples[:k]
        ],
    }

    summary["pairwise_reachability_disagreement"] = {
        f"{va}|{vb}": {
            "hosts": pair_n[(va, vb)],
            "disagree": pair_d[(va, vb)],
            "disagree_pct": round(pair_d[(va, vb)] / pair_n[(va, vb)] * 100, 2)
            if pair_n[(va, vb)] else None,
        }
        for va, vb in pairs
    }

    if baseline:
        cv = args.control_vantage if args.control_vantage in vantages else vantages[0]
        if cv != args.control_vantage:
            print(f"NOTE: control vantage '{args.control_vantage}' not found; "
                  f"using '{cv}' for the baseline delta.\n")
        gained_aaaa = lost_aaaa = 0
        gained_reach: list = []
        lost_reach: list = []
        n_common = 0
        for host, (b_rank, b_aaaa, b_reach) in baseline.items():
            s = by_host.get(host, {}).get(cv)
            if s is None:
                continue
            n_common += 1
            now_aaaa = bool(s.has_aaaa)
            now_reach = v6_ok(s)
            if now_aaaa and not b_aaaa:
                gained_aaaa += 1
            if b_aaaa and not now_aaaa:
                lost_aaaa += 1
            if now_reach and not b_reach:
                gained_reach.append((b_rank, host))
            if b_reach and not now_reach:
                lost_reach.append((b_rank, host))
        gained_reach.sort()
        lost_reach.sort()
        summary["baseline_delta"] = {
            "control_vantage": cv,
            "hosts_in_common": n_common,
            "gained_aaaa": gained_aaaa,
            "lost_aaaa": lost_aaaa,
            "gained_v6_reachability": len(gained_reach),
            "lost_v6_reachability": len(lost_reach),
            "examples": {
                "gained_v6_reachability": [
                    {"rank": r, "host": h} for r, h in gained_reach[:k]],
                "lost_v6_reachability": [
                    {"rank": r, "host": h} for r, h in lost_reach[:k]],
            },
        }

    summary["v6_vs_v4_latency"] = {
        v: {
            "dual_stack_hosts": len(d),
            "median_v6_minus_v4_connect_ms": median(d),
            "v6_faster_pct": round(sum(1 for x in d if x < 0) / len(d) * 100, 1)
            if d else None,
        }
        for v, d in lat_deltas.items()
    }

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
    print("Per-vantage observation windows")
    print("=" * 60)
    for v, s in per_vantage.items():
        print(f"  {v:>8}: {s['window_start_utc']} -> {s['window_end_utc']} "
              f"({s['window_hours']} h)")
    print()
    print("=" * 60)
    print(f"Consensus across {n_vantages} vantages "
          f"({n_complete:,} hosts complete)")
    print("=" * 60)
    print(f"  Hosts with AAAA anywhere:        {c['hosts_with_aaaa_anywhere']:>8,}")
    print(f"  Reachable from all vantages:     {c['reachable_everywhere']:>8,}")
    print(f"  Partially reachable:             {c['partially_reachable']:>8,}")
    print(f"  Broken v6 everywhere (v4 alive): {c['unreachable_everywhere_but_v4_alive']:>8,}")
    print(f"  Dead hosts (v4 dead too):        {c['unreachable_everywhere_and_v4_dead']:>8,}")
    print(f"  Geo-DNS divergent AAAA sets:     {c['geo_dns_divergent']:>8,}")
    ts_s = summary["temporal_skew"]["max_inter_vantage_gap"]
    print()
    print("=" * 60)
    print("Temporal skew: max inter-vantage observation gap (hours)")
    print("=" * 60)
    for label, key in (("All complete hosts", "all_complete_hosts"),
                       ("Partially reachable", "partially_reachable"),
                       ("Geo-DNS divergent", "geo_dns_divergent")):
        g = ts_s[key]
        if g is None:
            print(f"  {label:<22}: no timestamps available")
        else:
            print(f"  {label:<22}: median {g['median_h']}  p90 {g['p90_h']}  "
                  f"p99 {g['p99_h']}  max {g['max_h']}  (n={g['hosts']:,})")
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
