#!/usr/bin/env python3
"""Compare two merged multi-vantage runs over their common hosts,
restricted to a rank window, as internal validation of methodology
differences between the runs (e.g. temporal skew).

Inputs are merged per-host JSONL files from merge_mv.py (v2.1+), schema:
  rank, host, vantage_count, per_vantage{<name>{has_aaaa, aaaa_records,
  v6_reachable, v6_http_code, v6_connect_ms, v6_attempts, v4_reachable,
  v4_connect_ms}}, aaaa_vantages, v6_reachable_vantages,
  aaaa_sets_distinct (present only when all vantages saw AAAA)

Usage:
  python3 overlap_check.py RUN1.jsonl RUN2.jsonl \
      [--max-rank 100000] [--labels "run A,run B"] [--note "text"]

Notes:
  * Hosts are matched by hostname, not rank, so the runs may use
    different (overlapping) host lists; the comparison covers the
    intersection only, and its size is reported.
  * Geo-DNS divergence = aaaa_sets_distinct > 1, matching the merge.
  * Run-specific caveats belong in --note or the surrounding write-up,
    not in this tool.
"""

import argparse
import json
import sys

REQUIRED = ("rank", "host", "per_vantage", "aaaa_vantages",
            "v6_reachable_vantages")

# NOTE: aaaa_sets_distinct is only emitted by merge_mv.py when ALL
# vantages saw AAAA for the host; geo-DNS divergence is therefore
# defined over that subset only. Records without the field are treated
# as not counted for divergence, mirroring the merge's semantics.


def load(path, max_rank=None):
    """Stream merged JSONL into {host: slim record}."""
    hosts = {}
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            for k in REQUIRED:
                if k not in r:
                    sys.exit(f"{path}:{lineno}: missing field '{k}'; "
                             f"present: {sorted(r)}")
            if max_rank is not None and r["rank"] > max_rank:
                continue
            hosts[r["host"]] = {
                "aaaa_vantages": r["aaaa_vantages"],
                "v6_reachable_vantages": r["v6_reachable_vantages"],
                "aaaa_sets_distinct": r.get("aaaa_sets_distinct", 0),
                "v4_any": any(v.get("v4_reachable") is True
                              for v in r["per_vantage"].values()),
            }
    return hosts


def classify(r):
    """Consensus category for one slim record."""
    if r["aaaa_vantages"] == 0:
        return "no_aaaa"
    if r["v6_reachable_vantages"] == 0:
        return "broken_everywhere" if r["v4_any"] else "dead"
    if r["v6_reachable_vantages"] < r["aaaa_vantages"]:
        return "partial"
    return "reachable_all"


def summarise(hosts, keys):
    counts = {"aaaa_anywhere": 0, "reachable_all": 0, "partial": 0,
              "broken_everywhere": 0, "dead": 0, "geo_divergent": 0}
    for h in keys:
        r = hosts[h]
        c = classify(r)
        if c == "no_aaaa":
            continue
        counts["aaaa_anywhere"] += 1
        counts[c] += 1
        if r["aaaa_sets_distinct"] > 1:
            counts["geo_divergent"] += 1
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run1", help="First merged JSONL")
    ap.add_argument("run2", help="Second merged JSONL")
    ap.add_argument("--max-rank", type=int, default=100000,
                    help="Restrict both runs to ranks <= this "
                         "(default 100000)")
    ap.add_argument("--labels", default="run 1,run 2",
                    help="Comma-separated column labels")
    ap.add_argument("--note", default=None,
                    help="Free-text caveat to include in the output")
    args = ap.parse_args()

    l1, _, l2 = args.labels.partition(",")
    l1, l2 = l1.strip() or "run 1", l2.strip() or "run 2"

    print(f"Loading {l1}: {args.run1}", file=sys.stderr)
    a = load(args.run1, max_rank=args.max_rank)
    print(f"Loading {l2}: {args.run2}", file=sys.stderr)
    b = load(args.run2, max_rank=args.max_rank)

    common = set(a) & set(b)
    print("=" * 64)
    print(f"Overlap check: {l1} vs {l2} (rank <= {args.max_rank:,})")
    print("=" * 64)
    print(f"  {l1} hosts:    {len(a):>9,}")
    print(f"  {l2} hosts:    {len(b):>9,}")
    pct = 100.0 * len(common) / min(len(a), len(b)) if a and b else 0.0
    print(f"  Intersection: {len(common):>9,}  ({pct:.1f}% of smaller run)")
    print()
    print("  Hosts matched by hostname; comparison covers the")
    print("  intersection only.")
    if args.note:
        print(f"  NOTE: {args.note}")
    print()

    ca = summarise(a, common)
    cb = summarise(b, common)

    hdr = f"  {'category':<28}{l1:>14}{l2:>14}{'delta':>10}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for k, label in [
        ("aaaa_anywhere", "AAAA anywhere"),
        ("reachable_all", "reachable from all"),
        ("partial", "partially reachable"),
        ("broken_everywhere", "broken v6 everywhere"),
        ("dead", "dead (v4 too)"),
        ("geo_divergent", "geo-DNS divergent"),
    ]:
        va, vb = ca[k], cb[k]
        print(f"  {label:<28}{va:>14,}{vb:>14,}{vb - va:>+10,}")


if __name__ == "__main__":
    main()
