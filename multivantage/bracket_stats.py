#!/usr/bin/env python3
"""Compute rank-bracket adoption statistics from a merged multi-vantage JSONL file.

Extends the March report's brackets to 1M. For each bracket, counts:
  - total hosts
  - AAAA anywhere (aaaa_vantages >= 1)
  - reachable from all vantages (v6_reachable_vantages == vantage_count)
  - partially reachable (1 <= v6_reachable_vantages < vantage_count)

Usage:
    python3 bracket_stats.py data/publish/merged-1m.jsonl > brackets.json
"""

import json
import sys

BRACKETS = [
    (1, 100, "1–100"),
    (101, 500, "101–500"),
    (501, 1000, "501–1,000"),
    (1001, 2500, "1,001–2,500"),
    (2501, 5000, "2,501–5,000"),
    (5001, 10000, "5,001–10,000"),
    (10001, 25000, "10,001–25,000"),
    (25001, 50000, "25,001–50,000"),
    (50001, 100000, "50,001–100,000"),
    (100001, 250000, "100,001–250,000"),
    (250001, 500000, "250,001–500,000"),
    (500001, 1000000, "500,001–1,000,000"),
]


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <merged.jsonl>")

    stats = [
        {"label": lab, "lo": lo, "hi": hi, "total": 0,
         "aaaa": 0, "reachable_all": 0, "partial": 0}
        for lo, hi, lab in BRACKETS
    ]

    with open(sys.argv[1]) as f:
        for line in f:
            rec = json.loads(line)
            rank = rec["rank"]
            for s in stats:
                if s["lo"] <= rank <= s["hi"]:
                    s["total"] += 1
                    nv = rec["vantage_count"]
                    if rec["aaaa_vantages"] >= 1:
                        s["aaaa"] += 1
                    r = rec["v6_reachable_vantages"]
                    if r == nv:
                        s["reachable_all"] += 1
                    elif r >= 1:
                        s["partial"] += 1
                    break

    for s in stats:
        s["pct"] = round(100.0 * s["reachable_all"] / s["total"], 1) if s["total"] else 0.0
        s["aaaa_pct"] = round(100.0 * s["aaaa"] / s["total"], 1) if s["total"] else 0.0
        del s["lo"], s["hi"]

    json.dump(stats, sys.stdout, ensure_ascii=True)
    print()


if __name__ == "__main__":
    main()
