#!/usr/bin/env python3
"""Compute per-TLD adoption statistics from a merged multi-vantage JSONL file.

The TLD is the final DNS label of the hostname (so .co.uk hosts count
under "uk"). For each TLD: total hosts, AAAA anywhere, reachable from
all vantages. Output is the top N TLDs by host count, as JSON.

Usage:
    python3 tld_stats.py data/publish/merged-1m.jsonl [top_n] > tlds.json
"""

import json
import sys
from collections import defaultdict


def main() -> None:
    if len(sys.argv) not in (2, 3):
        sys.exit(f"usage: {sys.argv[0]} <merged.jsonl> [top_n]")
    top_n = int(sys.argv[2]) if len(sys.argv) == 3 else 20

    stats = defaultdict(lambda: [0, 0, 0])  # tld -> [total, aaaa, reach_all]
    with open(sys.argv[1]) as f:
        for line in f:
            rec = json.loads(line)
            tld = rec["host"].rstrip(".").rsplit(".", 1)[-1].lower()
            s = stats[tld]
            s[0] += 1
            if rec["aaaa_vantages"] >= 1:
                s[1] += 1
            if rec["v6_reachable_vantages"] == rec["vantage_count"]:
                s[2] += 1

    top = sorted(stats.items(), key=lambda kv: -kv[1][0])[:top_n]
    out = [
        {
            "tld": tld,
            "total": total,
            "aaaa": aaaa,
            "reachable_all": reach,
            "pct": round(100.0 * reach / total, 1),
            "aaaa_pct": round(100.0 * aaaa / total, 1),
        }
        for tld, (total, aaaa, reach) in top
    ]
    json.dump(out, sys.stdout, ensure_ascii=True)
    print()


if __name__ == "__main__":
    main()
