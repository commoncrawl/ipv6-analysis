#!/usr/bin/env python3
"""
combine_passes.py

Combine two probe passes for ONE vantage into a single JSONL that
merge_mv.py consumes unchanged. Semantics: a host is reachable if EITHER
pass reached it, so "unreachable" in the combined file means "failed in
both passes, hours apart". This filters transient failures out of the
cross-vantage comparison, where they otherwise masquerade as regional
breakage.

Field rules per host:
  * has_aaaa:      OR of the passes (DNS transients also filtered)
  * aaaa_records:  pass 2's non-empty set preferred, else pass 1's
                   (records from both are kept in aaaa_records_p1/p2)
  * v6 / v4:       reachable = OR; details (http_code, timing, attempts)
                   taken from a successful pass, preferring pass 2;
                   per-pass reachability kept as reachable_p1/reachable_p2
  * hosts present in only one pass are passed through with passes=1

Usage (per vantage):

  python3 combine_passes.py pass1/results-100k-nyc3.jsonl \
      pass2/results-100k-nyc3.jsonl -o combined/results-100k-nyc3.jsonl

Prints a transient report: how many hosts flipped between passes.
"""

from __future__ import annotations

import argparse
import json
import sys


def load(path: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    vantages = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            out[rec["host"]] = rec
            vantages.add(rec.get("vantage"))
    if len(vantages) > 1:
        sys.exit(f"ERROR: {path} contains multiple vantages {vantages}; "
                 "combine_passes.py works on one vantage at a time.")
    return out


def fam_reachable(rec: dict, fam: str) -> bool:
    d = rec.get(fam)
    return bool(d and d.get("reachable"))


def combine_family(r1: dict, r2: dict, fam: str) -> dict | None:
    d1, d2 = r1.get(fam), r2.get(fam)
    if d1 is None and d2 is None:
        return None
    ok1, ok2 = fam_reachable(r1, fam), fam_reachable(r2, fam)
    # detail source: successful pass, preferring pass 2; else pass 2, else pass 1
    if ok2:
        base = dict(d2)
    elif ok1:
        base = dict(d1)
    else:
        base = dict(d2 or d1)
    base["reachable"] = ok1 or ok2
    base["reachable_p1"] = ok1 if d1 is not None else None
    base["reachable_p2"] = ok2 if d2 is not None else None
    return base


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("pass1")
    p.add_argument("pass2")
    p.add_argument("-o", "--output", required=True)
    args = p.parse_args()

    p1, p2 = load(args.pass1), load(args.pass2)
    hosts = set(p1) | set(p2)

    stats = {
        "both_passes": 0, "one_pass_only": 0,
        "v6_flip_up": 0,       # unreachable p1 -> reachable p2
        "v6_flip_down": 0,     # reachable p1 -> unreachable p2
        "v6_rescued": 0,       # unreachable in exactly one pass (either way)
        "v6_failed_both": 0,
        "aaaa_flips": 0,
    }

    with open(args.output, "w") as out:
        for host in hosts:
            r1, r2 = p1.get(host), p2.get(host)
            if r1 is None or r2 is None:
                rec = dict(r2 or r1)
                rec["passes"] = 1
                stats["one_pass_only"] += 1
                out.write(json.dumps(rec, separators=(",", ":")) + "\n")
                continue

            stats["both_passes"] += 1
            has1, has2 = bool(r1.get("has_aaaa")), bool(r2.get("has_aaaa"))
            if has1 != has2:
                stats["aaaa_flips"] += 1
            ok1, ok2 = fam_reachable(r1, "v6"), fam_reachable(r2, "v6")
            if (has1 or has2):
                if not ok1 and ok2:
                    stats["v6_flip_up"] += 1
                elif ok1 and not ok2:
                    stats["v6_flip_down"] += 1
                if ok1 != ok2:
                    stats["v6_rescued"] += 1
                elif not ok1 and not ok2:
                    stats["v6_failed_both"] += 1

            rec = {
                "rank": r2.get("rank", r1.get("rank")),
                "host": host,
                "vantage": r2.get("vantage", r1.get("vantage")),
                "ts": r2.get("ts"),
                "ts_p1": r1.get("ts"),
                "passes": 2,
                "has_aaaa": has1 or has2,
                "aaaa_records": r2.get("aaaa_records") or r1.get("aaaa_records") or [],
                "aaaa_records_p1": r1.get("aaaa_records") or [],
                "aaaa_records_p2": r2.get("aaaa_records") or [],
                "v6": combine_family(r1, r2, "v6"),
                "v4": combine_family(r1, r2, "v4"),
            }
            if r1.get("error") or r2.get("error"):
                rec["error"] = r2.get("error") or r1.get("error")
            out.write(json.dumps(rec, separators=(",", ":")) + "\n")

    print(f"{args.output}:")
    print(f"  hosts in both passes:        {stats['both_passes']:>8,}")
    print(f"  hosts in one pass only:      {stats['one_pass_only']:>8,}")
    print(f"  AAAA presence flips:         {stats['aaaa_flips']:>8,}")
    print(f"  v6 transient (flipped once): {stats['v6_rescued']:>8,}"
          f"  (up {stats['v6_flip_up']:,} / down {stats['v6_flip_down']:,})")
    print(f"  v6 failed both passes:       {stats['v6_failed_both']:>8,}")


if __name__ == "__main__":
    main()
