#!/usr/bin/env python3
"""
make_hostlist.py

Freeze a host list ONCE, centrally, so every vantage point probes a
byte-identical input. Output is TSV: rank<TAB>host, one per line.

Two modes:

  1. From the published March 2026 results (Run A, top 100k, ranks match
     the original study exactly):

       python3 make_hostlist.py --from-cc-json docs/ipv6-results-100k.json \
           > hosts-100k.tsv

     --from-cc-json also accepts a URL, e.g. the GitHub Pages copy.

  2. From a web graph ranks file (Run B, e.g. top 1M of the newest release):

       python3 make_hostlist.py --from-ranks -n 1000000 > hosts-1m.tsv

     Streams only as many bytes as needed; the connection is closed once
     N rows have been read.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import urllib.request

DEFAULT_RANKS_URL = (
    "https://data.commoncrawl.org/projects/hyperlinkgraph"
    "/cc-main-2026-apr-may-jun/host"
    "/cc-main-2026-apr-may-jun-host-ranks.txt.gz"
)


def unreverse(host: str) -> str:
    """Convert com.facebook.www -> www.facebook.com."""
    return ".".join(reversed(host.split(".")))


def from_cc_json(src: str) -> None:
    if src.startswith(("http://", "https://")):
        req = urllib.request.Request(src, headers={"User-Agent": "ipv6-mv-hostlist/1.0"})
        with urllib.request.urlopen(req) as resp:
            data = json.load(resp)
    else:
        with open(src) as f:
            data = json.load(f)
    data.sort(key=lambda r: r["rank"])
    for rec in data:
        print(f"{rec['rank']}\t{rec['host']}")
    print(f"Wrote {len(data)} hosts from {src}", file=sys.stderr)


def from_ranks(url: str, n: int) -> None:
    print(f"Streaming top {n} hosts from {url}", file=sys.stderr)
    req = urllib.request.Request(url, headers={"User-Agent": "ipv6-mv-hostlist/1.0"})
    count = 0
    with urllib.request.urlopen(req) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            for raw_line in gz:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                if line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 5:
                    continue
                count += 1
                print(f"{count}\t{unreverse(parts[4].strip())}")
                if count >= n:
                    break  # connection closes; no more data pulled
    if count == 0:
        sys.exit("ERROR: no hosts parsed from ranks file. Check the URL.")
    print(f"Wrote {count} hosts", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--from-cc-json", metavar="PATH_OR_URL",
                      help="Derive list from a published ipv6-results JSON")
    mode.add_argument("--from-ranks", action="store_true",
                      help="Stream top N from a web graph ranks file")
    p.add_argument("--ranks-url", default=DEFAULT_RANKS_URL,
                   help="Ranks file URL (default: cc-main-2026-apr-may-jun host ranks)")
    p.add_argument("-n", type=int, default=1_000_000,
                   help="Number of hosts to take in --from-ranks mode (default 1M)")
    args = p.parse_args()

    if args.from_cc_json:
        from_cc_json(args.from_cc_json)
    else:
        from_ranks(args.ranks_url, args.n)


if __name__ == "__main__":
    main()
