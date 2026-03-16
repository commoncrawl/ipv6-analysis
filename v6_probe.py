#!/usr/bin/env python3
"""
ipv6_survey.py

Survey IPv6 support for the top N hosts in a Common Crawl webgraph release,
ranked by harmonic centrality.

Streams only as many bytes as needed from the (large) ranks file -- no full
download required.

Checks two things per host:
  1. Whether it has a AAAA DNS record
  2. Whether it actually serves traffic over IPv6 via curl -6

Usage:
  python3 ipv6_survey.py -n 1000
  python3 ipv6_survey.py -n 1000 --output results.json --chart chart.png

Requirements:
  pip install matplotlib   (optional, for the chart)
  dig and curl must be in PATH
"""

import argparse
import asyncio
import gzip
import json
import sys
import time
import urllib.request
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------

RANKS_URL = (
    "https://data.commoncrawl.org/projects/hyperlinkgraph"
    "/cc-main-2025-26-dec-jan-feb/host"
    "/cc-main-2025-26-dec-jan-feb-host-ranks.txt.gz"
)

# File columns (tab-separated):
#   0: harmonicc_pos   (rank, 1 = highest; file is sorted by this)
#   1: harmonicc_val
#   2: pr_pos
#   3: pr_val
#   4: host_rev        (reversed host, e.g. com.facebook.www)
#
# File is already sorted by harmonicc_pos, so we just take the first N rows.


def unreverse(host: str) -> str:
    """Convert com.facebook.www -> www.facebook.com."""
    return ".".join(reversed(host.split(".")))


def fetch_top_hosts(n: int) -> list[str]:
    """
    Stream the ranks file and return the top N hosts.
    Closes the connection once we have enough rows -- pulls only a small
    fraction of the 5 GiB file.
    """
    print(f"Streaming top {n} hosts from ranks file (no full download)...")
    print(f"  {RANKS_URL}")

    hosts: list[str] = []

    req = urllib.request.Request(RANKS_URL, headers={"User-Agent": "CCF-ipv6-survey/1.0"})
    with urllib.request.urlopen(req) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            for raw_line in gz:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                if line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 5:
                    continue
                host_rev = parts[4].strip()
                hosts.append(unreverse(host_rev))
                if len(hosts) >= n:
                    break  # connection closes here; server sends no more data

    if not hosts:
        sys.exit("ERROR: No hosts parsed from ranks file. Check the URL is still valid.")

    print(f"  Got {len(hosts)} hosts (top: {hosts[0]}, #{n}: {hosts[-1]})")
    return hosts


# ---------------------------------------------------------------------------
# DNS AAAA check
# ---------------------------------------------------------------------------

async def check_aaaa(host: str, sem: asyncio.Semaphore) -> list[str]:
    """Return list of AAAA addresses for host. Empty = no IPv6 in DNS."""
    async with sem:
        proc = await asyncio.create_subprocess_exec(
            "dig", "+short", "+time=3", "+tries=2", "AAAA", host,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        lines = stdout.decode(errors="replace").splitlines()
        return [l.strip() for l in lines if ":" in l.strip()]


# ---------------------------------------------------------------------------
# IPv6 reachability check via curl -6
# ---------------------------------------------------------------------------

async def check_curl6(
    host: str, sem: asyncio.Semaphore, timeout: int = 10
) -> tuple[bool, int]:
    """
    Attempt an HTTPS HEAD request to host over IPv6.
    Returns (reachable: bool, http_status_code: int).
    Any HTTP response (including 4xx/5xx) counts as reachable -- it means the
    IPv6 stack is working. Only connection failures and timeouts are negative.
    """
    async with sem:
        proc = await asyncio.create_subprocess_exec(
            "curl",
            "-6",
            "--max-time", str(timeout),
            "--head",
            "--silent",
            "--output", "/dev/null",
            "--write-out", "%{http_code}",
            "--location",
            "--max-redirs", "3",
            f"https://{host}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        code_str = stdout.decode().strip()
        try:
            code = int(code_str)
            return code > 0, code
        except ValueError:
            return False, 0


# ---------------------------------------------------------------------------
# Main survey loop
# ---------------------------------------------------------------------------

async def run_survey(
    hosts: list[str],
    dns_concurrency: int,
    curl_concurrency: int,
) -> list[dict]:
    results: list[dict] = []
    total = len(hosts)

    # Phase 1: DNS AAAA lookups
    print(f"\n[1/2] AAAA DNS lookups ({dns_concurrency} concurrent)...")
    dns_sem = asyncio.Semaphore(dns_concurrency)
    dns_tasks = [asyncio.create_task(check_aaaa(h, dns_sem)) for h in hosts]

    dns_results: dict[str, list[str]] = {}
    t0 = time.monotonic()
    for i, (host, task) in enumerate(zip(hosts, dns_tasks), 1):
        aaaa = await task
        dns_results[host] = aaaa
        if i % 100 == 0 or i == total:
            elapsed = time.monotonic() - t0
            pct_v6 = sum(1 for v in dns_results.values() if v) / len(dns_results) * 100
            rate = i / elapsed if elapsed > 0 else 0
            eta = (total - i) / rate if rate > 0 else 0
            print(
                f"  {i:>4}/{total}  {pct_v6:5.1f}% with AAAA so far  "
                f"[{elapsed:.0f}s elapsed, ~{eta:.0f}s remaining]"
            )

    aaaa_hosts = [h for h in hosts if dns_results[h]]
    print(
        f"  Done. {len(aaaa_hosts)}/{total} hosts have AAAA records "
        f"({len(aaaa_hosts)/total*100:.1f}%)"
    )

    # Phase 2: curl -6 reachability for AAAA-positive hosts
    print(f"\n[2/2] IPv6 reachability via curl -6 ({curl_concurrency} concurrent)...")
    curl_sem = asyncio.Semaphore(curl_concurrency)
    curl_tasks = [
        asyncio.create_task(check_curl6(h, curl_sem)) for h in aaaa_hosts
    ]

    curl_results: dict[str, tuple[bool, int]] = {}
    t0 = time.monotonic()
    for i, (host, task) in enumerate(zip(aaaa_hosts, curl_tasks), 1):
        reachable, code = await task
        curl_results[host] = (reachable, code)
        if i % 50 == 0 or i == len(aaaa_hosts):
            elapsed = time.monotonic() - t0
            pct_reach = sum(1 for v in curl_results.values() if v[0]) / len(curl_results) * 100
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(aaaa_hosts) - i) / rate if rate > 0 else 0
            print(
                f"  {i:>4}/{len(aaaa_hosts)}  {pct_reach:5.1f}% reachable so far  "
                f"[{elapsed:.0f}s elapsed, ~{eta:.0f}s remaining]"
            )

    reachable_count = sum(1 for v in curl_results.values() if v[0])
    print(
        f"  Done. {reachable_count}/{len(aaaa_hosts)} AAAA hosts reachable over IPv6 "
        f"({reachable_count/len(aaaa_hosts)*100:.1f}% of those with AAAA, "
        f"{reachable_count/total*100:.1f}% of all)"
    )

    # Assemble results
    for rank, host in enumerate(hosts, 1):
        aaaa = dns_results.get(host, [])
        reachable, http_code = curl_results.get(host, (False, 0))
        results.append({
            "rank": rank,
            "host": host,
            "has_aaaa": bool(aaaa),
            "aaaa_records": aaaa,
            "ipv6_reachable": reachable,
            "http_code": http_code if reachable else None,
        })

    return results


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------

def make_chart(results: list[dict], output_path: str) -> None:
    if not HAS_MATPLOTLIB:
        print("\nNote: matplotlib not installed, skipping chart. pip install matplotlib")
        return

    total = len(results)
    n_aaaa = sum(1 for r in results if r["has_aaaa"])
    n_reachable = sum(1 for r in results if r["ipv6_reachable"])
    n_aaaa_only = n_aaaa - n_reachable
    n_none = total - n_aaaa

    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    fig.suptitle(
        f"IPv6 Support: Top {total} Hosts by Harmonic Centrality\n"
        f"Common Crawl cc-main-2025-26-dec-jan-feb",
        fontsize=13,
        fontweight="bold",
        y=1.01,
    )

    # Bar chart
    ax = axes[0]
    labels = ["No AAAA record", "AAAA only\n(not reachable)", "AAAA +\nreachable over IPv6"]
    values = [n_none, n_aaaa_only, n_reachable]
    colors = ["#c0392b", "#e67e22", "#27ae60"]
    bars = ax.bar(labels, values, color=colors, width=0.55, edgecolor="white", linewidth=0.8)
    ax.set_ylabel("Number of hosts")
    ax.set_title("Breakdown")
    ax.spines[["top", "right"]].set_visible(False)
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + total * 0.005,
            f"{val:,}\n({val/total*100:.1f}%)",
            ha="center",
            va="bottom",
            fontsize=10,
        )
    ax.set_ylim(0, max(values) * 1.18)

    # Pie chart
    ax2 = axes[1]
    wedge_labels = [
        f"No IPv6\n{n_none:,} ({n_none/total*100:.0f}%)",
        f"AAAA, not reachable\n{n_aaaa_only:,} ({n_aaaa_only/total*100:.0f}%)",
        f"Full IPv6\n{n_reachable:,} ({n_reachable/total*100:.0f}%)",
    ]
    ax2.pie(
        values,
        labels=wedge_labels,
        colors=colors,
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
        textprops={"fontsize": 9.5},
    )
    ax2.set_title("Distribution")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {output_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Survey IPv6 support in top Common Crawl hosts by harmonic centrality",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-n",
        type=int,
        default=1000,
        help="Number of top hosts to survey",
    )
    parser.add_argument(
        "--dns-concurrency",
        type=int,
        default=100,
        help="Max concurrent DNS lookups",
    )
    parser.add_argument(
        "--curl-concurrency",
        type=int,
        default=50,
        help="Max concurrent curl -6 checks",
    )
    parser.add_argument(
        "--output",
        default="ipv6-results.json",
        help="Output file for raw results (JSON)",
    )
    parser.add_argument(
        "--chart",
        default="ipv6-results.png",
        help="Output file for chart image",
    )
    args = parser.parse_args()

    print("CC IPv6 Survey -- cc-main-2025-26-dec-jan-feb")
    print(f"  Top N:            {args.n}")
    print(f"  DNS concurrency:  {args.dns_concurrency}")
    print(f"  curl concurrency: {args.curl_concurrency}")
    print()

    hosts = fetch_top_hosts(args.n)

    t_start = time.monotonic()
    results = asyncio.run(run_survey(hosts, args.dns_concurrency, args.curl_concurrency))
    total_elapsed = time.monotonic() - t_start

    # Save JSON
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    # Summary
    total = len(results)
    n_aaaa = sum(1 for r in results if r["has_aaaa"])
    n_reachable = sum(1 for r in results if r["ipv6_reachable"])
    print(f"\n{'='*52}")
    print(f"Results ({total_elapsed:.0f}s total)")
    print(f"{'='*52}")
    print(f"  Total hosts surveyed:   {total:>6,}")
    print(f"  Have AAAA record:       {n_aaaa:>6,}  ({n_aaaa/total*100:.1f}%)")
    print(f"  Reachable over IPv6:    {n_reachable:>6,}  ({n_reachable/total*100:.1f}%)")
    print(f"  AAAA but unreachable:   {n_aaaa-n_reachable:>6,}  ({(n_aaaa-n_reachable)/total*100:.1f}%)")
    print(f"\nRaw results: {args.output}")

    make_chart(results, args.chart)


if __name__ == "__main__":
    main()
