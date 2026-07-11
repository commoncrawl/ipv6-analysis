#!/usr/bin/env python3
"""
v6probe_mv.py

Multi-vantage successor to v6_probe.py. Differences from v1:

  * Reads a FROZEN host list (rank<TAB>host TSV) produced centrally by
    make_hostlist.py, instead of streaming the ranks file per node, so all
    vantage points probe byte-identical inputs.
  * Tags every record with vantage metadata (name, timestamp) and writes a
    sidecar meta file (resolvers, source IPv6 address, timings, arguments).
  * Captures curl connection timing (connect / TLS / total, in ms).
  * Retries failed IPv6 probes once (default) so transient blips don't
    masquerade as regional breakage when comparing across vantages.
  * Optionally probes IPv4 for hosts that have AAAA records, to (a) separate
    "broken IPv6" from "host is dead entirely" and (b) enable v4 vs v6
    latency comparison.
  * Writes results incrementally as JSONL and is resumable with --resume,
    which matters for 1M-host runs.
  * No charting; analysis lives in merge_mv.py.

Usage (per node):

  python3 v6probe_mv.py --hosts hosts-100k.tsv --vantage nyc3 \
      --output results-100k-nyc3.jsonl

Requirements: dig and curl in PATH. No third-party Python packages.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_VERSION = "2.0"

CURL_WRITEOUT = "%{http_code} %{time_namelookup} %{time_connect} %{time_appconnect} %{time_total}"


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Vantage self-description (recorded in the meta sidecar)
# ---------------------------------------------------------------------------

def get_resolvers() -> list[str]:
    resolvers = []
    for path in ("/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"):
        try:
            with open(path) as f:
                for line in f:
                    m = re.match(r"\s*nameserver\s+(\S+)", line)
                    if m:
                        resolvers.append(m.group(1))
            if resolvers:
                return resolvers
        except OSError:
            continue
    return resolvers


def get_source_ipv6() -> str | None:
    """Ask the kernel which source address it would use for v6 egress."""
    try:
        out = subprocess.run(
            ["ip", "-6", "route", "get", "2001:4860:4860::8888"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        m = re.search(r"\bsrc\s+(\S+)", out)
        return m.group(1) if m else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Probes
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
    return sorted(l.strip() for l in lines if ":" in l.strip())


async def curl_once(host: str, family: str, timeout: int) -> dict:
    """
    One HTTPS HEAD request over the given IP family ("-6" or "-4").
    Any HTTP response (including 4xx/5xx) counts as reachable; only
    connection failures and timeouts are negative.
    """
    proc = await asyncio.create_subprocess_exec(
        "curl", family,
        "--max-time", str(timeout),
        "--head", "--silent",
        "--output", "/dev/null",
        "--write-out", CURL_WRITEOUT,
        "--location", "--max-redirs", "3",
        f"https://{host}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    parts = stdout.decode(errors="replace").strip().split()
    try:
        code = int(parts[0])
    except (ValueError, IndexError):
        code = 0
    result = {"reachable": code > 0, "http_code": code if code > 0 else None}
    if code > 0 and len(parts) == 5:
        try:
            t_ns, t_conn, t_app, t_total = (float(x) for x in parts[1:5])
            result["connect_ms"] = round((t_conn - t_ns) * 1000, 1)
            result["appconnect_ms"] = round((t_app - t_ns) * 1000, 1) if t_app > 0 else None
            result["total_ms"] = round(t_total * 1000, 1)
        except ValueError:
            pass
    return result


async def check_curl(host: str, family: str, sem: asyncio.Semaphore,
                     timeout: int, retries: int, retry_delay: float) -> dict:
    async with sem:
        attempt = 0
        while True:
            attempt += 1
            result = await curl_once(host, family, timeout)
            if result["reachable"] or attempt > retries:
                result["attempts"] = attempt
                return result
            await asyncio.sleep(retry_delay)


# ---------------------------------------------------------------------------
# Worker pool
# ---------------------------------------------------------------------------

async def process_host(rank: int, host: str, ctx: dict) -> dict:
    args = ctx["args"]
    aaaa = await check_aaaa(host, ctx["dns_sem"])
    record = {
        "rank": rank,
        "host": host,
        "vantage": args.vantage,
        "ts": utcnow(),
        "has_aaaa": bool(aaaa),
        "aaaa_records": aaaa,
        "v6": None,
        "v4": None,
    }
    if aaaa:
        record["v6"] = await check_curl(
            host, "-6", ctx["curl_sem"], args.timeout, args.retries, args.retry_delay
        )
    want_v4 = args.probe_v4 == "all" or (args.probe_v4 == "aaaa" and aaaa)
    if want_v4:
        record["v4"] = await check_curl(
            host, "-4", ctx["curl_sem"], args.timeout, args.retries, args.retry_delay
        )
    return record


async def worker(queue: asyncio.Queue, ctx: dict) -> None:
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        rank, host = item
        try:
            record = await process_host(rank, host, ctx)
        except Exception as exc:  # keep the run alive; log and move on
            record = {
                "rank": rank, "host": host, "vantage": ctx["args"].vantage,
                "ts": utcnow(), "error": repr(exc),
                "has_aaaa": None, "aaaa_records": [], "v6": None, "v4": None,
            }
        async with ctx["write_lock"]:
            ctx["outfile"].write(json.dumps(record, separators=(",", ":")) + "\n")
            ctx["stats"]["done"] += 1
            if record.get("has_aaaa"):
                ctx["stats"]["aaaa"] += 1
            if record.get("v6") and record["v6"].get("reachable"):
                ctx["stats"]["reachable"] += 1
            done = ctx["stats"]["done"]
            if done % ctx["progress_every"] == 0 or done == ctx["stats"]["total"]:
                ctx["outfile"].flush()
                s = ctx["stats"]
                elapsed = time.monotonic() - ctx["t0"]
                rate = done / elapsed if elapsed > 0 else 0
                eta = (s["total"] - done) / rate if rate > 0 else 0
                print(
                    f"  {done:>8}/{s['total']}  "
                    f"AAAA {s['aaaa']/done*100:5.1f}%  "
                    f"v6-reachable {s['reachable']/done*100:5.1f}%  "
                    f"[{elapsed:.0f}s elapsed, ~{eta:.0f}s remaining]",
                    flush=True,
                )
        queue.task_done()


async def run_survey(hosts: list[tuple[int, str]], args: argparse.Namespace,
                     outfile) -> None:
    n_workers = args.dns_concurrency
    ctx = {
        "args": args,
        "dns_sem": asyncio.Semaphore(args.dns_concurrency),
        "curl_sem": asyncio.Semaphore(args.curl_concurrency),
        "write_lock": asyncio.Lock(),
        "outfile": outfile,
        "stats": {"done": 0, "aaaa": 0, "reachable": 0, "total": len(hosts)},
        "t0": time.monotonic(),
        "progress_every": max(100, len(hosts) // 200),
    }
    queue: asyncio.Queue = asyncio.Queue(maxsize=n_workers * 2)
    workers = [asyncio.create_task(worker(queue, ctx)) for _ in range(n_workers)]
    for item in hosts:
        await queue.put(item)
    for _ in workers:
        await queue.put(None)
    await asyncio.gather(*workers)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def load_hostlist(path: str) -> list[tuple[int, str]]:
    hosts: list[tuple[int, str]] = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                hosts.append((int(parts[0]), parts[1].strip()))
            else:
                hosts.append((lineno, parts[0].strip()))
    return hosts


def load_done(path: Path) -> set[str]:
    done: set[str] = set()
    with open(path) as f:
        for line in f:
            try:
                done.add(json.loads(line)["host"])
            except (json.JSONDecodeError, KeyError):
                continue
    return done


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Multi-vantage IPv6 survey over a frozen host list",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--hosts", required=True,
                        help="Host list TSV (rank<TAB>host) from make_hostlist.py")
    parser.add_argument("--vantage", default=socket.gethostname().split(".")[0],
                        help="Vantage point name recorded in every result")
    parser.add_argument("--output", default=None,
                        help="Output JSONL (default: results-<vantage>.jsonl)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip hosts already present in the output file")
    parser.add_argument("--dns-concurrency", type=int, default=100,
                        help="Max concurrent DNS lookups (also worker count)")
    parser.add_argument("--curl-concurrency", type=int, default=50,
                        help="Max concurrent curl probes")
    parser.add_argument("--timeout", type=int, default=10,
                        help="curl --max-time in seconds")
    parser.add_argument("--retries", type=int, default=1,
                        help="Extra curl attempts after a failed probe")
    parser.add_argument("--retry-delay", type=float, default=2.0,
                        help="Seconds between curl attempts")
    parser.add_argument("--probe-v4", choices=["none", "aaaa", "all"], default="aaaa",
                        help="Also probe IPv4: never, for AAAA hosts, or for all hosts")
    args = parser.parse_args()

    if args.output is None:
        args.output = f"results-{args.vantage}.jsonl"
    out_path = Path(args.output)
    meta_path = out_path.with_suffix(out_path.suffix + ".meta.json")

    hosts = load_hostlist(args.hosts)
    total_in_list = len(hosts)
    if args.resume and out_path.exists():
        done = load_done(out_path)
        hosts = [(r, h) for r, h in hosts if h not in done]
        print(f"Resuming: {len(done)} hosts already done, {len(hosts)} remaining")
    elif out_path.exists():
        sys.exit(f"ERROR: {out_path} exists. Use --resume to continue, or remove it.")

    meta = {
        "script_version": SCRIPT_VERSION,
        "vantage": args.vantage,
        "hostname": socket.gethostname(),
        "source_ipv6": get_source_ipv6(),
        "resolvers": get_resolvers(),
        "hostlist": args.hosts,
        "hosts_in_list": total_in_list,
        "args": {k: v for k, v in vars(args).items()},
        "started": utcnow(),
        "finished": None,
    }
    if meta["source_ipv6"] is None:
        print("WARNING: no global IPv6 source address detected; "
              "v6 probes will all fail. Aborting.", file=sys.stderr)
        sys.exit(1)

    print(f"v6probe_mv {SCRIPT_VERSION}  vantage={args.vantage}")
    print(f"  hosts: {len(hosts)} (of {total_in_list} in list)")
    print(f"  source IPv6: {meta['source_ipv6']}")
    print(f"  resolvers: {', '.join(meta['resolvers']) or 'unknown'}")
    print(f"  concurrency: dns={args.dns_concurrency} curl={args.curl_concurrency}")
    print(f"  v4 probing: {args.probe_v4}\n")

    meta_path.write_text(json.dumps(meta, indent=2))

    t0 = time.monotonic()
    with open(out_path, "a") as outfile:
        asyncio.run(run_survey(hosts, args, outfile))
    meta["finished"] = utcnow()
    meta["elapsed_seconds"] = round(time.monotonic() - t0)
    meta_path.write_text(json.dumps(meta, indent=2))

    print(f"\nDone in {meta['elapsed_seconds']}s. Results: {out_path}  Meta: {meta_path}")


if __name__ == "__main__":
    main()
