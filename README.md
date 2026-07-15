# IPv6 Adoption in the Top 100,000 Web Hosts

A measurement study of IPv6 adoption among the 100,000 most-linked hosts on the web, using the [Common Crawl Web Graph](https://commoncrawl.org/web-graphs).

**Live report:** [commoncrawl.github.io/ipv6-analysis](https://commoncrawl.github.io/ipv6-analysis/)

## Key findings

- **36.9%** of the top 100,000 hosts are fully reachable over IPv6 (AAAA record + successful HTTPS probe).
- **1.2%** publish AAAA records but fail reachability checks — these hosts actively hurt dual-stack users via Happy Eyeballs fallback delays.
- **61.9%** have no AAAA record at all.
- Adoption correlates with rank: 71% of the top 100 hosts support IPv6, falling to 32% at ranks 50,001–100,000.

## Methodology

Hosts are ranked by harmonic centrality from the Common Crawl Web Graph release `cc-main-2025-26-dec-jan-feb`, built from three crawls spanning December 2025 through February 2026. Each host was assessed in two stages:

1. **DNS AAAA lookup** — `dig +short AAAA` to check for published IPv6 records.
2. **IPv6 reachability probe** — `curl -6 --head` over HTTPS with a 10-second timeout, following up to three redirects. Any HTTP response (including 4xx/5xx) counts as reachable.

Probes were issued from a host with native IPv6 connectivity in northwestern Santa Clara County, California. DNS lookups ran at up to 100 concurrent tasks; curl probes at up to 50.

## Files

- `docs/index.html` — The full interactive report with charts, tables, and methodology details. Served by GitHub Pages.
- `docs/ipv6-results-100k.json` — Raw per-host results (rank, hostname, AAAA records, reachability status, HTTP response code).
- `v6_probe.py` — The probe script used to collect the data.

## Data format

Each entry in `docs/ipv6-results-100k.json` looks like:

```json
{
  "rank": 1,
  "host": "www.facebook.com",
  "has_aaaa": true,
  "aaaa_records": ["2a03:2880:f36c:1:face:b00c:0:25de"],
  "ipv6_reachable": true,
  "http_code": 200
}
```

## Author

[Common Crawl Foundation](https://commoncrawl.org)

Measured 16 March 2026.

## License

This project uses a dual-license model:

- **Data** (`ipv6-results-100k.json`) is released under [CC0 1.0 Universal (Public Domain Dedication)](DATA-LICENSE).
- **Code** (`index.html` and everything else) is released under the [Apache License 2.0](LICENSE).
