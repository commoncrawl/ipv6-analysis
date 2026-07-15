# Multi-vantage IPv6 survey: dataset documentation

Work in progress, July 2026. Extends the March 2026 single-vantage
IPv6 study in this repository from one vantage point to five.
Presented at the MAPRG plenary, IETF 126 Vienna.

## Vantage points

| Label     | Location                          |
|-----------|-----------------------------------|
| nyc3      | New York, DigitalOcean            |
| sfo3      | San Francisco, DigitalOcean       |
| ams3      | Amsterdam, DigitalOcean           |
| sgp1      | Singapore, DigitalOcean           |
| ca-tunnel | California, Hurricane Electric 6in4 tunnel |

The ca-tunnel vantage uses tunnelled rather than native IPv6, and its
DNS resolution went through Google Public DNS rather than a local
resolver; per-vantage AAAA comparisons involving ca-tunnel should
bear this in mind.

## Runs

* **Run A** (top 100k hosts): two probe passes per vantage, except
  ams3, which entered the merge with a single pass. Host list derived
  from the March 2026 Common Crawl ranking.
* **Run B** (top 1M hosts): two probe passes per vantage, all five
  complete. Host list frozen from the cc-main-2026-apr-may-jun
  ranking; see hosts-1m.tsv below.

The two runs use different host lists (March list vs Apr-Jun ranks);
overlap comparisons are restricted to hosts present in both.

## Files

* `merged-100k.jsonl` - run A, one record per host (100,000 records)
* `merged-1m.jsonl` - run B, one record per host (1,000,000 records)
* `summary-100k.json`, `summary-1m.json` - aggregate statistics
* `hosts-1m.tsv` - the frozen run B probe list.
  md5: `f913bab8793d18e61c253188b8d4d9af`. October 2026 follow-up
  runs will re-probe this exact list; the hash makes the "same
  frozen list" claim externally verifiable.
* `overlap-check.txt` - run A vs run B comparison at rank <= 100,000
* `merge-report-1m.txt` - run B per-vantage headline numbers and
  observation windows
* `transient-flip-reports.txt` - run B pass-to-pass flip statistics
  per vantage

Raw per-vantage result files are held back for now and may be
published later.

## Record schema (merged JSONL)

Top-level fields, one JSON object per line:

* `rank` - position in the run's host list
* `host` - target hostname
* `vantage_count` - vantages with a complete observation
* `aaaa_vantages` - vantages that observed at least one AAAA record
* `aaaa_sets_distinct` - number of distinct AAAA record sets observed
  across vantages
* `v6_reachable_vantages` - vantages with a successful IPv6 connect
* `max_obs_gap_s` - largest inter-vantage observation gap in seconds
  (run B only; run A records do not carry this field)
* `per_vantage` - map of vantage label to observation:
  `has_aaaa`, `aaaa_records`, `v6_reachable`, `v6_http_code`,
  `v6_connect_ms`, `v6_attempts`, `v4_reachable`, `v4_connect_ms`

## Caveats

* **Temporal skew.** Vantages did not observe hosts simultaneously.
  Run B observation windows ranged from 2.4 h (ca-tunnel) to 30.4 h
  (nyc3); the median maximum inter-vantage gap is 13.3 h (p99
  27.68 h). Timestamps reflect pass 2 of the combined passes.
  Apparent inter-vantage disagreement can be temporal rather than
  topological. TODO: skew framing including the
  lower-gap-for-partial finding.
* **Geo-DNS divergence.** TODO: exact definition of
  "divergent AAAA sets" as used in the summaries, and its caveat.
* **Run A ams3 single-pass.** Transient-flip filtering could not be
  applied to ams3 in run A; treat its run A per-vantage numbers
  accordingly.
* **Sanitisation.** Vantage machine identifiers were replaced with
  the public labels above throughout these files. No addresses or
  measurement values were altered.

## Reproducing the aggregate numbers

TODO: pointer to the analysis tooling in multivantage/ and
invocation notes.
