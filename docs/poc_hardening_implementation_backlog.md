# TiDB Self-Service PoV Hardening Backlog

## Purpose

This document converts the current PoV review into an implementation-ready backlog for Codex.

Primary goal:
- Make the self-service PoV broadly applicable for pre-sales discovery
- Make TiDB performance results more representative and more defensible
- Create a clean transition from self-service PoV to SE-supported performance PoV

Constraints:
- Do not modify the user's local `config.yaml`
- Treat this repo as customer-facing; avoid adding internal-only language
- Prefer small, reviewable changes with explicit acceptance criteria

## Current Review Summary

The PoV is already strong for:
- compatibility validation
- warm workload validation
- import testing
- HTAP demonstration
- artifact generation and S3 archival

The current gaps are:
1. Starter/Serverless onboarding still fails by default because sample TiDB usernames do not use the required prefix format
2. HA reporting drops the actual failure phase in the PDF path
3. The default synthetic OLTP schema does not use TiDB-friendly write-path defaults
4. The default Python load harness is too weak to be the primary path for serious performance claims
5. Comparison support is broader in configuration than in actual automated execution

## Delivery Strategy

Implement in this order:

### Phase 1: Must-fix before next customer-facing use
1. Fix TiDB Cloud username defaults and validation
2. Fix HA phase naming/reporting bug
3. Promote warm-state metrics in summary/report output
4. Clarify comparison execution support in UI/report/config docs

### Phase 2: Performance credibility improvements
5. Make the main synthetic OLTP schema TiDB-optimized by default
6. Introduce explicit run modes: validation mode vs performance mode
7. Make high-throughput workload generation the default for performance-focused runs

### Phase 3: Broader applicability enhancements
8. Add curated workload presets and dataset profiles
9. Add report language that distinguishes synthetic, customer-query, and high-throughput findings
10. Add DB-side observability capture to the report package

---

## Workstream 1: Fix TiDB Cloud Username Defaults And Validation

### Problem

The self-service path starts users on Starter/Serverless, but sample config blocks still use `root` instead of `<prefix>.root`. This causes first-run connectivity failure for the default path.

### Files to update

- `config.yaml.example`
- `README.md`
- `setup/00_provision.md`
- `setup/poc_web_ui.py`
- `setup/templates/poc_web_ui.html`
- any helper that validates connection settings before run

### Required changes

1. Replace any default sample TiDB Cloud username of `root` with `<prefix>.root`
2. Add a preflight validator:
   - if tier is Starter/Serverless/Essential/Premium and user does not contain a dot-prefixed account segment, warn clearly
   - do not rely on MySQL error text alone
3. Add help text near the TiDB username field:
   - explain that TiDB Cloud usernames often look like `<prefix>.root`
4. Ensure script-only mode prints the same actionable guidance

### Acceptance criteria

- A new user following the quick-start docs sees a valid username example
- If they enter bare `root`, the run fails early with a clear remediation message
- The web UI and script-only flow use the same validation rule

### Test coverage

Add tests for:
- valid prefixed username passes
- bare `root` fails with expected message
- blank username still fails with existing error path

### Suggested implementation note

Centralize this validation in one helper rather than duplicating logic across UI and shell paths.

---

## Workstream 2: Fix HA Failure Phase Naming In Metrics And PDF

### Problem

Module 3 logs `failure`, but reporting expects `during_failure`. This causes the PDF chart to omit the actual failure period.

### Files to update

- `tests/03_high_availability/run.py`
- `report/collect_metrics.py`
- `report/generate_report.py`
- any HA-specific summary or chart helpers

### Required changes

Pick one canonical phase name and use it everywhere.

Recommended canonical value:
- `failure`

Then:
1. Update the collector phase map to use the canonical value
2. Update the PDF chart to read the canonical value
3. Keep backward compatibility:
   - if existing results contain `during_failure`, still read them
4. Verify RTO summary and chart labels remain accurate

### Acceptance criteria

- HA runs show warmup, failure, and recovery in the report
- Failure-window buckets are visible in the time series
- Existing result sets with old naming still render

### Test coverage

Add tests for:
- metrics aggregation with `failure`
- metrics aggregation with `during_failure`
- HA report chart data includes the failure section

---

## Workstream 3: Promote Warm-State Metrics In Executive Output

### Problem

Warm workload support exists, but warm-state KPIs are not prominent enough in the executive summary. Customers often care more about steady-state latency than cold-run numbers.

### Files to update

- `tests/01_baseline_perf/run.py`
- `report/collect_metrics.py`
- `report/generate_report.py`
- any summary card rendering in UI/report pages

### Required changes

1. Add explicit warm-state summary fields:
   - `warm_p50_ms`
   - `warm_p95_ms`
   - `warm_p99_ms`
   - `warm_tps`
2. Surface these in:
   - `results/metrics_summary.json`
   - PDF executive summary page
   - any dashboard summary cards
3. Keep baseline best-p99, but label it clearly as:
   - `best observed p99`
4. Add narrative text in report:
   - "warm steady-state metrics are the best proxy for normal operating latency"

### Acceptance criteria

- Warm metrics appear in summary JSON
- PDF includes at least one KPI card or summary table row for warm-state latency/TPS
- Report language makes the distinction between cold/best-case and warm/steady-state explicit

### Test coverage

Add tests for:
- warm metrics populated when warm phase exists
- warm metrics omitted or null when warm phase disabled

---

## Workstream 4: Clarify Comparison Target Execution Support

### Problem

The kit lets users configure PostgreSQL and SQL Server comparison targets, but the automated runner only executes MySQL-family targets. This is acceptable, but the product must not imply otherwise.

### Files to update

- `config.yaml.example`
- `README.md`
- `lib/comparison_targets.py`
- `setup/poc_web_ui.py`
- `setup/templates/poc_web_ui.html`
- any report pages that mention comparison mode

### Required changes

1. Split comparison targets into two concepts:
   - `configurable target`
   - `automatically executable target`
2. In UI:
   - clearly badge unsupported targets as "planning only"
3. In report:
   - if unsupported target selected, say comparison configuration captured but not executed
4. In script output:
   - do not silently skip; print a clear message

### Acceptance criteria

- A user can still store unsupported targets for planning
- The run output makes it obvious whether comparison actually executed
- The PDF never implies that unsupported target benchmarks were run

### Test coverage

Add tests for:
- supported target -> executed path
- unsupported target -> planning-only messaging path

---

## Workstream 5: Make Synthetic OLTP Schema TiDB-Optimized By Default

### Problem

The main OLTP schema uses `AUTO_INCREMENT` on write-heavy tables. That is not the best default for showcasing TiDB write scalability.

### Files to update

- `setup/generate_data.py`
- any workload definitions that assume contiguous integer IDs
- `tests/03b_write_contention/run.py`
- `README.md`
- report text describing schema assumptions

### Required changes

Recommended approach:

1. Introduce a schema tuning mode under `test` or `schema` config:
   - `schema_mode: tidb_optimized | mysql_compatible`
2. Default to:
   - `tidb_optimized`
3. In `tidb_optimized` mode:
   - use `AUTO_RANDOM` or another shard-friendly key pattern for the main write-heavy tables
4. Keep `mysql_compatible` mode available for lift-and-shift validation
5. Update write-contention module language so it remains meaningful:
   - it should compare explicit hotspot-prone pattern vs TiDB-optimized pattern
   - not compete with the baseline schema accidentally

### Acceptance criteria

- Fresh default runs use TiDB-friendly keys on the main OLTP write path
- A user can still request a MySQL-like schema mode
- Module 3b remains useful and understandable

### Test coverage

Add tests for:
- generated DDL under `tidb_optimized`
- generated DDL under `mysql_compatible`
- data generation still completes for both modes

### Important implementation note

Be careful not to break foreign-key-like logical relationships or downstream query generators that assume numeric IDs.

---

## Workstream 6: Introduce Explicit Run Modes

### Problem

The current PoV mixes validation and performance benchmarking into one default path. That makes the output harder to position and makes the harness choice ambiguous.

### Files to update

- `config.yaml.example`
- `run_all.sh`
- `setup/poc_web_ui.py`
- `setup/templates/poc_web_ui.html`
- report summary text

### Required changes

Add explicit run mode selection:

- `validation`
  - current Python harness is acceptable
  - optimized for broad compatibility, light setup, and customer confidence

- `performance`
  - should route to the higher-throughput workload generator path by default
  - used when the goal is peak QPS / low latency proof

### Behavior by mode

#### Validation mode
- synthetic or customer-query checks
- baseline concurrency ladder
- warm-state run
- compatibility, import, HTAP, online DDL

#### Performance mode
- use `tidb_blaster` / TiUP-backed rawsql or workload lab path
- emphasize:
  - threads
  - connections
  - multi-loadgen
  - warmup / steady-state / cooldown
- optionally reduce nonessential modules in the same run

### Acceptance criteria

- User can tell which mode they are running
- Report clearly states which mode produced the results
- Performance mode does not depend on the generic Python harness for core throughput numbers

### Test coverage

Add tests for:
- mode default resolution
- mode-specific command planning
- report summary includes mode

---

## Workstream 7: Make High-Throughput Workload Generation The Default For Performance Runs

### Problem

The repo already contains a stronger workload generator path, but it is not yet the default for serious performance proof.

### Files to update

- `load/tidb_blaster.py`
- any CLI/web entrypoint that chooses the runner
- `run_all.sh`
- `README.md`
- UI workload lab summary text

### Required changes

1. Use `tidb_blaster` as the default for `performance` mode
2. Add a simple mapping from PoV configuration to workload generator config:
   - TiDB DSN
   - warmup
   - duration
   - cooldown
   - threads/connections
   - loadgen hosts
3. Save workload-generator outputs into the same result package or link them clearly from the report package
4. Expose runner metadata in the report:
   - runner type
   - number of load generators
   - total threads
   - total connections

### Acceptance criteria

- Performance mode uses `tidb_blaster` without extra manual wiring
- Result package includes both PoV artifacts and workload-generator summary artifacts
- Report tells the reader which harness produced the headline performance numbers

### Test coverage

Add tests for:
- config translation into workload generator config
- run directory generation
- dry-run command rendering

---

## Workstream 8: Add Curated Presets And Datasets

### Problem

The synthetic dataset is broad, but generic. It is useful for validation, but not always persuasive for customer storytelling.

### Files to update

- `config.yaml.example`
- `README.md`
- workload preset definitions
- any dataset-import helper or sample SQL assets

### Required changes

Create a small preset library:

#### Workload presets
- `oltp_read_heavy`
- `oltp_balanced`
- `oltp_write_heavy`
- `warm_operations`
- `import_heavy`
- `htap_mixed`

Each preset should define:
- workload mix
- recommended concurrency
- warm phase defaults
- recommended modules
- recommended data scale

#### Dataset profiles
- generic SaaS
- payments/fintech
- events/observability
- e-commerce

Each profile should define:
- table emphasis
- hot-path query patterns
- recommended import scale

### Acceptance criteria

- A user can choose a preset without editing raw weights manually
- Presets are described in plain language
- Presets feed both self-service validation and performance mode planning

---

## Workstream 9: Distinguish Synthetic, Customer Query, And Benchmark Results In The Report

### Problem

Right now the report can read as though all results are equally customer-specific. They are not.

### Files to update

- `report/generate_report.py`
- `report/collect_metrics.py`
- any UI summary page that lists run details

### Required changes

Add explicit provenance labels:
- synthetic schema + synthetic workload
- synthetic schema + customer queries
- customer-shaped benchmark via workload generator

Add narrative rules:
- if customer queries are empty, say so
- if synthetic data only, say so
- if workload generator was used, say so prominently

### Acceptance criteria

- A non-technical reader can tell what the numbers represent
- The report does not overstate customer-specific realism

---

## Workstream 10: Capture DB-Side Observability Snapshot

### Problem

The report relies heavily on client-side latency and throughput numbers. For stronger credibility, the package should also include DB-side evidence.

### Files to update

- observability guide helpers
- report generators
- run artifact packaging

### Required changes

Add optional observability capture:
- TiDB Dashboard screenshots or exported panel data
- Grafana snapshots where available
- cluster tier and topology metadata
- TiFlash status when HTAP is enabled

Recommended minimum fields:
- cluster tier
- run mode
- warm phase enabled
- concurrency levels
- client runner type
- DB-side dashboard capture status

### Acceptance criteria

- Report package includes a machine-readable observability metadata file
- If screenshots are available, they are referenced in the final report appendix

---

## Suggested Implementation Order For Codex

### PR 1: Onboarding + reporting correctness
- Workstream 1
- Workstream 2
- Workstream 3
- Workstream 4

### PR 2: TiDB-optimized defaults
- Workstream 5
- update docs and sample configs

### PR 3: Performance mode
- Workstream 6
- Workstream 7

### PR 4: Presets and report clarity
- Workstream 8
- Workstream 9

### PR 5: Observability packaging
- Workstream 10

---

## Definition Of Done

The PoV should be considered hardened when all of the following are true:

1. A new TiDB Cloud user can complete a Starter/Serverless run without credential-format confusion
2. HA charts and summary metrics match the actual recorded failure phase
3. Warm-state metrics are visible in both machine-readable and executive-facing outputs
4. The default schema showcases TiDB-friendly write scaling
5. Performance mode uses the high-throughput harness by default
6. Unsupported comparison targets are clearly marked as planning-only
7. The PDF clearly states whether results came from synthetic, customer-query, or high-throughput benchmark paths

---

## Immediate Codex Prompt

Use this if you want to hand the first phase directly to Codex:

> Implement Phase 1 from `docs/poc_hardening_implementation_backlog.md`. Do not edit `config.yaml`. Fix TiDB Cloud username defaults and validation, fix HA failure-phase naming across module/collector/report, promote warm-state KPIs into summary outputs, and clarify unsupported comparison targets as planning-only in UI/docs/report text. Add regression tests for each change. Keep changes small and reviewable.

