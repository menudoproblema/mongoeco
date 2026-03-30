# Benchmarks

This directory contains the benchmark harness used to compare:

- `mongoeco` sync with the memory engine
- `mongoeco` sync with the SQLite engine
- `mongoeco` async with the memory engine
- `mongoeco` async with the SQLite engine
- `mongomock`

The goal is not to publish universal headline numbers. The goal is to make
performance discussions reproducible, diagnosable and comparable across
commits, machines and engines.

## What The Harness Measures

The suite currently covers these workload groups:

- `secondary_lookup_indexed`
- `secondary_lookup_unindexed`
- `secondary_lookup_diagnostics`
- `simple_aggregation`
- `materializing_aggregation`
- `sort_limit`
- `cursor_consumption`
- `filter_selectivity`
- `predicate_diagnostics`
- `sort_shape_diagnostics`

Each group is designed to answer a specific question:

- lookup workloads isolate point reads with and without usable indexes
- cursor workloads separate `first()` from full materialization
- filter workloads isolate predicate cost by selectivity
- predicate diagnostics separate equality vs comparison hot paths
- sort diagnostics separate `limit` top-k behavior from full sort behavior
- aggregation workloads distinguish mostly-streamable pipelines from clearly
  materializing pipelines

The harness measures end-to-end API calls, not internal operators in isolation.
That is deliberate: it lets us catch planner, materialization, codec, cursor
and engine overhead in one place.

## Reproducibility Contract

The harness is intended to be reproducible by anyone cloning the repository.
For that reason:

- datasets are deterministic
  - `generate_users(..., seed=42)`
  - `generate_orders(..., seed=43)`
- the report includes Python version, platform, selected workloads, git
  revision and whether the worktree is dirty
- benchmark outputs are local artifacts and are ignored by git
- workloads can be selected explicitly with repeated `--workload` flags so a
  discussion can focus on one subsystem without rerunning the whole suite

## Installation

Install the project and benchmark dependencies from the repository root:

```bash
python -m pip install -e .[dev]
python -m pip install -r benchmarks/requirements.txt
```

Optional extras:

- `mongomock` is required if you want to benchmark the `mongomock` adapter
- `psutil` improves RSS reporting; without it, RSS values are reported as zero
- `orjson` is optional and controlled through `MONGOECO_JSON_BACKEND`

## Recommended Community Workflow

For results that are worth sharing:

1. Use a clean working tree or, at minimum, note that the tree is dirty.
2. Run on the same machine, power profile and Python version when comparing commits.
3. Use at least `--warmup 1 --repetitions 5` for anything you plan to cite.
4. Keep outputs local under `benchmarks/reports/`.
5. Always share:
   - exact command
   - git revision
   - machine / OS / Python version
   - whether `mongomock`, `psutil` and `orjson` were installed

Suggested commands:

Smoke check:

```bash
python -m benchmarks.run --engine all --size 1000 --warmup 0 --repetitions 1
```

Publication-quality local snapshot:

```bash
python -m benchmarks.report \
  --engine all \
  --size 10000 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/latest.json \
  --output-markdown benchmarks/reports/latest.md
```

Targeted diagnostic run:

```bash
python -m benchmarks.report \
  --engine all \
  --size 1000 \
  --warmup 0 \
  --repetitions 3 \
  --workload predicate_diagnostics \
  --workload sort_shape_diagnostics \
  --output-json benchmarks/reports/diagnostics.json \
  --output-markdown benchmarks/reports/diagnostics.md
```

Baseline comparison:

```bash
python -m benchmarks.report \
  --engine all \
  --size 10000 \
  --warmup 1 \
  --repetitions 5 \
  --baseline-json benchmarks/reports/baseline.json \
  --output-json benchmarks/reports/latest.json \
  --output-markdown benchmarks/reports/latest.md
```

## How To Read The Results

Use the benchmark output to support claims like these:

- "this change improves sort top-k in `memory`"
- "this planner change helps SQLite equality but not range"
- "the remaining gap is in materialization, not filtering"

Avoid overclaiming from one noisy number. Prefer:

- deltas across repeated runs
- comparisons inside the same workload family
- plan summaries from `explain()`
- targeted diagnostics before and after a change

## Local Artifacts

`benchmarks/reports/` is reserved for local outputs. The harness writes there by
default in examples, but the generated JSON and Markdown reports are ignored by
git on purpose.
