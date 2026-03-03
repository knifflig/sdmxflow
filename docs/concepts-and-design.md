# Concepts & Design

This page explains the design choices behind `sdmxflow`: append-only artifacts, incremental refresh logic, and why the outputs are shaped for warehouse ingestion.

## The mental model

`sdmxflow` is not a “query SDMX interactively” tool. It is a **scheduled ingestion** tool that turns an SDMX dataset into stable artifacts you can load repeatedly.

Key idea: **you don’t mutate history**. You append a new upstream version as a new slice in `dataset.csv` and keep the lineage in `metadata.json`.

## Append-only artifacts (and why)

`sdmxflow` writes an append-only facts file:

- `dataset.csv` keeps *all* downloaded versions over time.
- Each row is tagged with a `last_updated` value (UTC ISO-8601) that identifies the upstream version it came from.

Why this is useful:

- **Reproducibility:** older versions remain available.
- **Idempotent scheduling:** a run that finds no upstream change does not rewrite the facts.
- **Warehouse-friendly:** you can load once and query “latest version” using `last_updated`.

> **Gotcha**
> Append-only means duplicates across versions are expected. The same “business key” row may appear multiple times (one per upstream version). Your downstream models should filter to the latest `last_updated` (or build a snapshot dimension).

## Incremental refresh logic (what “changed?” means)

A single `fetch()` run:

1. Ensures the output folder exists.
2. Queries the provider for an upstream *“last updated”* timestamp.
3. Loads `metadata.json` if present (otherwise initializes it).
4. Compares the upstream timestamp to the most recently recorded `last_updated_data_at`.
5. If different: downloads a new dataset slice and appends it; then updates metadata and codelists.
6. If unchanged: skips the dataset download/append, but still ensures metadata and codelists are up to date.

In the current implementation:

- For `source_id="ESTAT"`, upstream change detection uses Eurostat’s SDMX metadata (“annotations”) via `sdmxflow.query.last_updated_data.eurostat_last_updated()`.
- The comparison is strict string equality on the canonical UTC ISO-8601 timestamp stored in `metadata.json`.

> **Operational note**
> If the upstream provider republishes data without updating their “last updated” signal, `sdmxflow` will not append a new slice. That’s a provider semantics issue, not a local-state issue. See [Provider Support](provider-support.md).

## Metadata history (lineage & reproducibility)

`metadata.json` is the audit trail for your ingestion:

- dataset identity (`agency_id`, `dataset_id`, `key`, `params`)
- `created_at`, `last_fetched_at`, `last_updated_at`, `last_updated_data_at`
- `versions[]`: an append-only list of versions, with HTTP details and `rows_appended`
- `files`: relative paths for artifacts
- `codelists[]`: mapping from dataset columns to exported codelist CSVs

Why it exists:

- **Lineage:** where data came from (URL/status/headers when available)
- **Governance:** what changed and when
- **Debuggability:** what a run did without digging through logs

## Codelists and dataset dimensions

Many SDMX datasets encode dimensions/attributes as short codes (e.g., `SEX=M`). To interpret or join those columns in a warehouse model, you need the corresponding code → label tables.

`sdmxflow`:

- downloads SDMX structures (Dataflow + DSD)
- extracts the codelists referenced by dimensions/attributes
- writes one CSV per codelist under `codelists/` with columns `code,name`
- stores a column → codelist mapping in `metadata.json`

Practical implication: treat `codelists/*.csv` as reference tables in your warehouse.

## Deterministic artifacts (principles and gotchas)

`sdmxflow` aims for stable, deterministic outputs:

- Stable filenames (`dataset.csv`, `metadata.json`, `codelists/<ID>.csv`)
- Stable “slice tagging” via the `last_updated` column
- Strict schema matching when appending: if the provider CSV columns change, `sdmxflow` raises an error instead of silently corrupting the append history.

> **Warning**
> If the provider changes the dataset schema (columns), appends will fail with a metadata/schema error. In that case, treat it as a breaking upstream change and rebuild into a fresh `out_dir` (see [FAQ & Troubleshooting](faq.md)).

## Glossary (SDMX terms)

- **SDMX**: Statistical Data and Metadata eXchange, a standard for statistical data exchange.
- **Dataflow**: A dataset definition/endpoint identifier in SDMX.
- **DSD (Data Structure Definition)**: The schema describing dimensions, attributes, measures.
- **Dimension**: A column used to identify a slice of data (e.g., time, geography).
- **Attribute**: Additional descriptor columns (may also use codelists).
- **Codelist**: Reference list mapping a code to a human-readable label.
- **Last updated**: Provider-specific signal for when data changed; used by `sdmxflow` to decide whether to append.

Next:

- See [Output Artifacts (Contract)](output-layout.md) for the stable on-disk contract.
- See [Integration Patterns](integration-patterns.md) for warehouse loading examples.
