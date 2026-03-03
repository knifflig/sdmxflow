# Provider Support

This page documents which SDMX providers are supported today and how provider differences affect refresh semantics.

## Current support

- **Eurostat** (`source_id="ESTAT"`)
  - Change detection uses Eurostat’s upstream “last updated” metadata.
  - Data download uses Eurostat’s SDMX 3.0 bulk dissemination endpoint (CSV).
  - Codelists are extracted from SDMX structures (Dataflow + DSD).

> **Status**
> Provider support is currently limited to Eurostat, but the project is designed to add additional sources over time. The public artifact contract is intended to remain stable across providers.

## Planned / possible future work

This is intentionally generic (not a commitment):

- additional SDMX sources via the SDMX protocol (national statistical offices, central banks, etc.)
- provider-specific optimizations (bulk endpoints, caching)
- richer metadata capture for governance

## Provider differences (what can vary)

Across SDMX providers, these aspects can differ:

- **Change detection signal:** what “last updated” means and how reliable it is.
- **Download mechanics:** bulk CSV endpoints vs generic SDMX queries.
- **Schema stability:** some providers change column ordering or naming.
- **Codelist availability:** completeness and language coverage for labels.

`sdmxflow` tries to normalize what it controls (artifact layout, metadata schema), but it cannot force provider semantics.

## How `sdmxflow` detects updates

At a high level:

- Fetch upstream `last_updated`.
- Compare to the latest locally recorded version in `metadata.json`.
- Append only when the timestamp differs.

See [Concepts & Design](concepts-and-design.md) for the full workflow.

## How to add a provider (high-level)

Provider support is implemented in the Python package (not in docs config). The rough steps:

1. Implement an upstream “last updated” query for the provider.
2. Implement a dataset downloader that materializes a provider dataset slice as a CSV.
3. Wire the provider into `SdmxDataset.fetch()` (dispatch by `source_id`).
4. Ensure codelists/structures can be downloaded and mapped to dataset columns.
5. Add tests that cover:
   - changed vs unchanged upstream timestamps
   - schema mismatch behavior
   - error classification (timeout/unreachable/interrupted)

For contributor expectations and repo tooling, see [Development & Contributing](development.md).
