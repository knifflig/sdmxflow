# Glossary

This page defines SDMX and `sdmxflow` terms used throughout the documentation.

## SDMX terms

- **SDMX**: Statistical Data and Metadata eXchange, a standard for exchanging statistical data.
- **Dataflow**: A dataset definition / endpoint identifier in SDMX.
- **DSD (Data Structure Definition)**: Schema describing dimensions, attributes, and measures.
- **Dimension**: A column that identifies a slice of the data (e.g., time, geo).
- **Attribute**: Descriptor columns that provide additional context.
- **Codelist**: A reference list mapping a short code to a label.

## sdmxflow terms

- **Artifact contract**: The stable on-disk layout and file semantics `sdmxflow` produces.
- **Append-only**: New upstream versions are appended; older versions remain.
- **Slice**: One downloaded upstream version of the dataset.
- **`last_updated`**: The upstream version tag written into every fact row.
- **`metadata.json` versions**: The append-only history of downloaded slices.

Where to go next:

- [Concepts & Design](concepts-and-design.md)
- [Output Artifacts (Contract)](output-layout.md)
