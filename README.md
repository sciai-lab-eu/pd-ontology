# PD Ontology (sciai-lab)

A machine-readable ontology of what the Parkinson's-disease literature
actually **claims** — built bottom-up from structured extraction over
~189,000 papers, not top-down from expert design.

- **Concepts:** ~173,000 (PD-scoped, v0.2) across **21 entity types**
  (Gene, Protein, Chemical, BrainRegion, Symptom, Assay, CellType, …)
- **Assertions:** ~797,000 citation-backed claims (v0.2), each with pmid,
  verbatim context, direction, confidence, and extraction provenance
- **Relations:** 29 ratified types with explicit direction conventions
- **Anchoring:** HGNC (genes), UniProt (proteins), MeSH (chemicals and more);
  the remainder is a documented, adjudicated long tail
- **Releases:** [Zenodo concept DOI 10.5281/zenodo.22759155](https://doi.org/10.5281/zenodo.22759155)
  (versioned; v0.2 = 10.5281/zenodo.23317587) · **BioPortal PDOSC**
- **License:** CC BY-NC 4.0

## Repository contents

- `tools/` — the export pipeline (PostgreSQL knowledge graph → SKOS Turtle,
  edge CSV, assertion JSONL, manifest), including the v0.3 hierarchy writer
  (entity-type top level + MeSH-tree mid-level)
- `docs/` — the KG/ontology schema (entity types, relation vocabulary and
  direction rules), release notes
- Data files are **not** in git — get them from Zenodo (linked above).

## Design stance

The ontology is published **as work in progress, deliberately**: it is an
instrument for crowdsourced error identification. Known error classes and the
public correction process are documented in the article
[“An ontology that asks to be corrected”](https://pd.sciai.eu/) (pd.sciai.eu)
and the release notes. Every merge and normalization decision ships with a
rollback export and an adjudication record.
