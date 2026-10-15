# KG Schema v1 (frozen 2026-08-12 for the Sep 1 demo)

Status: ratified by jk 2026-08-07. Grounded in the KG development workplan
(`Planning/archive/KG_development_workplan_20260806_20260820.md`, §2)
and in the real shape of `universal_pd_v3` `extracted_data`.

## Architecture: Track B is the KG core

- **PostgreSQL `kg.assertions` is the source of truth.** One row per
  (canonical source, relation_type, canonical target, pmid), with
  `effect_direction`, `confidence`, `evidence_tier`, `year`, `sample_size`,
  `p_value`. Per-PMID provenance is never collapsed.
- **PostgreSQL `kg.edge_summaries`** holds aggregated edges: `n_papers`,
  `n_positive` / `n_negative` / `n_neutral`, evidence-tier counts, year range,
  avg/max confidence — the "12 papers report X→Y" statistics.
- **Neo4j is the serving/exploration layer only**: `CanonicalEntity` nodes,
  aggregated `ASSERTED` relationships, and `HAS_EVIDENCE → Paper(pmid)` links
  for drill-down. It is rebuilt from Postgres, never written to directly by
  extraction.

## Nodes: CanonicalEntity

```
CanonicalEntity {
  canonical_id,      # stable id from kg.assertions canonicalization
  name,              # canonical display name
  entity_type,       # one of 21 types (below)
  mesh_id?,          # MeSH DescriptorUI when matched
  synonyms[],        # known surface forms
  n_assertions       # assertion count for ranking
}
```

Entity types (21): Gene, Protein, Compound, Pathway, Disease, Symptom,
Biomarker, Exercise, CellType, Species, Mechanism, Outcome, Concept,
Organelle, Treatment, BrainRegion — plus 5 types ratified by jk 2026-09-12
during the top-300 Concept adjudication
(`eval_reports/kg_curation_apply_20260912.md`):

- **Neurotransmitter** — signaling molecules transmitting signals across synapses (e.g. dopamine, serotonin).
- **Assay** — laboratory or clinical measurement procedures/tests (e.g. ELISA, UPDRS).
- **EnvironmentalFactor** — external exposures influencing disease risk or course (e.g. pesticides, head trauma).
- **CellComponent** — subcellular structures not covered by Organelle (e.g. synapses, axons, membranes).
- **StudyDesign** — clinical/epidemiological study-design concepts (e.g. healthy controls, participants, cohorts).

Since 2026-09-13 the extraction schemas themselves emit the 21 types: the
`entity_type` enum and prompt rules in `universal_pd_v3/v4/v5.json`, the
`UniversalPDV3` pydantic model (also used to validate v4), the runner's
allowed-type list, and the KG extractor prompt/labels all include the 5 new
types. Note the ~40k v5 rows extracted before that date used the 16-type
vocabulary; output fields are otherwise identical.

(`mesh_id` is null in ~100% of raw v3
extractions; normalization happens post-extraction, at KG build time.)

**Curation exception (ratified 2026-09-12):** adjudicated curation (e.g. the
top-300 Concept reclassification) patches Neo4j directly with `SET`/`REMOVE`
— the "never written to directly" rule above applies to *extraction*, not to
owner-adjudicated curation. The durable state for such patches is PostgreSQL
(`kg.entity_synonyms.entity_type`) plus `CURATED_TYPE_OVERRIDES` in
`maintenance/kg_normalization_lib.py`, so any Track-B rebuild from Postgres
reproduces the adjudicated types.

## Edges: ASSERTED

Aggregated edge between two `CanonicalEntity` nodes:

```
ASSERTED {
  relation_type,      # raw type (29 values) — always preserved
  display_type,       # curated type (~20 values) for UI display
  effect_direction,   # positive / negative / neutral / unknown
  n_papers, n_positive, n_negative, n_neutral,
  avg_confidence, evidence_tiers, year_min, year_max
}
```

Per-PMID expansion lives in `kg.assertions`; in Neo4j the aggregated edge
carries `HAS_EVIDENCE → Paper(pmid)` links with per-evidence
`confidence`, `context`, `effect_direction` for drill-down.
(**Status 2026-08-12:** `HAS_EVIDENCE` links are not yet ingested — 0 edges.
The demo UI (`sciai_core/ui/panels/kg_explorer.py` via
`sciai_core/kg/trackb_queries.py`) therefore reads Track-B directly from the
PostgreSQL tables, which hold the same aggregated data plus per-PMID
provenance.)

### The 29 raw relation types

targets, treats, activates, involves, increases, inhibits, studies, assesses,
associated_with, induces, decreases, evaluates, models, damages, type_of,
regulates, biomarker_for, uses, co_occurs_with, other, binds_to, encodes,
combined_with, compared_with, located_in, comorbid_with, metabolized_to,
predicts, interacts_with.

Vocabulary locked by the 2026-09-13 adjudication
(`eval_reports/relation_type_review_20260913.md`, all 29 confirmed by jk;
applied in `eval_reports/relation_type_apply_20260913.md`).

Direction conventions (extraction prompt): increases/induces/activates →
positive; decreases/inhibits/damages/treats → negative — **`treats` is stored
as negative**: treatment reduces the condition; associated_with/involves/
co_occurs_with → neutral; studies → unknown; regulates → direction from
context (up = positive, down = negative).

Documented boundaries:

- `binds_to` = direct physical binding; `interacts_with` = functional or
  unspecified interaction (weaker claim).
- `assesses` = an instrument/assay/score measures an outcome in subjects;
  `evaluates` = a study/intervention evaluates a property, effect, or
  performance.
- `localizes_to` was merged into `located_in` (2026-09-13): `located_in`
  covers both anatomical and subcellular localization; the granularity is
  carried by the target's entity type (CellComponent/Organelle vs
  BrainRegion). Legacy payloads fold at load time
  (`kg_normalization_lib.RELATION_TYPE_FOLD`).

### Display curation (29 → 20)

Near-synonymous types collapse for **display only**; the raw `relation_type`
is always kept as a property. Canonical map (code:
`sciai_core/kg/extraction_models.py::RELATION_DISPLAY_ALIASES`):

| Raw type | Display type |
|---|---|
| increases | activates |
| decreases | inhibits |
| evaluates | assesses |
| uses, models | studies |
| targets, binds_to | interacts_with |
| comorbid_with | co_occurs_with |
| involves | associated_with |

All other types display as themselves → 20 curated display types.

## Entity normalization (priority order)

1. Exact `kg.entity_synonyms` hit.
2. Case/punctuation-folded hit.
3. MeSH descriptor/concept match (`MeSHTerm` lookup).
4. Curated gene/protein list (existing allow-list).
5. Frequency-ranked manual curation queue.
6. **Self-canonical fallback** (ratified 2026-08-07): keep the name as-is with
   `normalized=false` — better a noisy node than dropped evidence.

## Corpus linkage

`Paper` nodes get a `corpus_ids` property, populated from
`reviews.corpus_papers`, enabling corpus-scoped KG queries (subgraph stats,
top-edges tables, and the KG-evidence panel in CRA reports).

## Loader notes (Track A, kept read-only)

The Track-A incremental loader (`sciai_core/kg/entity_extractor.py`,
`pipeline.py`) accepts all 29 raw relation types; a paper that fails KG-model
validation is logged and skipped, never fatal. `corpus_id=None` in the loader
chain means "any corpus" — the weekly cron relies on this to pick up bulk
corpus extractions, not only ad-hoc (corpus_id=0) ones.
