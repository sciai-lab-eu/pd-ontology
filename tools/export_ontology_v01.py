#!/usr/bin/env python3
"""PD Ontology public export (Track A read-only) — v0.1 and v0.2.

v0.1 (2026-09-14): first public export, full corpus scope.
v0.2 (2026-10-12, prepared 2026-10-08): **PD-only scope** (jk 2026-10-06):
non-PD pilot material (esophagus/PPI topics) excluded, EXCEPT PPI×levodopa
material (PPI substances lowering levodopa efficiency). The scope predicate
reuses the production PD topic guard byte-for-byte
(sciai_core.topics._pd_guard_sql, strength=strong: 'parkinson' in
title/abstract OR MeSH 'Parkinson Disease') plus the documented exception:
paper mentions a PPI substance (keywords from config/sciai_harvesting_topics.yaml
topic 'ppi') AND levodopa/L-DOPA. Everything else (vocabulary, direction
conventions incl. the 2026-09-24 treats/regulates normalizations, entity
typing authority rule) is current-state.

Exports (CC BY-NC 4.0):
  pd_ontology_vX.ttl                SKOS vocabulary: 21 entity types + 29
                                    relation types, definitions, direction
                                    conventions, display curation map.
  pd_ontology_entities_vX.ttl.gz    Entity instances as SKOS concepts
                                    (v0.2: canonicals used in scoped
                                    assertions only).
  pd_ontology_edges_vX.csv.gz       PRIMARY artifact: aggregated edges with
                                    n_instances, direction split, confidence.
  pd_ontology_assertions_vX.jsonl.gz  Full assertion dump (only with
                                    --full-dump).
  LICENSE / README.md / manifest.json / CHANGELOG.md

Run:
    /55a11/sciai/.venv/bin/python /55a11/maintenance/export_ontology_v01.py \
        --version 0.2 --pd-only --full-dump     # Monday 2026-10-12 release run
    /55a11/sciai/.venv/bin/python /55a11/maintenance/export_ontology_v01.py \
        --version 0.1                            # reproduces v0.1 (unchanged path)
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import psycopg

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from sciai_automation_lib import (  # noqa: E402
    ensure_sciai_next_on_path,
    get_dsn,
    load_dotenv_repo,
    setup_logging,
)
from ingest_edge_summaries_to_neo4j import _load_entity_metadata  # noqa: E402

ensure_sciai_next_on_path()
load_dotenv_repo()

from sciai_core.kg.extraction_models import RELATION_DISPLAY_ALIASES  # noqa: E402

logger = setup_logging("export_ontology_v01", Path("/55a11/logs/export_ontology_v01.log"))
logger.propagate = False  # ingest_edge_summaries_to_neo4j calls logging.basicConfig on import

VERSION = "0.1"
EXPORT_DATE = "2026-09-14"
BASE_IRI = "https://pd.sciai.eu/ontology"
PROP_IRI = f"{BASE_IRI}/property"
DEFAULT_OUT = Path("/55a11/output/exports/pd_ontology_v0.1/")
SCHEMA_SOURCE = "sciai_next/docs/kg_schema_v1.md (KG Schema v1, frozen 2026-08-12, amended 2026-09-12/13)"

# Version registry. v0.1 is frozen (reproducible); v0.2 is the PD-only release
# (jk 2026-10-06), prepared 2026-10-08, released 2026-10-12.
VERSIONS = {
    "0.1": {"export_date": "2026-09-14",
            "out": Path("/55a11/output/exports/pd_ontology_v0.1/"),
            "pd_only": False},
    "0.2": {"export_date": "2026-10-12",
            "out": Path("/55a11/output/exports/pd_ontology_v0.2/"),
            "pd_only": True},
}


def build_pd_scope_sql() -> Tuple[str, List]:
    """Scope predicate for v0.2 (PD-only + PPI×levodopa exception).

    The PD part is the PRODUCTION corpus-building guard byte-for-byte
    (sciai_core.topics._pd_guard_sql, strength 'strong'). The exception adds
    papers mentioning a PPI substance (keywords of the 'ppi' harvest topic)
    AND levodopa/L-DOPA — jk's "PPIs as substances lowering levodopa
    efficiency" carve-out. Returns (sql_fragment referencing alias p, params).
    """
    from sciai_core.topics import TopicRegistry, _like_patterns, _pd_guard_sql

    pd_sql, pd_params = _pd_guard_sql("strong")
    ppi = TopicRegistry().get("ppi")
    ppi_kws = list(ppi.keywords) if ppi else []
    if not ppi_kws:
        raise RuntimeError("topic 'ppi' not found in the harvest registry — cannot build the exception")
    text_sql = "LOWER(COALESCE(p.title, '') || ' ' || COALESCE(p.abstract, ''))"
    ppi_sql = f"{text_sql} LIKE ANY(%s)"
    lev_sql = f"{text_sql} LIKE ANY(%s)"
    ppi_params = [p for kw in ppi_kws for p in _like_patterns(kw)]
    lev_params = ["%levodopa%", "%l-dopa%"]
    return f"(({pd_sql}) OR ({ppi_sql} AND {lev_sql}))", pd_params + [ppi_params, lev_params]


def create_scope_table(conn) -> Dict[str, int]:
    """Build temp table tmp_export_scope(pmid) with the v0.2 scope; return stats."""
    scope_sql, params = build_pd_scope_sql()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_export_scope")
        cur.execute("CREATE TEMP TABLE tmp_export_scope (pmid BIGINT PRIMARY KEY)")
        cur.execute(
            f"INSERT INTO tmp_export_scope SELECT p.pmid::bigint FROM pubmed.papers p WHERE {scope_sql}",
            params)
        cur.execute("SELECT count(*) FROM tmp_export_scope")
        n_scope = cur.fetchone()[0]
        # stats for the README: PD-only vs PPI-exception split
        pd_sql, pd_params = _pd_guard_sql_stats()
        cur.execute(f"SELECT count(*) FROM pubmed.papers p WHERE {pd_sql}", pd_params)
        n_pd = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pubmed.papers")
        n_all = cur.fetchone()[0]
    return {"papers_total": n_all, "papers_pd_guard": n_pd, "papers_scope": n_scope,
            "papers_ppi_exception": n_scope - n_pd}


def _pd_guard_sql_stats() -> Tuple[str, List]:
    from sciai_core.topics import _pd_guard_sql
    return _pd_guard_sql("strong")

# ---------------------------------------------------------------------------
# Locked vocabulary (sciai_next/docs/kg_schema_v1.md).
# Definitions for the 5 types ratified 2026-09-12 are verbatim from the schema
# doc; the 16 base-type definitions are editorial (flagged for jk in README).
# ---------------------------------------------------------------------------
ENTITY_TYPES: Dict[str, str] = {
    "Gene": "Genes and genetic loci (e.g. SNCA, LRRK2, GBA).",
    "Protein": "Proteins and peptides, including receptors and enzymes (e.g. alpha-synuclein, tyrosine hydroxylase).",
    "Compound": "Chemicals and drugs (e.g. levodopa, caffeine); neurotransmitters are typed separately.",
    "Pathway": "Biological pathways and pathological processes (e.g. autophagy, ferroptosis).",
    "Disease": "Diseases and syndromes (e.g. Parkinson disease, synucleinopathies).",
    "Symptom": "Signs and symptoms (e.g. bradykinesia, falls, hypoxia).",
    "Biomarker": "Measurable indicators of disease state or risk (e.g. beta oscillations).",
    "Exercise": "Physical exercise and rehabilitation activities (e.g. Tai Chi, treadmill training).",
    "CellType": "Cell types (e.g. astrocytes, dopaminergic neurons).",
    "Species": "Organisms, using the actual species name (e.g. Mus musculus).",
    "Mechanism": "Mechanisms, phenomena, and processes (e.g. neuroprotection, aging).",
    "Outcome": "Clinical or functional outcomes (e.g. activities of daily living, walking speed).",
    "Concept": "Residual biomedical concepts not covered by a specific type.",
    "Organelle": "Cellular structures (e.g. mitochondria, lysosomes, extracellular vesicles).",
    "Treatment": "Therapeutic and surgical interventions (e.g. deep brain stimulation).",
    "BrainRegion": "Anatomical brain regions (e.g. basal ganglia, substantia nigra).",
    "Neurotransmitter": (
        "Signaling molecules transmitting signals across synapses (e.g. dopamine, serotonin)."
    ),
    "Assay": "Laboratory or clinical measurement procedures/tests (e.g. ELISA, UPDRS).",
    "EnvironmentalFactor": (
        "External exposures influencing disease risk or course (e.g. pesticides, head trauma)."
    ),
    "CellComponent": "Subcellular structures not covered by Organelle (e.g. synapses, axons, membranes).",
    "StudyDesign": "Clinical/epidemiological study-design concepts (e.g. healthy controls, participants, cohorts).",
}

# relation_type -> (definition, default direction convention).
# Direction conventions are verbatim from the extraction prompt documented in
# kg_schema_v1.md; "context" = direction decided from context;
# "unspecified" = no convention fixed in the schema doc.
RELATION_TYPES: Dict[str, Tuple[str, str]] = {
    "targets": ("The source entity is a molecular or therapeutic target of/by the target entity.", "unspecified"),
    "treats": (
        "The source treatment reduces or ameliorates the target condition. "
        "Stored as NEGATIVE: treatment reduces the condition.",
        "negative"),
    "activates": ("The source entity activates or up-regulates the target.", "positive"),
    "involves": ("The target entity is involved in the source process or condition.", "neutral"),
    "increases": ("The source entity increases the target entity or process.", "positive"),
    "inhibits": ("The source entity inhibits or down-regulates the target.", "negative"),
    "studies": ("The source study or instrument studies the target.", "unknown"),
    "assesses": (
        "An instrument/assay/score measures an outcome in subjects "
        "(boundary: 'evaluates' = a study/intervention evaluates a property, effect, or performance).",
        "unspecified"),
    "associated_with": ("The source entity is associated with the target entity.", "neutral"),
    "induces": ("The source entity induces the target entity or process.", "positive"),
    "decreases": ("The source entity decreases the target entity or process.", "negative"),
    "evaluates": (
        "A study or intervention evaluates a property, effect, or performance "
        "(boundary: 'assesses' = an instrument/assay/score measures an outcome in subjects).",
        "unspecified"),
    "models": ("The source model system models the target disease or process.", "unspecified"),
    "damages": ("The source entity damages the target.", "negative"),
    "type_of": ("The source entity is a type/subclass of the target entity.", "unspecified"),
    "regulates": (
        "The source entity regulates the target; direction from context "
        "(upregulation = positive, downregulation = negative).",
        "context"),
    "biomarker_for": ("The source entity is a biomarker for the target condition.", "unspecified"),
    "uses": ("The source study or method uses the target entity.", "unspecified"),
    "co_occurs_with": ("The source and target entities co-occur.", "neutral"),
    "other": (
        "Residual relation type. NOTE: 'other' relationships are not loaded into "
        "kg.assertions (skipped at populate time), so zero assertions carry it.",
        "unspecified"),
    "binds_to": (
        "Direct physical binding between source and target "
        "(boundary: 'interacts_with' = functional or unspecified interaction, a weaker claim).",
        "unspecified"),
    "encodes": ("The source gene encodes the target protein.", "unspecified"),
    "combined_with": ("The source entity is administered/used in combination with the target.", "unspecified"),
    "compared_with": ("The source entity is compared with the target entity.", "unspecified"),
    "located_in": (
        "The source entity is located in the target region or structure; covers both anatomical "
        "and subcellular localization (the former 'localizes_to' was merged into 'located_in' "
        "2026-09-13; granularity is carried by the target's entity type: CellComponent/Organelle "
        "vs BrainRegion).",
        "unspecified"),
    "comorbid_with": ("The source condition is comorbid with the target condition.", "unspecified"),
    "metabolized_to": ("The source compound is metabolized to the target compound.", "unspecified"),
    "predicts": ("The source entity predicts the target outcome or condition.", "unspecified"),
    "interacts_with": (
        "Functional or unspecified interaction between source and target "
        "(boundary: 'binds_to' = direct physical binding, a stronger claim).",
        "unspecified"),
}

# ---------------------------------------------------------------------------
# Turtle helpers (dependency-free; rdflib is not installed in the venv).
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Escape a string for a Turtle \"...\" literal."""
    return (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _lit(text: str, lang: bool = True) -> str:
    return f'"{_esc(text)}"' + ("@en" if lang else "")


def _entity_iri(canonical_id: str) -> str:
    return f"<{BASE_IRI}/entity/{quote(canonical_id, safe='')}>"


def _type_iri(entity_type: str) -> str:
    return f"<{BASE_IRI}/type/{entity_type}>"


def _relation_iri(relation_type: str) -> str:
    return f"<{BASE_IRI}/relation/{relation_type}>"


# ---------------------------------------------------------------------------
# Vocabulary export
# ---------------------------------------------------------------------------

def build_vocabulary_ttl() -> str:
    v2 = VERSION == "0.2"
    lines: List[str] = []
    w = lines.append
    w("@prefix owl: <http://www.w3.org/2002/07/owl#> .")
    w("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
    w("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
    w("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .")
    w("@prefix dcterms: <http://purl.org/dc/terms/> .")
    w("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .")
    w(f"@prefix pdo: <{BASE_IRI}/> .")
    w(f"@prefix sciai: <{PROP_IRI}/> .")
    w("")
    w(f"<{BASE_IRI}/> a owl:Ontology ;")
    w('    dcterms:title "SciAI Parkinson\'s Disease Ontology"@en ;')
    w(f'    owl:versionInfo "{VERSION}" ;')
    w(f'    dcterms:issued "{EXPORT_DATE}"^^xsd:date ;')
    w('    dcterms:creator "sciai-lab / Johannes Keizer" ;')
    w('    dcterms:license <https://creativecommons.org/licenses/by-nc/4.0/> ;')
    w('    dcterms:rights "CC BY-NC 4.0 — attribution: sciai-lab / Johannes Keizer"@en ;')
    w(f'    dcterms:source "{_esc(SCHEMA_SOURCE)}" ;')
    if v2:
        w('    dcterms:description "Work-in-progress ontology of Parkinson\'s disease research, '
          'extracted from the PubMed literature by the SciAI platform. v0.2 is PD-only in scope '
          '(the v0.1 pilot topics esophagus/PPI are excluded; PPI×levodopa material is kept). '
          'Vocabulary: 21 entity types and 29 relation types (locked 2026-09-12/13). Entity '
          'canonicals are authority-anchored where possible (HGNC for genes, UniProt accessions '
          'for proteins, MeSH for chemicals — 2026-09-29/30 anchoring program, see CHANGELOG). '
          'Direction conventions normalized 2026-09-24: treats is stored negative (treatment '
          'reduces the condition); regulates is direction-free (neutral)."@en ;')
    else:
        w('    dcterms:description "Work-in-progress ontology of Parkinson\'s disease research, '
          'extracted from the PubMed literature by the SciAI platform. Vocabulary: 21 entity types '
          'and 29 relation types (locked 2026-09-12/13). Instances are entity concepts with '
          'labels, synonyms and MeSH cross-references; assertions ship separately as aggregated '
          'edges and a full dump."@en ;')
    w('    rdfs:comment "SKOS is an export format only; no OWL reasoning is applied. '
      'The instance tail (SELF: identifiers) is unnormalized by design — see the export README '
      'for honest WIP metrics."@en .')
    w("")
    # Custom annotation properties
    for prop, label, comment in [
        ("defaultDirection", "default direction convention",
         "Effect-direction convention for a relation type per the extraction prompt "
         "(positive/negative/neutral/unknown/context/unspecified). Individual assertions "
         "still carry their own effect_direction."),
        ("displayType", "curated display type",
         "Curated display type (29 raw -> 20 display) per RELATION_DISPLAY_ALIASES; "
         "the raw relation_type is always preserved."),
        ("assertionCount", "assertion count",
         "Number of assertions in which this entity participates (source or target)."),
        ("canonicalSource", "canonical source",
         "Provenance of the canonical id: mesh (MeSH DescriptorUI/SCR), hgnc (HGNC gene anchor, "
         "v0.2), uniprot (UniProt accession anchor, v0.2), curated_mesh/curated_concept "
         "(human-adjudicated, v0.2), sciai (curated list), cluster (machine cluster of fallback "
         "forms), self (self-canonical fallback, normalized=false)."),
    ]:
        w(f"sciai:{prop} a owl:AnnotationProperty ;")
        w(f"    rdfs:label {_lit(label)} ;")
        w(f"    rdfs:comment {_lit(comment)} .")
        w("")
    # Entity-type concept scheme
    w(f"<{BASE_IRI}/scheme/entity-types> a skos:ConceptScheme ;")
    w('    dcterms:title "SciAI PD Ontology entity types"@en ;')
    w("    skos:hasTopConcept " + ", ".join(_type_iri(t) for t in ENTITY_TYPES) + " .")
    w("")
    for etype, definition in ENTITY_TYPES.items():
        w(f"{_type_iri(etype)} a skos:Concept ;")
        w(f"    skos:inScheme <{BASE_IRI}/scheme/entity-types> ;")
        w(f"    skos:prefLabel {_lit(etype)} ;")
        w(f"    skos:definition {_lit(definition)} .")
        w("")
    # Relation-type concept scheme
    w(f"<{BASE_IRI}/scheme/relation-types> a skos:ConceptScheme ;")
    w('    dcterms:title "SciAI PD Ontology relation types"@en ;')
    if v2:
        w('    rdfs:comment "Vocabulary locked by the 2026-09-13 adjudication. Direction '
          'conventions normalized 2026-09-24: increases/induces/activates -> positive; '
          'decreases/inhibits/damages/treats -> negative (treats is stored as negative: treatment '
          'reduces the condition; a runner-side coercion enforces it, 10,489 historical rows '
          'flipped); associated_with/involves/co_occurs_with -> neutral; studies -> unknown; '
          'regulates -> NEUTRAL (direction-free by design; 44,919 historical signed rows '
          'neutralized — direction is never guessed)."@en ;')
    else:
        w('    rdfs:comment "Vocabulary locked by the 2026-09-13 adjudication (all 29 confirmed '
          'by the owner). Direction conventions: increases/induces/activates -> positive; '
          'decreases/inhibits/damages/treats -> negative (treats is stored as negative: treatment '
          'reduces the condition); associated_with/involves/co_occurs_with -> neutral; '
          'studies -> unknown; regulates -> direction from context."@en ;')
    w("    skos:hasTopConcept " + ", ".join(_relation_iri(r) for r in RELATION_TYPES) + " .")
    w("")
    for rtype, (definition, direction) in RELATION_TYPES.items():
        display = RELATION_DISPLAY_ALIASES.get(rtype, rtype)
        if v2 and rtype == "regulates":
            direction = "neutral"  # direction-free since 2026-09-24 (schema prompts + coercion)
        w(f"{_relation_iri(rtype)} a skos:Concept ;")
        w(f"    skos:inScheme <{BASE_IRI}/scheme/relation-types> ;")
        w(f"    skos:prefLabel {_lit(rtype)} ;")
        w(f"    skos:definition {_lit(definition)} ;")
        w(f'    sciai:defaultDirection "{direction}" ;')
        if display != rtype:
            w(f'    sciai:displayType "{display}" ;')
            w(f'    rdfs:comment "Displays as \'{display}\' in the UI (display curation 29 -> 20); '
              f'the raw type is authoritative."@en ;')
        w(f'    skos:notation "{rtype}" .')
        w("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Database reads
# ---------------------------------------------------------------------------

def gather_stats(conn, scoped: bool = False) -> Dict[str, object]:
    """Frozen counts + data-quality probes (read-only). Scoped = v0.2 PD-only."""
    join = "JOIN tmp_export_scope sc ON sc.pmid = a.pmid" if scoped else ""
    a = "kg.assertions a" if scoped else "kg.assertions"
    stats: Dict[str, object] = {}
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {a} {join}")
        stats["n_assertions"] = cur.fetchone()[0]
        cur.execute(f"SELECT count(DISTINCT a.pmid) FROM {a} {join}")
        stats["n_pmids"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM kg.edge_summaries")
        stats["n_edges_view"] = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT canonical_id) FROM kg.entity_synonyms WHERE canonical_id IS NOT NULL")
        stats["n_canonicals_registered"] = cur.fetchone()[0]
        cur.execute(f"""
            WITH used AS (
                SELECT a.source_canonical_id AS cid FROM {a} {join}
                UNION SELECT a.target_canonical_id FROM {a} {join}
            ) SELECT count(*) FROM used
        """)
        stats["n_canonicals_used"] = cur.fetchone()[0]
        cur.execute(f"""
            WITH used AS (
                SELECT a.source_canonical_id AS cid FROM {a} {join}
                UNION SELECT a.target_canonical_id FROM {a} {join}
            )
            SELECT count(*) FROM used u
            LEFT JOIN kg.entity_synonyms s ON s.canonical_id = u.cid
            WHERE s.canonical_id IS NULL
        """)
        stats["n_orphan_canonicals"] = cur.fetchone()[0]
        cur.execute(f"SELECT a.effect_direction, count(*) FROM {a} {join} GROUP BY 1")
        stats["direction_counts"] = {str(k): v for k, v in cur.fetchall()}
        cur.execute(f"SELECT a.relation_type, count(*) FROM {a} {join} GROUP BY 1")
        stats["relation_counts"] = dict(cur.fetchall())
        cur.execute(f"SELECT a.source_schema, count(*) FROM {a} {join} GROUP BY 1")
        stats["schema_counts"] = {str(k): v for k, v in cur.fetchall()}
        cur.execute(f"SELECT a.normalized, count(*) FROM {a} {join} GROUP BY 1")
        stats["normalized_counts"] = {str(k): v for k, v in cur.fetchall()}
        cur.execute(f"SELECT count(*) FROM {a} {join} WHERE a.effect_direction IS NULL")
        stats["n_null_direction"] = cur.fetchone()[0]
        if scoped:
            cur.execute("""
                SELECT s.canonical_source, count(DISTINCT s.canonical_id)
                FROM kg.entity_synonyms s
                WHERE s.canonical_id IN (
                    SELECT a.source_canonical_id FROM kg.assertions a
                    JOIN tmp_export_scope sc ON sc.pmid = a.pmid
                    UNION
                    SELECT a.target_canonical_id FROM kg.assertions a
                    JOIN tmp_export_scope sc ON sc.pmid = a.pmid)
                GROUP BY 1""")
            stats["source_shares"] = dict(cur.fetchall())
    unknown_relations = sorted(set(stats["relation_counts"]) - set(RELATION_TYPES))
    stats["relations_outside_vocabulary"] = unknown_relations
    if unknown_relations:
        logger.warning("Relation types outside the locked 29: %s", unknown_relations)
    return stats


def load_synonyms(conn) -> Dict[str, List[str]]:
    """canonical_id -> all registered surface forms (kg.entity_synonyms).

    authority_alias rows (preserved loser authority codes from code-preserving
    merges) are NOT surface forms; they are exported as skos:exactMatch via
    load_authority_aliases instead of polluting altLabel."""
    synonyms: Dict[str, List[str]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT canonical_id, surface_form FROM kg.entity_synonyms
            WHERE canonical_id IS NOT NULL
              AND (canonical_source IS NULL OR canonical_source <> 'authority_alias')
            ORDER BY canonical_id, synonym_id
        """)
        for cid, form in cur:
            synonyms[cid].append(form)
    return synonyms


def load_authority_aliases(conn) -> Dict[str, List[str]]:
    """winner canonical_id -> preserved loser authority codes (authority_alias rows)."""
    aliases: Dict[str, List[str]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT canonical_id, surface_form FROM kg.entity_synonyms
            WHERE canonical_source = 'authority_alias' AND canonical_id IS NOT NULL
            ORDER BY canonical_id, synonym_id
        """)
        for cid, code in cur:
            aliases[cid].append(code)
    return aliases


def authority_alias_iri(code: str) -> Optional[str]:
    """Authority IRI for a preserved alias code, or None when the namespace has none."""
    import re as _re
    if _re.fullmatch(r"[DC]\d+", code):  # MeSH descriptor/SCR id (CLU:/C-codes stay distinct)
        return f"http://id.nlm.nih.gov/mesh/{quote(code, safe='')}"
    if code.startswith("HGNC:"):
        return f"https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/{quote(code, safe='')}"
    import kg_normalization_lib as _knl  # local: keep module import light
    if _knl.UNIPROT_ACCESSION_RE.fullmatch(code):
        return f"https://www.uniprot.org/uniprotkb/{quote(code, safe='')}"
    return None


def load_assertion_counts(conn, scoped: bool = False) -> Counter:
    """canonical_id -> number of assertions it participates in (scoped if v0.2)."""
    join = "JOIN tmp_export_scope sc ON sc.pmid = a.pmid" if scoped else ""
    counts: Counter = Counter()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT cid, sum(n) FROM (
                SELECT a.source_canonical_id AS cid, count(*) AS n
                FROM kg.assertions a {join} GROUP BY 1
                UNION ALL
                SELECT a.target_canonical_id, count(*)
                FROM kg.assertions a {join} GROUP BY 1
            ) t GROUP BY cid
        """)
        for cid, n in cur:
            counts[cid] = int(n)
    return counts


def load_orphan_ids(conn, scoped: bool = False) -> List[str]:
    """Canonical ids used in (scoped) assertions but absent from kg.entity_synonyms.

    All observed orphans are SELF: fallback ids created at populate time and
    never registered (extend_entity_synonyms registers only a subset)."""
    join = "JOIN tmp_export_scope sc ON sc.pmid = a.pmid" if scoped else ""
    a = "kg.assertions a" if scoped else "kg.assertions"
    with conn.cursor() as cur:
        cur.execute(f"""
            WITH used AS (
                SELECT a.source_canonical_id AS cid FROM {a} {join}
                UNION SELECT a.target_canonical_id FROM {a} {join}
            )
            SELECT u.cid FROM used u
            LEFT JOIN kg.entity_synonyms s ON s.canonical_id = u.cid
            WHERE s.canonical_id IS NULL ORDER BY u.cid
        """)
        return [r[0] for r in cur]


def _canonical_source(canonical_id: str) -> str:
    if canonical_id.startswith("SELF:"):
        return "self"
    if canonical_id.startswith("CLU:"):
        return "cluster"
    if canonical_id.startswith("SCIAI:"):
        return "sciai"
    if canonical_id.startswith("HGNC:"):
        return "hgnc"
    if canonical_id.startswith(("D", "C")):
        return "mesh"
    import kg_normalization_lib as _knl  # local: keep module import light
    if _knl.UNIPROT_ACCESSION_RE.fullmatch(canonical_id):
        return "uniprot"
    return "sciai"


# ---------------------------------------------------------------------------
# Artifact writers
# ---------------------------------------------------------------------------

def write_entities(path: Path, entity_type: Dict[str, str], preferred_name: Dict[str, str],
                   synonyms: Dict[str, List[str]], assertion_counts: Counter,
                   orphan_ids: List[str], used_only: bool = False,
                   aliases: Optional[Dict[str, List[str]]] = None) -> int:
    """Write canonicals as SKOS concepts (gzipped Turtle). Returns count.

    used_only (v0.2 PD-only): only canonicals used in the scoped assertions
    are exported (plus the scoped orphan SELF ids).
    aliases (code-preserving merges): winner -> preserved loser authority
    codes, emitted as additional skos:exactMatch IRIs so BOTH authority codes
    survive the merge in the export."""
    n = 0
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n")
        fh.write("@prefix dcterms: <http://purl.org/dc/terms/> .\n")
        fh.write(f"@prefix sciai: <{PROP_IRI}/> .\n\n")
        fh.write(f"# SciAI PD Ontology v{VERSION} entity instances — {EXPORT_DATE}\n")
        fh.write("# SKOS concepts: prefLabel = preferred name, altLabel = surface forms,\n")
        fh.write("# exactMatch = MeSH cross-reference, broader = entity type.\n\n")
        if used_only:
            ordered = [c for c in sorted(synonyms.keys()) if assertion_counts.get(c)] + orphan_ids
        else:
            ordered = sorted(synonyms.keys()) + orphan_ids
        for cid in ordered:
            forms = synonyms.get(cid, [])
            pref = preferred_name.get(cid)
            if pref is None:
                # Orphan SELF id: the id embeds the folded surface form (lossy:
                # case/punctuation were folded away). Documented in README.
                pref = cid.split(":", 1)[1] if ":" in cid else cid
            etype = entity_type.get(cid, "Concept")
            lines = [f"{_entity_iri(cid)} a skos:Concept ;"]
            lines.append(f"    skos:prefLabel {_lit(pref)} ;")
            alts = sorted({f for f in forms if f != pref})
            for alt in alts:
                lines.append(f"    skos:altLabel {_lit(alt)} ;")
            if _canonical_source(cid) == "mesh":
                lines.append(f"    skos:exactMatch <http://id.nlm.nih.gov/mesh/{quote(cid, safe='')}> ;")
            for alias_code in (aliases or {}).get(cid, []):
                alias_iri = authority_alias_iri(alias_code)
                if alias_iri:
                    lines.append(f"    skos:exactMatch <{alias_iri}> ;")
            lines.append(f"    skos:broader {_type_iri(etype)} ;")
            lines.append(f'    sciai:canonicalSource "{_canonical_source(cid)}" ;')
            lines.append(f"    sciai:assertionCount {assertion_counts.get(cid, 0)} ;")
            lines.append(f"    dcterms:identifier \"{_esc(cid)}\" .")
            fh.write("\n".join(lines) + "\n\n")
            n += 1
    return n


EDGE_SQL = """
    SELECT
        source_canonical_id, relation_type, target_canonical_id,
        count(DISTINCT pmid)                                  AS n_papers,
        count(*)                                              AS n_assertions,
        count(*) FILTER (WHERE effect_direction = 'positive') AS n_positive,
        count(*) FILTER (WHERE effect_direction = 'negative') AS n_negative,
        count(*) FILTER (WHERE effect_direction = 'neutral')  AS n_neutral,
        count(*) FILTER (WHERE effect_direction = 'unknown')  AS n_unknown,
        count(*) FILTER (WHERE evidence_tier = 1)             AS n_tier1,
        count(*) FILTER (WHERE evidence_tier = 2)             AS n_tier2,
        count(*) FILTER (WHERE evidence_tier = 3)             AS n_tier3,
        min(year)                                             AS year_min,
        max(year)                                             AS year_max,
        avg(confidence)                                       AS avg_confidence,
        max(confidence)                                       AS max_confidence
    FROM kg.assertions
    GROUP BY 1, 2, 3
"""

EDGE_COLUMNS = [
    "source_canonical_id", "source_name", "source_type",
    "relation_type",
    "target_canonical_id", "target_name", "target_type",
    "n_papers", "n_assertions",
    "n_positive", "n_negative", "n_neutral", "n_unknown",
    "n_tier1", "n_tier2", "n_tier3",
    "year_min", "year_max", "avg_confidence", "max_confidence",
]


def edge_sql(scoped: bool = False) -> str:
    join = "JOIN tmp_export_scope sc ON sc.pmid = a.pmid" if scoped else ""
    a = "kg.assertions a" if scoped else "kg.assertions"
    return f"""
    SELECT
        a.source_canonical_id, a.relation_type, a.target_canonical_id,
        count(DISTINCT a.pmid)                                AS n_papers,
        count(*)                                              AS n_assertions,
        count(*) FILTER (WHERE a.effect_direction = 'positive') AS n_positive,
        count(*) FILTER (WHERE a.effect_direction = 'negative') AS n_negative,
        count(*) FILTER (WHERE a.effect_direction = 'neutral')  AS n_neutral,
        count(*) FILTER (WHERE a.effect_direction = 'unknown')  AS n_unknown,
        count(*) FILTER (WHERE a.evidence_tier = 1)             AS n_tier1,
        count(*) FILTER (WHERE a.evidence_tier = 2)             AS n_tier2,
        count(*) FILTER (WHERE a.evidence_tier = 3)             AS n_tier3,
        min(a.year)                                             AS year_min,
        max(a.year)                                             AS year_max,
        avg(a.confidence)                                       AS avg_confidence,
        max(a.confidence)                                       AS max_confidence
    FROM {a} {join}
    GROUP BY 1, 2, 3
"""


def write_edges(conn, path: Path, entity_type: Dict[str, str], preferred_name: Dict[str, str],
                scoped: bool = False) -> int:
    """Aggregate kg.assertions -> edges CSV (gz). Same derivation as the
    kg.edge_summaries view plus n_unknown (the view lacks it). Scoped = v0.2."""
    n = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(EDGE_COLUMNS)
        with conn.cursor(name="edge_export_stream") as cur:
            cur.execute(edge_sql(scoped))
            while True:
                rows = cur.fetchmany(10000)
                if not rows:
                    break
                for (src, rt, tgt, n_papers, n_assert, n_pos, n_neg, n_neu, n_unk,
                     t1, t2, t3, y_min, y_max, avg_conf, max_conf) in rows:
                    writer.writerow([
                        src, preferred_name.get(src, src.split(":", 1)[-1]), entity_type.get(src, "Concept"),
                        rt,
                        tgt, preferred_name.get(tgt, tgt.split(":", 1)[-1]), entity_type.get(tgt, "Concept"),
                        n_papers, n_assert, n_pos, n_neg, n_neu, n_unk, t1, t2, t3,
                        y_min if y_min is not None else "", y_max if y_max is not None else "",
                        f"{avg_conf:.4f}" if avg_conf is not None else "",
                        f"{max_conf:.4f}" if max_conf is not None else "",
                    ])
                    n += 1
    return n


DUMP_SQL = """
    SELECT assertion_id, source_canonical_id, relation_type, target_canonical_id,
           pmid, paper_type, species, effect_direction, context, confidence,
           evidence_tier, year, sample_size, p_value, normalized, source_schema, created_at
    FROM kg.assertions
"""

DUMP_COLUMNS = [
    "assertion_id", "source_canonical_id", "relation_type", "target_canonical_id",
    "pmid", "paper_type", "species", "effect_direction", "context", "confidence",
    "evidence_tier", "year", "sample_size", "p_value", "normalized", "source_schema", "created_at",
]


def dump_sql(scoped: bool = False) -> str:
    join = "JOIN tmp_export_scope sc ON sc.pmid = a.pmid" if scoped else ""
    a = "kg.assertions a" if scoped else "kg.assertions"
    return f"""
    SELECT a.assertion_id, a.source_canonical_id, a.relation_type, a.target_canonical_id,
           a.pmid, a.paper_type, a.species, a.effect_direction, a.context, a.confidence,
           a.evidence_tier, a.year, a.sample_size, a.p_value, a.normalized, a.source_schema,
           a.created_at
    FROM {a} {join}
"""


def write_full_dump(conn, path: Path, scoped: bool = False) -> int:
    n = 0
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        with conn.cursor(name="assertion_dump_stream") as cur:
            cur.execute(dump_sql(scoped))
            while True:
                rows = cur.fetchmany(20000)
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(DUMP_COLUMNS, row))
                    if record["created_at"] is not None:
                        record["created_at"] = record["created_at"].isoformat()
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    n += 1
    return n


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def license_header() -> str:
    return f"""\
SciAI Parkinson's Disease Ontology v{VERSION} and associated data exports
(vocabulary, entity concepts, aggregated edges, assertion dump)

Copyright (c) 2026 sciai-lab / Johannes Keizer

This work is licensed under the Creative Commons
Attribution-NonCommercial 4.0 International Public License (CC BY-NC 4.0):
https://creativecommons.org/licenses/by-nc/4.0/

Attribution: sciai-lab / Johannes Keizer — SciAI PD Ontology v{VERSION}
(https://pd.sciai.eu)

You are free to share and adapt this material for NonCommercial purposes
only, under the terms below. The full legal code follows.

=======================================================================

"""


V02_SCOPE_NOTE = """\
## Scope: PD-only (new in v0.2)

v0.1 shipped the full harvest, including the esophagus/PPI pilot topics
(~21% of assertions were on non-PD-titled papers). Per the owner decision of
2026-10-06, v0.2 is **PD-only**: a paper is in scope iff it passes the
platform's production PD topic guard — the same predicate the corpus builder
uses (`sciai_core.topics`, strength "strong": 'parkinson' in title or
abstract, or MeSH descriptor 'Parkinson Disease') — OR it mentions a proton
pump inhibitor substance (keywords of the 'ppi' harvest topic: omeprazole,
esomeprazole, lansoprazole, pantoprazole, rabeprazole, dexlansoprazole,
"proton pump inhibitor") AND levodopa/L-DOPA (the ratified exception: PPIs as
substances lowering levodopa efficiency). Entity concepts in this export are
the canonicals used in the scoped assertions.
"""


def write_readme(path: Path, stats: Dict[str, object], files: Dict[str, Dict[str, object]],
                 n_entities: int, n_edges: int, n_dump: Optional[int],
                 scope_stats: Optional[Dict[str, int]] = None,
                 draft: bool = False) -> None:
    v2 = VERSION == "0.2"
    dq = []
    if stats["n_orphan_canonicals"]:
        dq.append(
            f"- **Orphan canonicals:** {stats['n_orphan_canonicals']:,} canonical ids used in assertions "
            "have no row in `kg.entity_synonyms`. All are `SELF:` self-canonical fallback ids created at "
            "populate time and never registered. They are exported as SKOS concepts anyway (type `Concept`, "
            "label = the folded surface form embedded in the id — lossy: case and punctuation were folded "
            "away), so every edge endpoint resolves to an entity record."
        )
    if stats["n_null_direction"]:
        dq.append(f"- **Missing direction:** {stats['n_null_direction']:,} assertions have NULL effect_direction.")
    else:
        dq.append("- **Missing direction:** none — every assertion carries an effect_direction value.")
    if stats["relations_outside_vocabulary"]:
        dq.append(f"- **Vocabulary violation:** relation types outside the locked 29: "
                  f"{stats['relations_outside_vocabulary']}.")
    else:
        dq.append("- **Relation vocabulary:** all assertion relation_types are within the locked 29. "
                  "`other` is in the vocabulary but carries zero assertions by design (skipped at load time).")
    dq.append(
        "- **`kg.edge_summaries` view gap:** the view counts n_positive/n_negative/n_neutral but has no "
        f"n_unknown column; {stats['direction_counts'].get('unknown', 0):,} assertions carry "
        "effect_direction='unknown'. The edges CSV in this export adds n_unknown."
    )
    if not v2:
        dq.append(
            "- **New-type coverage:** the 5 entity types ratified 2026-09-12 (Neurotransmitter, Assay, "
            "EnvironmentalFactor, CellComponent, StudyDesign) are still rare in `kg.entity_synonyms` "
            "(179/51/19/22/27 rows) — pre-2026-09-13 extractions used the 16-type vocabulary; the tail "
            "re-typing happens through curation rounds."
        )

    rows = "\n".join(
        f"| `{name}` | {meta['bytes']:,} | `{meta['sha256'][:16]}…` | {meta['description']} |"
        for name, meta in files.items()
    )
    scope_block = ""
    if v2 and scope_stats:
        scope_block = (V02_SCOPE_NOTE + f"""
Measured scope: {scope_stats['papers_scope']:,} papers in scope of
{scope_stats['papers_total']:,} harvested ({scope_stats['papers_pd_guard']:,} by the PD guard,
{scope_stats['papers_ppi_exception']:,} added by the PPI×levodopa exception);
{stats['n_assertions']:,} assertions on {stats['n_pmids']:,} of them ship here.
""")
    citation_block = ""
    if v2:
        citation_block = """
## Citation

Keizer, J. (sciai-lab). *SciAI PD Ontology*. Zenodo. Concept DOI (all
versions): https://doi.org/10.5281/zenodo.22759155 · v0.1 version DOI:
https://doi.org/10.5281/zenodo.22759156. BioPortal: acronym **PDOSC**
(https://bioportal.bioontology.org/ontologies/PDOSC).
"""
    draft_block = ("\n> **DRAFT** — prepared ahead of the release date for owner review; "
                   "counts are re-frozen at release.\n" if draft else "")
    text = f"""# SciAI PD Ontology v{VERSION} — export of {EXPORT_DATE}
{draft_block}
Work-in-progress ontology of Parkinson's disease research, extracted from the
PubMed literature by the SciAI platform. **License: CC BY-NC 4.0** (see
`LICENSE`); attribution: **sciai-lab / Johannes Keizer**.
{citation_block}
Schema authority: **{SCHEMA_SOURCE}** — the locked vocabulary of 21 entity
types and 29 relation types, with definitions and direction conventions
(including that `treats` is stored as **negative**: treatment reduces the
condition{"; since 2026-09-24 `regulates` is **direction-free (neutral)** — direction is never guessed" if v2 else ""}).
SKOS is an export format only; no OWL reasoning is applied.

{scope_block}
## Artifacts

| File | Bytes | SHA-256 (prefix) | Contents |
|------|------:|------------------|----------|
{rows}

## Frozen counts (PostgreSQL `sciai.kg`, {EXPORT_DATE})

- Assertions: **{stats['n_assertions']:,}** across **{stats['n_pmids']:,}** PMIDs
  (source schemas: {_fmt_counts(stats['schema_counts'])})
- Aggregated edges: **{n_edges:,}** (subject, relation_type, object)
- Entity concepts exported: **{n_entities:,}**
  ({stats['n_canonicals_registered']:,} registered in `kg.entity_synonyms`
  + {stats['n_orphan_canonicals']:,} orphan SELF ids used in assertions)
- Entity types in vocabulary: **{len(ENTITY_TYPES)}**; relation types: **{len(RELATION_TYPES)}**
  ({len(stats['relation_counts'])} present in data — `other` carries zero assertions by design)
- Direction split of assertions: {_fmt_counts(stats['direction_counts'])}
- Assertions fully authority-mapped (normalized=true): {stats['normalized_counts'].get('True', 0):,};
  with self-canonical/cluster fallback (normalized=false): {stats['normalized_counts'].get('False', 0):,}

## Data model

- `pd_ontology_v{VERSION}.ttl` — the vocabulary: both concept schemes (entity types,
  relation types) with definitions, direction conventions
  (`sciai:defaultDirection`), and the display-curation map (29 raw → 20 display,
  `sciai:displayType`).
- `pd_ontology_entities_v{VERSION}.ttl.gz` — entity instances as SKOS concepts:
  `skos:prefLabel` (preferred name), `skos:altLabel` (surface forms),
  `skos:exactMatch` → `http://id.nlm.nih.gov/mesh/<UI>` for MeSH-mapped
  canonicals, `skos:broader` → the entity type, `sciai:canonicalSource`
  (mesh/sciai/cluster/self), `sciai:assertionCount`. Entity IRIs:
  `{BASE_IRI}/entity/<percent-encoded canonical_id>`.
- `pd_ontology_edges_v{VERSION}.csv.gz` — **primary artifact**: one row per
  (source, relation_type, target) with `n_papers`, `n_assertions` (n_instances),
  the direction split (`n_positive`/`n_negative`/`n_neutral`/`n_unknown`),
  evidence-tier counts, year range, and avg/max confidence. This carries the
  contest signal: an edge with both high n_positive and high n_negative is
  contested in the literature. Same derivation as the `kg.edge_summaries` view
  plus `n_unknown` (which the view lacks).
{f"- `pd_ontology_assertions_v{VERSION}.jsonl.gz` — full assertion dump ({n_dump:,} rows), one JSON object per line with per-PMID provenance (columns: {', '.join(DUMP_COLUMNS)})." if n_dump is not None else "- Full assertion dump: not generated in this run (rerun with `--full-dump`)."}

## Entity fit check (SKOS vs CSV/JSONL) — decision: SKOS

The canonical-entity model maps cleanly onto SKOS: name → `skos:prefLabel`,
synonyms → `skos:altLabel`, MeSH id → `skos:exactMatch`, type membership →
`skos:broader` into the entity-type scheme. The only lossy corner is the
`SELF:` fallback tail (label recovered from the folded id), which is a data
caveat, not a format mismatch. Entities therefore ship as gzipped SKOS Turtle.

## Data-quality notes

{chr(10).join(dq)}

## Provenance

Generated by `maintenance/export_ontology_v01.py` from PostgreSQL `sciai`
(schema `kg`), {EXPORT_DATE}. Entity typing follows the ratified authority rule
(MeSH tree location → confidence-weighted surface-form vote → curated owner
pins), the same rule used by the Neo4j ingest. Contact: sciai-lab /
Johannes Keizer — https://pd.sciai.eu
"""
    path.write_text(text, encoding="utf-8")


def _fmt_counts(counts: Dict[str, int]) -> str:
    return ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items(), key=lambda kv: -kv[1]))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global VERSION, EXPORT_DATE
    ap = argparse.ArgumentParser(description="Export the PD Ontology public artifacts from PostgreSQL kg.*")
    ap.add_argument("--version", choices=sorted(VERSIONS), default="0.1",
                    help="Export version (default 0.1 — reproduces the v0.1 artifact set)")
    ap.add_argument("--pd-only", action="store_true",
                    help="PD-only scope filter (required and implied for v0.2; forbidden for v0.1)")
    ap.add_argument("--draft", action="store_true",
                    help="Mark manifest/README as DRAFT (pre-release preparation runs)")
    ap.add_argument("--export-date", default=None, help="Override the frozen export date")
    ap.add_argument("--out", type=Path, default=None, help="Output directory (default per version)")
    ap.add_argument("--dry-run", action="store_true", help="Counts only; write no files")
    ap.add_argument("--full-dump", action="store_true",
                    help="Also export the full assertion dump (gzipped JSONL)")
    args = ap.parse_args()

    spec = VERSIONS[args.version]
    VERSION = args.version
    EXPORT_DATE = args.export_date or spec["export_date"]
    scoped = args.pd_only or spec["pd_only"]
    if scoped and VERSION == "0.1":
        ap.error("--pd-only is a v0.2 mode; v0.1 is the full-scope export")
    out = args.out or spec["out"]

    dsn = get_dsn()
    with psycopg.connect(dsn) as conn:
        scope_stats = None
        if scoped:
            logger.info("Building PD-only scope (production PD guard + PPI×levodopa exception)")
            scope_stats = create_scope_table(conn)
            logger.info("Scope: %s", scope_stats)
        logger.info("Connected; gathering frozen counts and data-quality probes")
        stats = gather_stats(conn, scoped=scoped)
        logger.info("Frozen counts: %s", json.dumps(stats, default=str))
        for key in ("n_assertions", "n_pmids", "n_edges_view", "n_canonicals_registered",
                    "n_canonicals_used", "n_orphan_canonicals"):
            logger.info("  %s = %s", key, f"{stats[key]:,}")
        logger.info("  direction split: %s", stats["direction_counts"])
        logger.info("  relation types in data: %d (of 29 locked)", len(stats["relation_counts"]))
        if scoped and stats.get("source_shares"):
            logger.info("  canonical-source shares (used canonicals): %s", stats["source_shares"])

        if args.dry_run:
            logger.info("Dry run — no files written.")
            return

        out.mkdir(parents=True, exist_ok=True)
        files: Dict[str, Dict[str, object]] = {}

        # 1. Vocabulary
        vocab_path = out / f"pd_ontology_v{VERSION}.ttl"
        vocab_path.write_text(build_vocabulary_ttl(), encoding="utf-8")
        files[vocab_path.name] = {"description": "SKOS vocabulary (21 entity types, 29 relation types)"}
        logger.info("Wrote %s", vocab_path)

        # 2. Entities
        logger.info("Loading entity metadata (authority rule), synonyms, assertion counts")
        entity_type, preferred_name = _load_entity_metadata(conn)
        synonyms = load_synonyms(conn)
        aliases = load_authority_aliases(conn)
        assertion_counts = load_assertion_counts(conn, scoped=scoped)
        orphan_ids = load_orphan_ids(conn, scoped=scoped)
        logger.info("Registered canonicals: %d; orphan SELF ids used in assertions: %d",
                    len(synonyms), len(orphan_ids))
        entities_path = out / f"pd_ontology_entities_v{VERSION}.ttl.gz"
        n_entities = write_entities(entities_path, entity_type, preferred_name,
                                    synonyms, assertion_counts, orphan_ids, used_only=scoped,
                                    aliases=aliases)
        files[entities_path.name] = {"description": f"Entity instances as SKOS concepts ({n_entities:,})"}
        logger.info("Wrote %s (%d concepts)", entities_path, n_entities)

        # 3. Aggregated edges (primary artifact)
        edges_path = out / f"pd_ontology_edges_v{VERSION}.csv.gz"
        n_edges = write_edges(conn, edges_path, entity_type, preferred_name, scoped=scoped)
        files[edges_path.name] = {"description": f"Aggregated edges, primary artifact ({n_edges:,})"}
        logger.info("Wrote %s (%d edges)", edges_path, n_edges)

        # 4. Full assertion dump (optional)
        n_dump: Optional[int] = None
        if args.full_dump:
            dump_path = out / f"pd_ontology_assertions_v{VERSION}.jsonl.gz"
            n_dump = write_full_dump(conn, dump_path, scoped=scoped)
            files[dump_path.name] = {"description": f"Full assertion dump ({n_dump:,} rows)"}
            logger.info("Wrote %s (%d assertions)", dump_path, n_dump)

    # 5. LICENSE (CC BY-NC 4.0 legalcode, verbatim from creativecommons.org)
    license_path = out / "LICENSE"
    license_text = license_header() + (Path(__file__).parent / "cc_by_nc_4.0_legalcode.txt").read_text(encoding="utf-8")
    license_path.write_text(license_text, encoding="utf-8")
    files[license_path.name] = {"description": "CC BY-NC 4.0 legalcode + attribution"}
    logger.info("Wrote %s", license_path)

    # 6. Hashes, README, manifest, CHANGELOG
    for name, meta in files.items():
        p = out / name
        meta["bytes"] = p.stat().st_size
        meta["sha256"] = _sha256(p)
    readme_path = out / "README.md"
    write_readme(readme_path, stats, files, n_entities, n_edges, n_dump,
                 scope_stats=scope_stats, draft=args.draft)
    files[readme_path.name] = {
        "description": "Artifact manifest, frozen counts, data-quality notes",
        "bytes": readme_path.stat().st_size,
        "sha256": _sha256(readme_path),
    }
    manifest = {
        "ontology": "SciAI PD Ontology",
        "version": VERSION,
        "status": "draft" if args.draft else "release",
        "export_date": EXPORT_DATE,
        "scope": ({"pd_only": True, "rule": "production PD topic guard (strong) OR "
                                            "(PPI-substance keyword AND levodopa/L-DOPA mention)",
                   **scope_stats} if scoped else {"pd_only": False}),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "license": "CC BY-NC 4.0",
        "attribution": "sciai-lab / Johannes Keizer",
        "schema_source": SCHEMA_SOURCE,
        "zenodo_concept_doi": "10.5281/zenodo.22759155",
        "bioportal_acronym": "PDOSC",
        "frozen_counts": {
            "assertions": stats["n_assertions"],
            "pmids": stats["n_pmids"],
            "edges": n_edges,
            "entity_concepts": n_entities,
            "canonicals_registered": stats["n_canonicals_registered"],
            "orphan_self_ids": stats["n_orphan_canonicals"],
            "entity_types": len(ENTITY_TYPES),
            "relation_types_vocabulary": len(RELATION_TYPES),
            "relation_types_in_data": len(stats["relation_counts"]),
            "direction_split": stats["direction_counts"],
        },
        "files": files,
    }
    if scoped and stats.get("source_shares"):
        manifest["frozen_counts"]["canonical_source_shares"] = stats["source_shares"]
    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    if VERSION == "0.2":
        write_changelog_v02(out / "CHANGELOG.md", stats, n_entities, n_edges, scope_stats)
        logger.info("Wrote %s", out / "CHANGELOG.md")
    logger.info("Wrote %s and %s", readme_path, manifest_path)
    logger.info("Export complete -> %s", out)


CHANGELOG_V02 = """# Changelog

## v0.2 — {date} (PD-only scope)

- **Scope (jk 2026-10-06): PD-only.** Assertions/edges now cover only papers
  passing the platform's production PD topic guard ('parkinson' in title or
  abstract, or MeSH 'Parkinson Disease'), plus the ratified exception:
  PPI-substance papers that also mention levodopa/L-DOPA. The v0.1 pilot
  topics (esophagus, generic PPI adverse-effects) are excluded. Scope rule is
  documented in the README.
- **Authority anchoring landed** (2026-09-29/30): genes anchor to HGNC ids
  (3,944 canonicals), proteins to UniProt accessions (3,702 canonicals),
  chemicals consolidated against MeSH incl. CAS-name widening (2026-10-01).
  Canonical ids in `hgnc`/`uniprot` namespaces are new; `canonicalSource`
  values gained `hgnc`, `uniprot`, `curated_mesh`, `curated_concept`.
- **Direction conventions normalized** (2026-09-24): `treats` is enforced
  negative (treatment reduces the condition; 10,489 historical rows flipped);
  `regulates` is direction-free (neutral; 44,919 historical rows
  neutralized). Direction is never guessed anymore.
- **Five adjudication rounds applied**: term-review (293 groups), chemical
  fuzzy batch (173), LLM canonicalization pilot (200), arbitration (6),
  merge round (44 groups incl. the α-synuclein family structure).
- kg.assertions ≈ {n_assertions:,} rows ({n_pmids:,} PMIDs, in scope).
  Entities: {n_entities:,} concepts. Edges: {n_edges:,}.
- Neo4j/PostgreSQL consistency: the graph is an exact mirror of
  kg.edge_summaries (assertion-level reconciliation rounds after every
  anchoring step).
"""


def write_changelog_v02(path: Path, stats: Dict[str, object], n_entities: int, n_edges: int,
                        scope_stats: Optional[Dict[str, int]]) -> None:
    path.write_text(CHANGELOG_V02.format(
        date=EXPORT_DATE, n_assertions=stats["n_assertions"], n_pmids=stats["n_pmids"],
        n_entities=n_entities, n_edges=n_edges), encoding="utf-8")


if __name__ == "__main__":
    main()
