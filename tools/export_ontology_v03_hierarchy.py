#!/usr/bin/env python3
"""PD Ontology v0.3 hierarchy export — workstream A1+A2 (dry-run build, 2026-10-13).

Plan: Planning/20261013_ontology_v03_plan.md §2 (workstream A). This script is
the DRY-RUN builder for the announced v0.3 hierarchy; it writes ONLY into
--out (default eval_reports/ontology_v03_hierarchy_20261013/) and never
touches /55a11/output/exports/, Zenodo, or BioPortal. Database access is
read-only (the PD-scope temp table is session-local, as in the v0.2 export).

A1 — entity-type top level: the 21 entity types are emitted as SKOS concepts
INSIDE the entities TTL (self-contained tree roots — the v0.2 entities file
referenced type IRIs defined only in the separate vocabulary TTL, which is
why the BioPortal tree view was empty). Every concept gets skos:broader to
its type unless A2 places it deeper.

A2 — MeSH tree mid-level: concepts whose canonical id is a MeSH descriptor
(D-id) with a tree number in the local desc2026 map are placed under their
MeSH tree parent instead of directly under the type node. Intermediate
MeSH-tree nodes are emitted as structural SKOS concepts (they may not be in
our concept inventory). Rules (deterministic):
  * Multi-tree descriptors: the FIRST tree number of the desc2026
    TreeNumberList is used (the "primary" tree, same convention as
    maintenance/mesh_tree_depths.py).
  * Parent chain: strip the last tree-number segment; the descriptor sitting
    at the shortened number is the parent. Prefixes without a descriptor are
    skipped. The chain root (single-segment tree number) is attached to the
    entity type derived from the MeSH tree location
    (kg_normalization_lib.mesh_entity_type; fallback type "Concept").
  * Cycle guard: chains are structurally acyclic (each step shortens the
    tree number); a visited set + depth cap guards against data oddities,
    and a descriptor that would become its own ancestor (multiple nested
    tree numbers) has the self-link removed. All guard hits are counted.
  * MeSH supplementary concepts (C-ids) carry no tree numbers -> A1
    fallback, as do D-ids absent from the local tree map.

Structural nodes reuse the entity IRI namespace (…/entity/<UI>): if such a
descriptor later enters the concept inventory, its IRI does not change.

Everything else about the export (edges CSV, JSONL dump, manifest) is
unchanged vs v0.2 — hierarchy only ADDS triples; those artifacts are not
regenerated in this dry run.

Run:
    /55a11/sciai/.venv/bin/python /55a11/maintenance/export_ontology_v03_hierarchy.py
    /55a11/sciai/.venv/bin/python /55a11/maintenance/export_ontology_v03_hierarchy.py --sample 5000
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import quote

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import psycopg

import export_ontology_v01 as exp
import kg_normalization_lib as knl
from ingest_edge_summaries_to_neo4j import _load_entity_metadata
from sciai_automation_lib import get_dsn, setup_logging

logger = setup_logging("export_ontology_v03_hierarchy",
                       Path("/55a11/logs/export_ontology_v03_hierarchy.log"))
logger.propagate = False

DEFAULT_OUT = Path("/55a11/eval_reports/ontology_v03_hierarchy_20261013")
MAX_CHAIN_DEPTH = 64  # cycle guard cap; real MeSH trees are ≤ ~13 deep

DESCRIPTOR_RE = re.compile(r"^D\d{6,}$")

BroaderTarget = tuple[str, str]  # ("type", entity_type) | ("mesh", descriptor_ui)


# ---------------------------------------------------------------------------
# Pure hierarchy logic (unit-tested, no DB)
# ---------------------------------------------------------------------------

def parent_tree_number(tree: str) -> str | None:
    """Strip the last segment; None for a single-segment (root) tree number."""
    head, sep, _ = tree.rpartition(".")
    return head if sep else None


def build_tree_to_ui(tree_map: dict[str, list[str]]) -> dict[str, str]:
    """tree_number -> descriptor UI. Collisions (should not occur in MeSH)
    resolve to the lexicographically first UI — deterministic."""
    tree_to_ui: dict[str, str] = {}
    for ui in sorted(tree_map):
        for tree in tree_map[ui]:
            tree_to_ui.setdefault(tree, ui)
    return tree_to_ui


def mesh_ancestor_chain(ui: str, tree_map: dict[str, list[str]],
                        tree_to_ui: dict[str, str]) -> tuple[list[str], Counter]:
    """Ancestor descriptor UIs of `ui`, nearest first, via the primary tree.

    Returns (chain, guard_counts). The chain is empty when the descriptor has
    no tree number or sits at a tree root. Guard counters:
    self_link_removed (ui would be its own ancestor), depth_cap_hit."""
    guards: Counter = Counter()
    trees = tree_map.get(ui)
    if not trees:
        return [], guards
    chain: list[str] = []
    seen = {ui}
    tree = trees[0]  # primary tree number: first of the desc2026 TreeNumberList
    depth = 0
    while True:
        parent = parent_tree_number(tree)
        if parent is None or depth >= MAX_CHAIN_DEPTH:
            if depth >= MAX_CHAIN_DEPTH:
                guards["depth_cap_hit"] += 1
            break
        depth += 1
        parent_ui = tree_to_ui.get(parent)
        if parent_ui is not None:
            if parent_ui in seen:
                guards["self_link_removed"] += 1
            else:
                chain.append(parent_ui)
                seen.add(parent_ui)
        tree = parent
    return chain, guards


def plan_hierarchy(concept_ids: list[str], entity_type: dict[str, str],
                   tree_map: dict[str, list[str]],
                   supp_heading_map: dict[str, list[str]],
                   ) -> tuple[dict[str, BroaderTarget], dict[str, dict], Counter]:
    """Compute the broader target of every exported concept + structural nodes.

    Returns (broader_map, structural_nodes, stats):
      broader_map       canonical_id -> ("mesh", parent_ui) | ("type", entity_type)
      structural_nodes  descriptor_ui -> {"broader": BroaderTarget}
      stats             placement counters (mesh_mid_level, fallback_*,
                        guard hits, structural node counts, depth histogram)
    """
    stats: Counter = Counter()
    tree_to_ui = build_tree_to_ui(tree_map)
    exported = set(concept_ids)
    broader_map: dict[str, BroaderTarget] = {}
    chains: dict[str, list[str]] = {}

    for cid in concept_ids:
        etype = entity_type.get(cid, "Concept")
        chain: list[str] = []
        if DESCRIPTOR_RE.match(cid) and cid in tree_map:
            chain, guards = mesh_ancestor_chain(cid, tree_map, tree_to_ui)
            stats.update(guards)
        if chain:
            broader_map[cid] = ("mesh", chain[0])
            chains[cid] = chain
            stats["mesh_mid_level"] += 1
            stats[f"mesh_depth_{min(len(chain), 8)}"] += 1
        else:
            broader_map[cid] = ("type", etype)
            if DESCRIPTOR_RE.match(cid):
                stats["fallback_mesh_descriptor_no_tree"] += 1
            elif cid.startswith("C") and exp._canonical_source(cid) == "mesh":
                stats["fallback_mesh_scr_no_tree"] += 1
            else:
                stats["fallback_non_mesh"] += 1

    # Structural nodes: every ancestor UI not itself exported. Their own
    # chains are computed with the same rule; the chain root attaches to the
    # entity type derived from its MeSH tree location.
    structural: dict[str, dict] = {}
    queue = [ui for chain in chains.values() for ui in chain]
    while queue:
        ui = queue.pop()
        if ui in exported or ui in structural:
            continue
        sub_chain, guards = mesh_ancestor_chain(ui, tree_map, tree_to_ui)
        stats.update(guards)
        if sub_chain:
            structural[ui] = {"broader": ("mesh", sub_chain[0])}
            queue.extend(sub_chain)
            stats["structural_mid"] += 1
        else:
            rtype = knl.mesh_entity_type(ui, tree_map, supp_heading_map) or "Concept"
            structural[ui] = {"broader": ("type", rtype)}
            stats["structural_root"] += 1
    stats["structural_total"] = len(structural)
    stats["concepts_total"] = len(concept_ids)
    stats["type_level_total"] = (stats["fallback_mesh_descriptor_no_tree"]
                                 + stats["fallback_mesh_scr_no_tree"]
                                 + stats["fallback_non_mesh"])
    return broader_map, structural, stats


def load_ui_names() -> dict[str, str]:
    """MeSH descriptor UI -> descriptor name (from the local lookup cache)."""
    names: dict[str, str] = {}
    lookup = knl.load_mesh_lookup()
    for key in sorted(lookup):
        ui, name = lookup[key]
        names.setdefault(ui, name)
    return names


# ---------------------------------------------------------------------------
# Turtle writer
# ---------------------------------------------------------------------------

def _scheme_header() -> list[str]:
    """Prefixes + the entity-type scheme with the 21 type concepts (A1 roots,
    emitted inside the entities TTL so the tree is self-contained)."""
    lines = [
        "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .",
        "@prefix dcterms: <http://purl.org/dc/terms/> .",
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        f"@prefix sciai: <{exp.PROP_IRI}/> .",
        "",
        f"<{exp.BASE_IRI}/scheme/entity-types> a skos:ConceptScheme ;",
        '    dcterms:title "SciAI PD Ontology entity types (hierarchy roots)"@en ;',
        "    skos:hasTopConcept " + ", ".join(exp._type_iri(t) for t in exp.ENTITY_TYPES) + " .",
        "",
        "sciai:structuralNode a owl:AnnotationProperty ;",
        '    rdfs:label "structural MeSH-tree node"@en ;',
        ('    rdfs:comment "True on MeSH-tree intermediate nodes emitted for hierarchy only; '
         'they are not part of the extracted concept inventory."@en .'),
        "",
    ]
    for etype, definition in exp.ENTITY_TYPES.items():
        lines += [
            f"{exp._type_iri(etype)} a skos:Concept ;",
            f"    skos:inScheme <{exp.BASE_IRI}/scheme/entity-types> ;",
            f"    skos:prefLabel {exp._lit(etype)} ;",
            f"    skos:definition {exp._lit(definition)} .",
            "",
        ]
    return lines


def write_entities_hierarchy(path: Path, concept_ids: list[str],
                             entity_type: dict[str, str], preferred_name: dict[str, str],
                             synonyms: dict[str, list[str]], assertion_counts: Counter,
                             broader_map: dict[str, BroaderTarget], structural: dict[str, dict],
                             ui_names: dict[str, str]) -> int:
    """Write the hierarchy entities TTL (gzipped). Returns concept count."""
    n = 0
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(_scheme_header()))
        fh.write("# Structural MeSH-tree nodes (A2 mid-level; not part of the concept inventory)\n\n")
        for ui in sorted(structural):
            kind, target = structural[ui]["broader"]
            broader = exp._type_iri(target) if kind == "type" else exp._entity_iri(target)
            lines = [
                f"{exp._entity_iri(ui)} a skos:Concept ;",
                f"    skos:inScheme <{exp.BASE_IRI}/scheme/entity-types> ;",
                f"    skos:prefLabel {exp._lit(ui_names.get(ui, ui))} ;",
                f"    skos:exactMatch <http://id.nlm.nih.gov/mesh/{ui}> ;",
                f"    skos:broader {broader} ;",
                "    sciai:structuralNode true ;",
                f"    dcterms:identifier \"{ui}\" .",
                "",
            ]
            fh.write("\n".join(lines))
        fh.write("# Entity concepts (A1 type level + A2 MeSH mid-level placement)\n\n")
        for cid in concept_ids:
            forms = synonyms.get(cid, [])
            pref = preferred_name.get(cid) or (cid.split(":", 1)[1] if ":" in cid else cid)
            kind, target = broader_map[cid]
            broader = exp._type_iri(target) if kind == "type" else exp._entity_iri(target)
            lines = [f"{exp._entity_iri(cid)} a skos:Concept ;"]
            lines.append(f"    skos:inScheme <{exp.BASE_IRI}/scheme/entity-types> ;")
            lines.append(f"    skos:prefLabel {exp._lit(pref)} ;")
            for alt in sorted({f for f in forms if f != pref}):
                lines.append(f"    skos:altLabel {exp._lit(alt)} ;")
            if exp._canonical_source(cid) == "mesh":
                lines.append(f"    skos:exactMatch <http://id.nlm.nih.gov/mesh/{quote(cid, safe='')}> ;")
            lines.append(f"    skos:broader {broader} ;")
            lines.append(f'    sciai:canonicalSource "{exp._canonical_source(cid)}" ;')
            lines.append(f"    sciai:assertionCount {assertion_counts.get(cid, 0)} ;")
            lines.append(f"    dcterms:identifier \"{exp._esc(cid)}\" .")
            fh.write("\n".join(lines) + "\n\n")
            n += 1
    return n


# ---------------------------------------------------------------------------
# Validation (rdflib)
# ---------------------------------------------------------------------------

def validate_ttl(path: Path) -> dict[str, object]:
    """rdflib validation: parse, triple/concept counts, dangling broader
    targets, broader-graph cycle check, root count."""
    import rdflib
    from rdflib.namespace import RDF, SKOS

    g = rdflib.Graph()
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        g.parse(fh, format="turtle")
    concepts = set(g.subjects(RDF.type, SKOS.Concept))
    broader: dict[object, object] = {}
    dangling = 0
    for s, o in g.subject_objects(SKOS.broader):
        broader[s] = o
        if o not in concepts:
            dangling += 1
    # cycle check over the broader graph (every node has ≤1 broader)
    cycles = 0
    for start in list(broader):
        seen = set()
        node = start
        while node in broader:
            if node in seen:
                cycles += 1
                break
            seen.add(node)
            node = broader[node]
    type_iris = {rdflib.URIRef(f"{exp.BASE_IRI}/type/{t}") for t in exp.ENTITY_TYPES}
    roots = sum(1 for s, o in broader.items() if o in type_iris)
    structural = set(g.subjects(rdflib.URIRef(f"{exp.PROP_IRI}/structuralNode"), rdflib.Literal(True)))
    return {
        "file": str(path),
        "bytes": path.stat().st_size,
        "rdflib_triples": len(g),
        "skos_concepts": len(concepts),
        "concepts_with_broader": len(broader),
        "broader_to_type_root": roots,
        "structural_nodes": len(structural),
        "dangling_broader_targets": dangling,
        "broader_cycles": cycles,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="v0.3 hierarchy dry-run export (A1 type level + A2 MeSH mid-level)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--sample", type=int, default=None,
                    help="Export only the first N concepts (sorted id order) — quick dev runs")
    ap.add_argument("--export-date", default="2026-10-13")
    args = ap.parse_args()

    dsn = get_dsn()
    with psycopg.connect(dsn) as conn:
        logger.info("Building PD-only scope (same predicate as the v0.2 export)")
        scope_stats = exp.create_scope_table(conn)
        logger.info("Scope: %s", scope_stats)
        logger.info("Loading entity metadata (authority rule), synonyms, assertion counts, orphans")
        entity_type, preferred_name = _load_entity_metadata(conn)
        synonyms = exp.load_synonyms(conn)
        assertion_counts = exp.load_assertion_counts(conn, scoped=True)
        orphan_ids = exp.load_orphan_ids(conn, scoped=True)

    concept_ids = [c for c in sorted(synonyms.keys()) if assertion_counts.get(c)] + orphan_ids
    if args.sample:
        concept_ids = concept_ids[: args.sample]
        logger.info("Sample mode: first %d concepts", len(concept_ids))
    logger.info("Concepts to place: %d", len(concept_ids))

    tree_map = knl.load_mesh_tree_map()
    supp_heading_map = knl.load_mesh_supp_heading_map()
    broader_map, structural, stats = plan_hierarchy(concept_ids, entity_type, tree_map, supp_heading_map)
    logger.info("Placement stats: %s", json.dumps(stats, default=str))

    ui_names = load_ui_names()
    args.out.mkdir(parents=True, exist_ok=True)
    ttl_path = args.out / "pd_ontology_v03_hierarchy_entities.ttl.gz"
    n = write_entities_hierarchy(ttl_path, concept_ids, entity_type, preferred_name,
                                 synonyms, assertion_counts, broader_map, structural, ui_names)
    logger.info("Wrote %s (%d concepts + %d structural nodes)", ttl_path, n, len(structural))

    validation = validate_ttl(ttl_path)
    logger.info("Validation: %s", json.dumps(validation, default=str))

    report = {
        "export_date": args.export_date,
        "sample": args.sample,
        "scope": scope_stats,
        "placement": dict(stats),
        "entities_written": n,
        "validation": validation,
    }
    stats_path = args.out / "hierarchy_stats.json"
    stats_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    logger.info("Wrote %s", stats_path)


if __name__ == "__main__":
    main()
