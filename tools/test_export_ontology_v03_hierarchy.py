#!/usr/bin/env python3
"""Unit tests for export_ontology_v03_hierarchy pure logic (no DB/Neo4j/network).

Run:
    cd /55a11/maintenance && /55a11/sciai/.venv/bin/python -m pytest test_export_ontology_v03_hierarchy.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_ontology_v03_hierarchy as h

# Minimal MeSH fixture:
#   D000001 Levodopa           at D02.092.111        (chemical)
#   D000002 "Chemicals"        at D02.092            (structural mid)
#   D000003 "Organic chemicals" at D02               (structural root)
#   D010300 Parkinson Disease  at C10.228.662.600    (disease)
#   D009422 Nervous System Dis at C10.228.662        (in "inventory")
#   D000010 "C10.228" node     at C10.228            (structural)
#   D000011 "C10" node         at C10                (structural root)
#   D000100 multi-tree with nested self: primary A.1.2, also at A.1
TREE_MAP = {
    "D000001": ["D02.092.111"],
    "D000002": ["D02.092"],
    "D000003": ["D02"],
    "D010300": ["C10.228.662.600"],
    "D009422": ["C10.228.662"],
    "D000010": ["C10.228"],
    "D000011": ["C10"],
    "D000100": ["A.1.2", "A.1"],          # primary's parent chain hits itself
    "D000200": ["Z.9"],                   # root-level tree -> no ancestors
}
ENTITY_TYPE = {
    "D000001": "Compound",
    "D010300": "Disease",
    "D009422": "Disease",
    "C015436": "Compound",                # SCR — no tree numbers
    "D099999": "Compound",                # descriptor absent from the tree map
    "HGNC:18618": "Gene",
    "SELF:foo": "Concept",
}


def test_parent_tree_number():
    assert h.parent_tree_number("C10.228.662.600") == "C10.228.662"
    assert h.parent_tree_number("C10") is None
    assert h.parent_tree_number("D02.092") == "D02"


def test_build_tree_to_ui_deterministic_collision():
    tree_map = {"D000002": ["X.1"], "D000001": ["X.1"]}
    assert h.build_tree_to_ui(tree_map)["X.1"] == "D000001"  # sorted-first wins


def test_ancestor_chain_basic():
    chain, guards = h.mesh_ancestor_chain("D000001", TREE_MAP, h.build_tree_to_ui(TREE_MAP))
    assert chain == ["D000002", "D000003"]
    assert not guards


def test_ancestor_chain_no_tree_and_root():
    t2u = h.build_tree_to_ui(TREE_MAP)
    assert h.mesh_ancestor_chain("D099999", TREE_MAP, t2u)[0] == []
    assert h.mesh_ancestor_chain("D000200", TREE_MAP, t2u)[0] == []  # root tree


def test_ancestor_chain_self_link_removed():
    chain, guards = h.mesh_ancestor_chain("D000100", TREE_MAP, h.build_tree_to_ui(TREE_MAP))
    assert "D000100" not in chain
    assert guards["self_link_removed"] == 1


def test_plan_hierarchy_placement_and_fallbacks():
    concepts = ["D000001", "D010300", "D009422", "C015436", "D099999", "HGNC:18618", "SELF:foo"]
    broader, structural, stats = h.plan_hierarchy(concepts, ENTITY_TYPE, TREE_MAP, {})

    # A2: descriptors with tree numbers sit under their MeSH parent
    assert broader["D000001"] == ("mesh", "D000002")
    assert broader["D010300"] == ("mesh", "D009422")          # parent IS exported
    assert broader["D009422"] == ("mesh", "D000010")          # parent is structural
    # A1 fallbacks
    assert broader["C015436"] == ("type", "Compound")         # SCR: no tree numbers
    assert broader["D099999"] == ("type", "Compound")         # no local tree number
    assert broader["HGNC:18618"] == ("type", "Gene")
    assert broader["SELF:foo"] == ("type", "Concept")
    assert stats["mesh_mid_level"] == 3
    assert stats["fallback_mesh_scr_no_tree"] == 1
    assert stats["fallback_mesh_descriptor_no_tree"] == 1
    assert stats["fallback_non_mesh"] == 2

    # Structural nodes: D000002 (mid, under root D000003), D000003 (root),
    # D000010 (mid), D000011 (root). D009422 is exported -> NOT structural.
    assert "D009422" not in structural
    assert structural["D000002"]["broader"] == ("mesh", "D000003")
    assert structural["D000010"]["broader"] == ("mesh", "D000011")
    assert structural["D000003"]["broader"][0] == "type"
    assert structural["D000011"]["broader"][0] == "type"
    # Root type comes from the MeSH tree location (C10 -> Disease in knl prefixes)
    assert structural["D000011"]["broader"] == ("type", "Disease")
    assert stats["structural_root"] == 2
    assert stats["structural_mid"] == 2
    assert stats["structural_total"] == 4


def test_plan_hierarchy_no_mesh_concepts_means_no_structural():
    broader, structural, stats = h.plan_hierarchy(["HGNC:18618"], ENTITY_TYPE, TREE_MAP, {})
    assert broader["HGNC:18618"] == ("type", "Gene")
    assert structural == {}
    assert stats["mesh_mid_level"] == 0
