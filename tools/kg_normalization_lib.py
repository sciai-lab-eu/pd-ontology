#!/usr/bin/env python3
"""Shared normalization helpers for the KG assertions pipeline (Track B).

Used by extend_entity_synonyms.py and populate_kg_assertions.py.
Implements the ratified normalization priority order (KG workplan
2026-08-06..2026-08-20, section 2):

  1. exact kg.entity_synonyms hit
  2. case/punctuation-folded hit (incl. Greek-letter and singular folding)
  3. MeSH descriptor/entry-term match (parsed from data/mesh/desc2026.xml)
  4. curated PD gene/protein allow-list (from sciai_core.kg.entity_normalizer)
  5. frequency-ranked manual curation queue (artifact, no auto-mapping)
  6. self-canonical fallback -- intentionally NOT implemented: unmapped
     surface forms stay unmapped but must be reported.

This module is dependency-light (stdlib only); the curated allow-lists are
imported lazily so the module also loads without sciai_core on sys.path.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("kg_normalization_lib")

MESH_XML_PATH = Path("/55a11/data/mesh/desc2026.xml")
MESH_CACHE_PATH = Path("/55a11/data/mesh/mesh_lookup_cache.json")
MESH_SUPP_XML_PATH = Path("/55a11/data/mesh/supp2026.xml")
MESH_SUPP_CACHE_PATH = Path("/55a11/data/mesh/mesh_supp_lookup_cache.json")
MESH_TREE_CACHE_PATH = Path("/55a11/data/mesh/mesh_tree_cache.json")
MESH_SUPP_HEADING_CACHE_PATH = Path("/55a11/data/mesh/mesh_supp_headings_cache.json")

# Greek letters commonly used in biomedical surface forms (e.g. α-synuclein).
GREEK_LETTERS = {
    "α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta", "ε": "epsilon",
    "κ": "kappa", "λ": "lambda", "μ": "mu", "τ": "tau", "σ": "sigma",
    "ω": "omega", "φ": "phi", "ρ": "rho", "ν": "nu", "π": "pi",
    "η": "eta", "θ": "theta", "ζ": "zeta", "χ": "chi", "ψ": "psi",
}

_PUNCT_RE = re.compile(r"[-_/\\,;:.()\[\]{}+*/!?]")
_WS_RE = re.compile(r"\s+")
_PARENS_RE = re.compile(r"\([^)]*\)")

# Relation-type folds ratified in the 2026-09-13 vocabulary adjudication
# (eval_reports/relation_type_review_20260913.md). Applied when loading
# kg.assertions so legacy extraction payloads never re-introduce merged types.
RELATION_TYPE_FOLD: Dict[str, str] = {
    "localizes_to": "located_in",
}


def fold(name: str) -> str:
    """Case/punctuation-folded lookup key.

    Lowercases, spells out Greek letters, drops apostrophes (so possessives
    like "Parkinson's" -> "parkinsons"), turns punctuation into spaces and
    collapses whitespace.
    """
    s = unicodedata.normalize("NFC", name.strip().lower())
    for char, spelled in GREEK_LETTERS.items():
        s = s.replace(char, spelled + " ")
    s = s.replace("&", " and ")
    s = s.replace("'", "").replace("’", "").replace("`", "")
    s = _PUNCT_RE.sub(" ", s)
    return _WS_RE.sub(" ", s).strip()


def singularize_last(key: str) -> str:
    """Naive English singularization of the last token of a folded key."""
    if not key:
        return key
    tokens = key.split(" ")
    word = tokens[-1]
    if len(word) > 4 and word.endswith("ies"):
        word = word[:-3] + "y"
    elif len(word) > 3 and re.search(r"(xes|zes|ches|shes)$", word):
        word = word[:-2]
    elif len(word) > 3 and word.endswith("s") and not word.endswith(("ss", "us", "is")):
        word = word[:-1]
    tokens[-1] = word
    return " ".join(tokens)


def lookup_keys(name: str) -> List[str]:
    """Ordered folded-key candidates for a surface form (strategy 2/3 keys)."""
    keys: List[str] = []
    base = fold(name)
    if base:
        keys.append(base)
    # Parenthetical abbreviation form: "Parkinson's Disease (PD)" -> base part.
    if "(" in name:
        stripped = fold(_PARENS_RE.sub(" ", name))
        if stripped:
            keys.append(stripped)
    for key in list(keys):
        singular = singularize_last(key)
        if singular != key:
            keys.append(singular)
    # Deduplicate, preserve order.
    seen = set()
    ordered = []
    for key in keys:
        if key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered


class SynonymResolver:
    """Resolve (surface_form, entity_type) to a canonical_id via kg.entity_synonyms.

    Indexes:
      - exact:  (lower(surface_form), entity_type) -- legacy behaviour, last wins
      - fold:   (folded_key, entity_type) -- unambiguous keys only
      - any:    folded_key across all entity types -- unambiguous keys only
    """

    def __init__(self, rows: Iterable[Tuple[str, str, str]]) -> None:
        self.exact_idx: Dict[Tuple[str, str], str] = {}
        fold_votes: Dict[Tuple[str, str], set] = {}
        any_votes: Dict[str, set] = {}
        for surface_form, entity_type, canonical_id in rows:
            if not canonical_id:
                continue
            self.exact_idx[(surface_form.lower().strip(), entity_type)] = canonical_id
            for key in lookup_keys(surface_form):
                fold_votes.setdefault((key, entity_type), set()).add(canonical_id)
                any_votes.setdefault(key, set()).add(canonical_id)
        # Keep only keys that vote for a single canonical id.
        self.fold_idx = {k: next(iter(v)) for k, v in fold_votes.items() if len(v) == 1}
        self.any_idx = {k: next(iter(v)) for k, v in any_votes.items() if len(v) == 1}
        self.ambiguous_fold_keys = sum(1 for v in fold_votes.values() if len(v) > 1)

    def resolve(self, name: str, entity_type: str) -> Optional[Tuple[str, str]]:
        """Return (canonical_id, strategy) or None."""
        hit = self.exact_idx.get((name.lower().strip(), entity_type))
        if hit:
            return hit, "exact"
        for key in lookup_keys(name):
            hit = self.fold_idx.get((key, entity_type))
            if hit:
                return hit, "fold"
        for key in lookup_keys(name):
            hit = self.any_idx.get(key)
            if hit:
                return hit, "fold_any_type"
        return None


def load_mesh_lookup(
    xml_path: Path = MESH_XML_PATH,
    cache_path: Path = MESH_CACHE_PATH,
) -> Dict[str, Tuple[str, str]]:
    """Return {folded_key: (mesh_ui, descriptor_name)} from MeSH descriptors XML.

    Indexes descriptor names and all entry terms. Keys voting for more than
    one descriptor UI are dropped as ambiguous. The parsed lookup is cached
    as JSON next to the XML; the cache is rebuilt when the XML is newer.
    """
    if cache_path.exists() and cache_path.stat().st_mtime >= xml_path.stat().st_mtime:
        with open(cache_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {k: (v[0], v[1]) for k, v in raw.items()}

    logger.info("Parsing MeSH descriptors from %s (one-off, this is slow)", xml_path)
    votes: Dict[str, set] = {}
    descriptor_names: Dict[str, str] = {}
    descriptor_name_key: Dict[str, str] = {}  # mesh_ui -> folded key of its own name
    context = ET.iterparse(str(xml_path), events=("end",))
    n_records = 0
    for _event, elem in context:
        if elem.tag != "DescriptorRecord":
            continue
        ui_el = elem.find("DescriptorUI")
        name_el = elem.find("DescriptorName/String")
        if ui_el is None or name_el is None or not ui_el.text or not name_el.text:
            elem.clear()
            continue
        mesh_ui = ui_el.text.strip()
        descriptor = name_el.text.strip()
        descriptor_names[mesh_ui] = descriptor
        descriptor_name_key[mesh_ui] = fold(descriptor)
        names = {descriptor}
        for term_string in elem.iter("String"):
            if term_string.text and term_string.text.strip():
                names.add(term_string.text.strip())
        for term in names:
            key = fold(term)
            if key:
                votes.setdefault(key, set()).add(mesh_ui)
        n_records += 1
        elem.clear()
    # Ambiguity resolution: prefer the descriptor whose own name folds to the
    # key (e.g. "dopamine" -> D004298 Dopamine, not descriptors merely listing
    # it as an entry term); drop the key if no unique owner exists.
    result: Dict[str, Tuple[str, str]] = {}
    n_owner = 0
    n_dropped = 0
    for key, uis in votes.items():
        if len(uis) == 1:
            ui = next(iter(uis))
        else:
            owners = [u for u in uis if descriptor_name_key.get(u) == key]
            if len(owners) != 1:
                n_dropped += 1
                continue
            ui = owners[0]
            n_owner += 1
        result[key] = (ui, descriptor_names[ui])
    logger.info(
        "MeSH lookup built: %d descriptors, %d keys (%d via owner-priority, %d ambiguous dropped)",
        n_records, len(result), n_owner, n_dropped,
    )
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump({k: [v[0], v[1]] for k, v in result.items()}, f)
    return result


def load_mesh_supp_lookup(
    xml_path: Path = MESH_SUPP_XML_PATH,
    cache_path: Path = MESH_SUPP_CACHE_PATH,
) -> Dict[str, Tuple[str, str]]:
    """Return {folded_key: (scr_ui, record_name)} from MeSH supplementary concepts XML.

    Supplementary concept records (SCRs, C-id namespace, mostly drugs and
    chemicals) are kept separate from the descriptor (D-id) namespace. Only
    the record name and concept/term strings are indexed -- descriptor names
    inside HeadingMappedToList/PharmacologicalActionList are NOT synonyms of
    the SCR. Same owner-priority ambiguity rule as load_mesh_lookup. Cached
    as JSON next to the XML.
    """
    if cache_path.exists() and cache_path.stat().st_mtime >= xml_path.stat().st_mtime:
        with open(cache_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {k: (v[0], v[1]) for k, v in raw.items()}

    logger.info("Parsing MeSH supplementary concepts from %s (one-off, this is slow)", xml_path)
    votes: Dict[str, set] = {}
    record_names: Dict[str, str] = {}
    record_name_key: Dict[str, str] = {}  # scr_ui -> folded key of its own name
    context = ET.iterparse(str(xml_path), events=("end",))
    n_records = 0
    for _event, elem in context:
        if elem.tag != "SupplementalRecord":
            continue
        ui_el = elem.find("SupplementalRecordUI")
        name_el = elem.find("SupplementalRecordName/String")
        if ui_el is None or name_el is None or not ui_el.text or not name_el.text:
            elem.clear()
            continue
        scr_ui = ui_el.text.strip()
        record_name = name_el.text.strip()
        record_names[scr_ui] = record_name
        record_name_key[scr_ui] = fold(record_name)
        names = {record_name}
        # Only concept terms are true synonyms of the SCR.
        for term_string in elem.findall("ConceptList/Concept/TermList/Term/String"):
            if term_string.text and term_string.text.strip():
                names.add(term_string.text.strip())
        for term in names:
            key = fold(term)
            if key:
                votes.setdefault(key, set()).add(scr_ui)
        n_records += 1
        elem.clear()
    result: Dict[str, Tuple[str, str]] = {}
    n_owner = 0
    n_dropped = 0
    for key, uis in votes.items():
        if len(uis) == 1:
            ui = next(iter(uis))
        else:
            owners = [u for u in uis if record_name_key.get(u) == key]
            if len(owners) != 1:
                n_dropped += 1
                continue
            ui = owners[0]
            n_owner += 1
        result[key] = (ui, record_names[ui])
    logger.info(
        "MeSH supp lookup built: %d records, %d keys (%d via owner-priority, %d ambiguous dropped)",
        n_records, len(result), n_owner, n_dropped,
    )
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump({k: [v[0], v[1]] for k, v in result.items()}, f)
    return result


def match_mesh(name: str, mesh_lookup: Dict[str, Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    """Exact folded-key MeSH match. Return (mesh_ui, descriptor_name) or None."""
    for key in lookup_keys(name):
        hit = mesh_lookup.get(key)
        if hit:
            return hit
    return None


def fuzzy_mesh_proposal(
    name: str,
    mesh_lookup: Dict[str, Tuple[str, str]],
    mesh_keys: List[str],
    cutoff: float = 0.88,
) -> Optional[Tuple[str, str, float]]:
    """Below-confidence fuzzy MeSH proposal for the manual curation queue.

    Returns (mesh_ui, descriptor_name, ratio) or None. Guards mirror
    propose_entity_synonyms.py: no fuzzy matching for short strings or
    acronym-like tokens.
    """
    import difflib

    key = fold(name)
    if len(key) < 5:
        return None
    if re.fullmatch(r"[a-z0-9]+", key) and len(key) <= 8:
        return None
    close = difflib.get_close_matches(key, mesh_keys, n=1, cutoff=cutoff)
    if not close:
        return None
    mesh_ui, descriptor = mesh_lookup[close[0]]
    ratio = difflib.SequenceMatcher(None, key, close[0]).ratio()
    return mesh_ui, descriptor, round(ratio, 3)


def load_pd_allow_lists() -> Tuple[set, set]:
    """Curated PD gene/protein allow-lists (strategy 4), shared with sciai_core."""
    try:
        from sciai_core.kg.entity_normalizer import PD_GENE_SYMBOLS, PD_PROTEIN_NAMES
        return set(PD_GENE_SYMBOLS), set(PD_PROTEIN_NAMES)
    except Exception:
        # Fallback copy (keep in sync with sciai_core/kg/entity_normalizer.py).
        genes = {
            "SNCA", "LRRK2", "GBA", "PARK2", "PARK7", "PINK1", "PRKN",
            "DJ-1", "DJ1", "MAPT", "VPS35", "GCH1", "CHCHD2", "FBXO7",
            "PLA2G6", "DNAJC6", "SYNJ1", "ATP13A2", "UCHL1", "GIGYF2",
            "HTRA2", "EIF4G1", "TMEM230", "DNAJC13", "RIC3",
            "LRP10", "DCTN1", "SPG11", "C19ORF12", "COQ2", "C9ORF72",
        }
        proteins = {
            "ALPHA-SYNUCLEIN", "ALPHA SYNUCLEIN", "α-SYNUCLEIN", "ASYN",
            "LRRK2", "DARDARIN", "PARKIN", "PINK1", "DJ-1", "GBA", "GLUCOCEREBROSIDASE",
        }
        return genes, proteins


# ---------------------------------------------------------------------------
# MeSH tree numbers -> entity type authority map (KG workplan, Mon Aug 10)
# ---------------------------------------------------------------------------
# Canonical type authority rule: for canonical IDs in the MeSH namespaces
# (D-id descriptors, C-id supplementary concepts), the entity type is derived
# from the descriptor's tree location instead of the confidence-weighted
# majority vote over surface forms. SCRs carry no tree numbers, so their type
# comes from their HeadingMappedTo descriptor(s).
#
# TREE_TYPE_PREFIXES is evaluated longest-prefix-first per tree number; across
# a descriptor's multiple tree numbers the hit with the longest matching
# prefix wins, ties break by order of first appearance in this list.
#
# Types with NO clean MeSH branch (fall back to majority vote): Gene (MeSH
# does not model genes), Pathway, Outcome, Biomarker, Exercise.
#
# Debatable assignments (flagged for jk):
# - C23.888 "Signs and Symptoms" -> Symptom: some diseases also carry a
#   manifestations tree (e.g. under C23.888.592 Neurologic Manifestations);
#   with equal-length prefixes the earlier prefix wins, so C10 (nervous
#   system diseases) is listed before C23.888 to keep diseases as Disease.
# - D12.776/D12.644 "Proteins"/"Peptides" -> Protein: covers the protein
#   branches; D12.125 (Amino Acids) is deliberately NOT mapped to Protein so
#   single amino acids (e.g. levodopa's D12.125 trees) fall through to
#   Compound via the generic D rule.
# - D23.101 -> Biomarker: the Biomarkers branch; its members are biomarkers
#   by definition, but the vote fallback still decides anything outside it.
# - E01/E05 (diagnosis, investigative techniques) -> Concept via the generic
#   E fallback would lose procedures; instead only E02/E04 are Treatment and
#   the rest of E is Concept. Specific pins belong in CURATED_TYPE_OVERRIDES.
TREE_TYPE_PREFIXES: List[Tuple[str, str]] = [
    ("A08.186.211", "BrainRegion"),  # Brain
    ("A11.284", "Organelle"),        # Cellular Structures (mitochondria, lysosomes, ...)
    ("A11", "CellType"),             # Cells
    ("A", "Concept"),                # other anatomy (body regions, tissues, organs)
    ("B", "Species"),                # Organisms
    ("C10", "Disease"),              # Nervous System Diseases (before C23.888 tie-breaks)
    ("C23.888", "Symptom"),          # Signs and Symptoms
    ("C", "Disease"),                # Diseases (other branches)
    ("D12.776", "Protein"),          # Proteins
    ("D12.644", "Protein"),          # Peptides
    ("D23.101", "Biomarker"),        # Biomarkers branch
    ("D", "Compound"),               # Chemicals and Drugs (incl. D27 pharmacological actions)
    ("E02", "Treatment"),            # Therapeutics
    ("E04", "Treatment"),            # Surgical Procedures, Operative
    ("E", "Concept"),                # diagnosis / investigative techniques (debatable)
    ("F03", "Disease"),              # Mental Disorders
    ("F", "Concept"),                # Behavior and Behavior Mechanisms etc.
    ("G", "Mechanism"),              # Phenomena and Processes
    ("H", "Concept"), ("I", "Concept"), ("J", "Concept"), ("K", "Concept"),
    ("L", "Concept"), ("M", "Concept"), ("N", "Concept"),
    ("V", "Concept"), ("Z", "Concept"),
]

# Owner-pinned types, consulted last-wins (overrides both tree rule and vote).
# The 2026-09-12 top-300 adjudication superseded the original MRI (D008279,
# Treatment -> Assay) and Nerve Degeneration (D009410, Mechanism -> Disease)
# pins; the current values live in the dated block below.
CURATED_TYPE_OVERRIDES: Dict[str, str] = {
    # jk curation 2026-09-12: top-300 Concept adjudication
    "ALPHA-SYNUCLEIN": "Protein",  # rank 44: alpha-Synuclein
    "C510793": "Compound",  # rank 219: pimavanserin
    "C537180": "Symptom",  # rank 272: paroxysmal kinesigenic dyskinesia
    "CLU:anxiety-like-behavior": "Symptom",  # rank 258: anxiety-like behavior
    "CLU:beta-oscillation": "Biomarker",  # rank 103: beta oscillations
    "CLU:dementia-with-lewy-bodies-2": "Mechanism",  # rank 28: dementia with Lewy bodies
    "CLU:disease-duration": "Outcome",  # rank 201: disease duration
    "CLU:fall-risk": "Symptom",  # rank 278: fall risk
    "CLU:levodopa-induced-dyskinesia-2": "Symptom",  # rank 179: levodopa-induced dyskinesia
    "CLU:motor-impairment": "Symptom",  # rank 212: motor impairment
    "CLU:neuronal-cell-death": "Pathway",  # rank 155: neuronal death
    "CLU:neuronal-survival": "Pathway",  # rank 227: neuronal survival
    "CLU:parkinson-s-disease-dementia": "Disease",  # rank 115: Parkinson's disease dementia
    "CLU:statistical-analysis": "Mechanism",  # rank 268: statistical analysis
    "D000058": "Symptom",  # rank 101: Falls
    "D000066829": "Mechanism",  # rank 19: Neuroprotection
    "D000067128": "Organelle",  # rank 52: Extracellular Vesicles
    "D000069196": "Mechanism",  # rank 70,223: gut microbiota / gut microbiome
    "D000069550": "Mechanism",  # rank 27: Machine Learning
    "D000070627": "Disease",  # rank 285: Chronic Traumatic Encephalopathy
    "D000070642": "Symptom",  # rank 245: traumatic brain injury
    "D000071679": "Protein",  # rank 260: GSK-3β
    "D000072797": "Outcome",  # rank 198: Walking Speed
    "D000073216": "Assay",  # rank 82,144: Unified Parkinson's Disease Rating Scale (UPDRS) / Montreal Cognitive Assessment (MoCA)
    "D000073865": "EnvironmentalFactor",  # rank 299: Cigarette Smoking
    "D000076142": "Exercise",  # rank 99: Virtual Reality
    "D000077321": "Mechanism",  # rank 180: Deep Learning
    "D000078182": "Mechanism",  # rank 92: Systematic Review
    "D000079403": "Pathway",  # rank 41: Ferroptosis
    "D000080874": "Disease",  # rank 255: Synucleinopathies
    "D000084802": "Outcome",  # rank 91: Caregiver Burden
    "D000086382": "Disease",  # rank 269: COVID-19
    "D000086402": "Species",  # rank 76: SARS-CoV-2
    "D000087502": "Pathway",  # rank 256: gut-brain axis
    "D000088282": "Disease",  # rank 195: corticobasal syndrome
    "D000089943": "Symptom",  # rank 208: Sleep Quality
    "D000090862": "Disease",  # rank 2: Neuroinflammatory Diseases
    "D000098435": "Mechanism",  # rank 297: Machine Learning Algorithms
    "D000109": "Compound",  # rank 100: Acetylcholine
    "D000203": "Outcome",  # rank 141: Activities of Daily Living
    "D000375": "Mechanism",  # rank 39: Aging
    "D000544": "Disease",  # rank 3: Alzheimer Disease
    "D000682": "Pathway",  # rank 139: Amyloid
    "D000690": "Disease",  # rank 51: Amyotrophic Lateral Sclerosis
    "D000860": "Symptom",  # rank 229: Hypoxia
    "D000893": "Mechanism",  # rank 253: anti-inflammatory
    "D000975": "Mechanism",  # rank 75: Antioxidants
    "D001007": "Symptom",  # rank 22: Anxiety
    "D001253": "CellType",  # rank 293: astrocyte
    "D001259": "Symptom",  # rank 93: Ataxia
    "D001288": "Outcome",  # rank 170: Attention
    "D001343": "Pathway",  # rank 13: Autophagy
    "D001479": "BrainRegion",  # rank 29: Basal Ganglia
    "D001678": "Mechanism",  # rank 210: Organelle Biogenesis
    "D001812": "BrainRegion",  # rank 32: Blood-Brain Barrier
    "D001905": "Compound",  # rank 95: botulinum toxin
    "D001921": "BrainRegion",  # rank 126: Brain
    "D002110": "Compound",  # rank 123: Caffeine
    "D002185": "Compound",  # rank 56: Cannabidiol
    "D002490": "BrainRegion",  # rank 33: Central Nervous System
    "D002524": "Symptom",  # rank 97: Cerebellar Ataxia
    "D002531": "BrainRegion",  # rank 37: Cerebellum
    "D002555": "BrainRegion",  # rank 78: Cerebrospinal Fluid
    "D002819": "Symptom",  # rank 109: Chorea
    "D002940": "Mechanism",  # rank 200: Circadian Rhythm
    "D003071": "Outcome",  # rank 57,66: Cognition / cognitive function
    "D003248": "Symptom",  # rank 71: Constipation
    "D003291": "Disease",  # rank 257: functional movement disorder
    "D003474": "Compound",  # rank 74: Curcumin
    "D003680": "Symptom",  # rank 137: dysphagia
    "D003687": "Compound",  # rank 164: DHEA
    "D003704": "Disease",  # rank 24: Dementia
    "D003863": "Symptom",  # rank 8: Depression
    "D003933": "Mechanism",  # rank 114: Diagnosis
    "D004249": "Mechanism",  # rank 235: DNA Damage
    "D004298": "Neurotransmitter",  # rank 7: Dopamine
    "D004318": "Compound",  # rank 273: Doxycycline
    "D004401": "Symptom",  # rank 183: Dysarthria
    "D004421": "Symptom",  # rank 18: Dystonia
    "D004569": "Assay",  # rank 145: Electroencephalography
    "D004576": "Assay",  # rank 196: Electromyography
    "D004856": "Symptom",  # rank 120: Postural Balance
    "D005221": "Symptom",  # rank 49: Fatigue
    "D005260": "Species",  # rank 189: Female
    "D005609": "Mechanism",  # rank 280: Free Radicals
    "D005680": "Neurotransmitter",  # rank 271: GABA
    "D005684": "Outcome",  # rank 25: Gait
    "D005820": "Assay",  # rank 102: Genetic Testing
    "D006816": "Disease",  # rank 59: Huntington Disease
    "D006973": "Disease",  # rank 202: Hypertension
    "D007024": "Symptom",  # rank 111: orthostatic hypotension
    "D007107": "Mechanism",  # rank 274: Immune System
    "D007109": "Mechanism",  # rank 218: immune response
    "D007319": "Symptom",  # rank 206: insomnia
    "D007333": "Pathway",  # rank 135: Insulin Resistance
    "D007501": "Compound",  # rank 106: Iron
    "D008124": "Outcome",  # rank 62,279: locomotor activity / Locomotion
    "D008279": "Assay",  # rank 185: Magnetic Resonance Imaging
    "D008297": "Species",  # rank 160: Male
    "D008345": "Compound",  # rank 153: Manganese
    "D008550": "Compound",  # rank 84: Melatonin
    "D008568": "Outcome",  # rank 131: Memory
    "D008570": "Outcome",  # rank 264: working memory
    "D008687": "Compound",  # rank 193: Metformin
    "D008995": "Protein",  # rank 162: MAO-B
    "D009026": "Outcome",  # rank 72: Mortality
    "D009043": "Symptom",  # rank 296: Motor Activity
    "D009069": "Symptom",  # rank 34,124: Movement Disorders / movement disorder
    "D009369": "Disease",  # rank 184: Neoplasms
    "D009410": "Disease",  # rank 21: Neurodegeneration
    "D009422": "Disease",  # rank 265: neurological disorders
    "D009473": "Mechanism",  # rank 246: Neuroplasticity
    "D009474": "CellType",  # rank 270: neuron
    "D009538": "EnvironmentalFactor",  # rank 11: Nicotine
    "D009714": "BrainRegion",  # rank 233: Nucleus Accumbens
    "D009830": "BrainRegion",  # rank 216: Olfactory Bulb
    "D010146": "Outcome",  # rank 73: Pain
    "D010300": "Disease",  # rank 10: Parkinson's Disease (PD)
    "D010361": "Species",  # rank 261: Patient
    "D010949": "CellComponent",  # rank 168: Plasma
    "D011699": "BrainRegion",  # rank 64: Putamen
    "D011788": "Outcome",  # rank 6: Quality of Life
    "D011794": "Compound",  # rank 171: Quercetin
    "D011930": "Mechanism",  # rank 281: Reaction Time
    "D012640": "Disease",  # rank 169: Seizures
    "D012680": "Treatment",  # rank 88: Sensitivity and Specificity
    "D012701": "Neurotransmitter",  # rank 110: Serotonin
    "D012723": "Mechanism",  # rank 133: Sex
    "D012798": "Disease",  # rank 225: Sialorrhea
    "D012892": "Symptom",  # rank 291: Sleep Deprivation
    "D012893": "Symptom",  # rank 175: Sleep Wake Disorders
    "D012907": "EnvironmentalFactor",  # rank 177: Smoking
    "D013494": "Disease",  # rank 108: Progressive Supranuclear Palsy
    "D013788": "BrainRegion",  # rank 89: Thalamus
    "D014202": "Symptom",  # rank 16: Tremor
    "D014446": "Protein",  # rank 242: Tyrosine hydroxylase
    "D014807": "Compound",  # rank 244: Vitamin D
    "D014887": "Symptom",  # rank 50: freezing of gait
    "D014930": "Species",  # rank 231: Women
    "D015227": "Pathway",  # rank 148: Lipid Peroxidation
    "D015415": "Biomarker",  # rank 125,203: Biomarkers / biomarker
    "D015444": "Exercise",  # rank 1,107,122: Exercise / Physical exercise / aerobic exercise
    "D015655": "Compound",  # rank 94: MPP+
    "D015671": "Assay",  # rank 167: Electroacupuncture
    "D015850": "Protein",  # rank 132: IL-6
    "D015870": "Mechanism",  # rank 55: Gene Expression
    "D015995": "Treatment",  # rank 211: Prevalence
    "D016138": "Exercise",  # rank 226: Walking
    "D016229": "Biomarker",  # rank 220: amyloid-beta
    "D016328": "Protein",  # rank 83: NF-κB
    "D016430": "StudyDesign",  # rank 60: Clinical Trial
    "D016449": "StudyDesign",  # rank 67: Randomized Controlled Trial
    "D016627": "Compound",  # rank 134,267: 6-OHDA / 6-Hydroxydopamine
    "D016631": "Biomarker",  # rank 117: Lewy Bodies
    "D016875": "Protein",  # rank 138,165: tau protein / tau
    "D016923": "Pathway",  # rank 173: Cell Death
    "D017286": "Outcome",  # rank 191: Polysomnography
    "D017382": "Mechanism",  # rank 30,249: Reactive Oxygen Species / ROS
    "D017397": "BrainRegion",  # rank 61: Prefrontal Cortex
    "D017628": "CellType",  # rank 9: Microglia
    "D018377": "Neurotransmitter",  # rank 298: Neurotransmitter Agents
    "D018476": "Symptom",  # rank 36: Hypokinesia
    "D018491": "Neurotransmitter",  # rank 276: Dopamine Agonists
    "D018696": "Treatment",  # rank 161: Neuroprotective Agents
    "D018698": "Compound",  # rank 98: Glutamic Acid
    "D019175": "Mechanism",  # rank 182: DNA Methylation
    "D019208": "Protein",  # rank 35: BDNF
    "D019578": "Disease",  # rank 17: Multiple System Atrophy
    "D019636": "Disease",  # rank 15: Neurodegenerative Diseases
    "D020187": "Symptom",  # rank 31: REM Sleep Behavior Disorder
    "D020734": "Disease",  # rank 20: Parkinsonism
    "D020820": "Symptom",  # rank 292: Dyskinesia
    "D020961": "Disease",  # rank 190: Lewy body dementia
    "D028361": "Disease",  # rank 12: Mitochondrial Diseases
    "D035061": "StudyDesign",  # rank 239: control group
    "D035683": "CellComponent",  # rank 121: MicroRNAs
    "D044967": "CellComponent",  # rank 188: Serum
    "D046690": "Treatment",  # rank 5: Deep Brain Stimulation
    "D049109": "Mechanism",  # rank 275: Cell Proliferation
    "D050781": "Treatment",  # rank 127: Transcranial Magnetic Stimulation
    "D051100": "Protein",  # rank 192: GDNF
    "D051267": "Protein",  # rank 48: Nrf2
    "D051844": "Protein",  # rank 4,44: α-synuclein / alpha-Synuclein
    "D055070": "Exercise",  # rank 295: Resistance Training
    "D055495": "Mechanism",  # rank 105: Neurogenesis
    "D055948": "Symptom",  # rank 154: Sarcopenia
    "D056004": "Treatment",  # rank 251: Molecular Dynamics Simulation
    "D056564": "Protein",  # rank 163: SIRT1
    "D057185": "Symptom",  # rank 215: Sedentary Behavior
    "D059350": "Symptom",  # rank 237: Chronic Pain
    "D059808": "Compound",  # rank 221: Polyphenols
    "D059865": "Pathway",  # rank 174: Endoplasmic Reticulum Stress
    "D059906": "Assay",  # rank 152: Neuroimaging
    "D060825": "Symptom",  # rank 23: cognitive impairment
    "D063154": "Mechanism",  # rank 283: Mitochondrial Dynamics
    "D063306": "Biomarker",  # rank 54: Mitophagy
    "D064307": "Mechanism",  # rank 130: Microbiota
    "D065908": "Exercise",  # rank 187: Transcranial Direct Current Stimulation
    "D066191": "BrainRegion",  # rank 277: Sensorimotor Cortex
    "D066329": "Mechanism",  # rank 46: protein aggregation
    "GBA": "Gene",  # rank 116: GBA1
    "PRKN": "Gene",  # rank 259: PRKN
    "SCIAI:healthy-controls": "StudyDesign",  # rank 90: healthy controls
    "SCIAI:motor-symptoms": "Symptom",  # rank 43: motor symptoms
    "SCIAI:non-motor-symptoms": "Symptom",  # rank 68: non-motor symptoms
    "SCIAI:sh-sy5y": "CellType",  # rank 77: SH-SY5Y cells
    "SELF:anti iglon5 disease": "Disease",  # rank 143: anti-IgLON5 disease
    "SELF:autonomic dysfunction": "Symptom",  # rank 236: autonomic dysfunction
    "SELF:balance": "Outcome",  # rank 63: Balance
    "SELF:berg balance scale": "Outcome",  # rank 217: Berg Balance Scale
    "SELF:cytotoxicity": "Mechanism",  # rank 294: cytotoxicity
    "SELF:diabetes": "Disease",  # rank 290: diabetes
    "SELF:drug delivery": "Mechanism",  # rank 284: drug delivery
    "SELF:functional connectivity": "Pathway",  # rank 85: functional connectivity
    "SELF:irisin": "Gene",  # rank 156: Irisin
    "SELF:lps": "Compound",  # rank 176: LPS
    "SELF:microglial activation": "Mechanism",  # rank 247: microglial activation
    "SELF:mitochondrial function": "Pathway",  # rank 146: mitochondrial function
    "SELF:mobility": "Outcome",  # rank 147: Mobility
    "SELF:motor deficits": "Symptom",  # rank 214: motor deficits
    "SELF:motor dysfunction": "Symptom",  # rank 178: motor dysfunction
    "SELF:motor function": "Mechanism",  # rank 47: motor function
    "SELF:motor learning": "Mechanism",  # rank 213: Motor learning
    "SELF:neuronal damage": "Mechanism",  # rank 172: neuronal damage
    "SELF:neuronal loss": "Pathway",  # rank 181: neuronal loss
    "SELF:neurotoxicity": "Mechanism",  # rank 119: neurotoxicity
    "SELF:participants": "StudyDesign",  # rank 205: participants
    "SELF:physiotherapy": "Exercise",  # rank 86: Physiotherapy
    "SELF:spasticity": "Assay",  # rank 289: spasticity
    "SELF:status dystonicus": "Symptom",  # rank 262: status dystonicus
    "SELF:stride length": "Outcome",  # rank 230: stride length
    "SELF:tdp 43": "Protein",  # rank 234: TDP-43
    "SELF:treadmill exercise": "Exercise",  # rank 53: Treadmill exercise
    "SELF:updrs": "Assay",  # rank 254: UPDRS
    "SELF:wearable sensors": "Biomarker",  # rank 238: wearable sensors
    "SNCA": "Protein",  # rank 87: SNCA
    "SELF:physical therapy modalities": "Treatment",  # rank 129: Physical Therapy Modalities (Track-A-only today)
    "SELF:disorders of excessive somnolence": "Disease",  # rank 157: Disorders of Excessive Somnolence (Track-A-only today)
    "SELF:dystonic disorders": "Disease",  # rank 263: Dystonic Disorders (Track-A-only today)
}


def _cached_json(cache_path: Path, source_path: Path) -> Optional[Dict]:
    """Return parsed JSON cache when it is newer than its source, else None."""
    if cache_path.exists() and cache_path.stat().st_mtime >= source_path.stat().st_mtime:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_mesh_tree_map(
    xml_path: Path = MESH_XML_PATH,
    cache_path: Path = MESH_TREE_CACHE_PATH,
) -> Dict[str, List[str]]:
    """Return {descriptor_ui: [tree_number, ...]} from MeSH descriptors XML."""
    cached = _cached_json(cache_path, xml_path)
    if cached is not None:
        return {k: list(v) for k, v in cached.items()}

    logger.info("Parsing MeSH tree numbers from %s (one-off, this is slow)", xml_path)
    tree_map: Dict[str, List[str]] = {}
    context = ET.iterparse(str(xml_path), events=("end",))
    for _event, elem in context:
        if elem.tag != "DescriptorRecord":
            continue
        ui_el = elem.find("DescriptorUI")
        if ui_el is not None and ui_el.text:
            trees = [
                t.text.strip()
                for t in elem.findall("TreeNumberList/TreeNumber")
                if t.text and t.text.strip()
            ]
            if trees:
                tree_map[ui_el.text.strip()] = trees
        elem.clear()
    logger.info("MeSH tree map built: %d descriptors with tree numbers", len(tree_map))
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(tree_map, f)
    return tree_map


def load_mesh_supp_heading_map(
    xml_path: Path = MESH_SUPP_XML_PATH,
    cache_path: Path = MESH_SUPP_HEADING_CACHE_PATH,
) -> Dict[str, List[str]]:
    """Return {scr_ui: [descriptor_ui, ...]} from MeSH supplementary concepts XML.

    SCRs have no tree numbers; their HeadingMappedTo descriptors provide the
    tree location. Starred UIs ("*D001561" = main heading) are unstarred.
    """
    cached = _cached_json(cache_path, xml_path)
    if cached is not None:
        return {k: list(v) for k, v in cached.items()}

    logger.info("Parsing MeSH SCR heading mappings from %s (one-off, this is slow)", xml_path)
    heading_map: Dict[str, List[str]] = {}
    context = ET.iterparse(str(xml_path), events=("end",))
    for _event, elem in context:
        if elem.tag != "SupplementalRecord":
            continue
        ui_el = elem.find("SupplementalRecordUI")
        if ui_el is not None and ui_el.text:
            uis = []
            for dui in elem.findall(
                "HeadingMappedToList/HeadingMappedTo/DescriptorReferredTo/DescriptorUI"
            ):
                if dui.text and dui.text.strip():
                    uis.append(dui.text.strip().lstrip("*"))
            if uis:
                heading_map[ui_el.text.strip()] = uis
        elem.clear()
    logger.info("MeSH SCR heading map built: %d records with heading mappings", len(heading_map))
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(heading_map, f)
    return heading_map


# UniProtKB accession pattern (canonical accessions; no isoform "-1" or
# version ".1" suffixes). https://web.expasy.org/docs/userman.html#ID_line
UNIPROT_ACCESSION_RE = re.compile(
    r"[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}")


def mesh_entity_type(
    canonical_id: str,
    tree_map: Dict[str, List[str]],
    supp_heading_map: Dict[str, List[str]],
    prefixes: Optional[List[Tuple[str, str]]] = None,
    overrides: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Authority-rule entity type for a MeSH canonical id (D- or C-id).

    Namespace rules run ahead of the majority vote: HGNC:* ids are Gene and
    UniProt accessions are Protein by construction (vocabulary normalization
    Phases 1+2, ratified 2026-09-29).
    Returns None when the id carries no authority rule or no tree prefix
    matches (caller falls back to the majority vote).
    CURATED_TYPE_OVERRIDES wins last.
    """
    overrides = CURATED_TYPE_OVERRIDES if overrides is None else overrides
    if canonical_id in overrides:
        return overrides[canonical_id]
    if canonical_id.startswith("HGNC:"):
        return "Gene"
    if UNIPROT_ACCESSION_RE.fullmatch(canonical_id):
        return "Protein"
    prefixes = TREE_TYPE_PREFIXES if prefixes is None else prefixes

    if canonical_id.startswith("D"):
        trees = tree_map.get(canonical_id, [])
    elif canonical_id.startswith("C"):
        trees = []
        for descriptor_ui in supp_heading_map.get(canonical_id, []):
            trees.extend(tree_map.get(descriptor_ui, []))
    else:
        return None

    best: Optional[Tuple[int, int, str]] = None  # (prefix_len, -prefix_order, type)
    for tree in trees:
        for order, (prefix, entity_type) in enumerate(prefixes):
            # Top-level MeSH trees have no dot after the category letter
            # ("D02.092", not "D.02.092"), so single-letter prefixes match on
            # plain startswith; deeper prefixes must match a whole segment.
            if tree == prefix or tree.startswith(prefix + ".") or (
                len(prefix) == 1 and tree.startswith(prefix)
            ):
                candidate = (len(prefix), -order, entity_type)
                if best is None or candidate[:2] > best[:2]:
                    best = candidate
    return best[2] if best else None


def self_canonical_id(surface_form: str) -> Optional[str]:
    """Self-canonical fallback id for an unmapped surface form (normalized=false).

    Returns None when the folded form is empty (pathological input stays
    unmappable).
    """
    folded = fold(surface_form)
    return f"SELF:{folded}" if folded else None


def self_synonym_row(surface_form: str, entity_type: str) -> Optional[Dict[str, object]]:
    """Row payload for registering a SELF fallback form in kg.entity_synonyms.

    Registered rows let populate resolve SELF ids through the normal indexes
    and let the Neo4j ingest type (majority vote over extracted types) and
    name (original surface form) the node properly. Returns None for
    pathological input (empty folded form).
    """
    self_id = self_canonical_id(surface_form)
    if self_id is None:
        return None
    return {
        "surface_form": surface_form,
        "canonical_id": self_id,
        "canonical_source": "self",
        "entity_type": entity_type,
        "confidence": 1.0,
        "curated": False,
    }
