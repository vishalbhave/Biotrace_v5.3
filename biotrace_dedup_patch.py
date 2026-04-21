"""
biotrace_dedup_patch.py  —  BioTrace v5.4
────────────────────────────────────────────────────────────────────────────
DROP-IN REPLACEMENT for dedup_occurrences() and _canon() in biotrace_gnv.py.

ROOT CAUSE of the CSV duplicates
─────────────────────────────────
The original key is:
    f"{canon(validName)}||{canon(verbatimLocality)}"

"canon" only lowercases + collapses spaces, so these are FOUR different keys:
    cassiopea andromeda || gulf of kutch, gujarat
    cassiopea andromeda || gulf of kutch, india
    cassiopea andromeda || narara
    cassiopea andromeda || arambhada coast

None of them match each other, so all four records survive dedup.

WHAT THIS PATCH ADDS
────────────────────
1. Locality normalisation strips administrative suffixes common in Nominatim
   output and Indian address strings before computing the key.

2. Locality containment check: after per-record dedup by exact key, a second
   pass merges records where one locality string is a substring of another
   for the SAME species (e.g. "narara" ⊂ "narara, gulf of kutch, gujarat").
   The record with the MORE SPECIFIC (longer) locality is kept.

3. Life-stage / non-taxonomic name filter (also used by schema patch):
   Records whose recordedName / validName matches a known life-stage term or
   abbreviated genus (single capital letter + ".") are flagged as
   `taxonomicStatus = "non-taxonomic"` and excluded from dedup output.
   See LIFE_STAGE_TERMS and _is_non_taxon() below.

HOW TO INTEGRATE
────────────────
In biotrace_gnv.py, replace:

    def dedup_occurrences(...):   [the whole function]
    def _canon(s):                [the helper]

with the equivalents exported here.  Or, more surgically, add at the top of
biotrace_gnv.py:

    from biotrace_dedup_patch import dedup_occurrences, _canon   # overrides

and remove the original definitions.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("biotrace.dedup")

# ─────────────────────────────────────────────────────────────────────────────
#  Life-stage / non-taxonomic blocklist
# ─────────────────────────────────────────────────────────────────────────────

# Exact terms (case-insensitive) that should NEVER be treated as species names
LIFE_STAGE_TERMS: frozenset[str] = frozenset({
    # Jellyfish / scyphozoan
    "scyphistoma", "scyphistomae", "ephyra", "ephyrae", "strobila", "strobilae",
    "medusa", "medusae", "polyp", "polyps",
    # General cnidarian
    "planula", "planulae", "zooid", "zooids", "hydroid", "hydroids",
    # General larval / developmental
    "larva", "larvae", "juvenile", "juveniles", "nauplius", "nauplii",
    "cypris", "zoea", "megalopa", "veliger", "trochophore", "spat",
    # Echinoderm
    "bipinnaria", "brachiolaria", "doliolaria", "auricularia",
    # Fish
    "alevin", "fry", "fingerling",
    # Invertebrate structural
    "spore", "cyst", "resting cyst",
    # Generic
    "adult", "sub-adult", "subadult", "immature",
})

# Regex: abbreviated genus "C. something" or just bare "C." — single capital + period
_ABBREV_GENUS_RE = re.compile(r"^[A-Z]\.\s*\w*$")

# Admin-hierarchy noise words to strip from locality strings before comparing
_LOC_STRIP_WORDS = re.compile(
    r"\b(india|gujarat|tamilnadu|kerala|maharashtra|karnataka|andhra pradesh"
    r"|goa|rajasthan|odisha|west bengal|lakshadweep|andaman|nicobar"
    r"|district|taluka|tehsil|mandal|block|village|coast|coastal|waters"
    r"|sea|bay|gulf|island|islands|creek|estuary|lagoon|reef|shoal"
    r"|national park|marine national park|wildlife sanctuary"
    r"|[,;]\s*(?:\d{6})?"              # pin codes
    r")\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _canon(s: str) -> str:
    """Canonical form: lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", str(s).lower().strip())


def _loc_key(locality: str) -> str:
    """
    Locality key for CONTAINMENT matching.
    Strips trailing admin suffixes ("Narara, Gulf of Kutch, Gujarat" → "narara")
    so that a coarse locality and its enriched form collapse to the same root token.
    """
    s = _canon(locality)
    # Remove stop-words
    s = _LOC_STRIP_WORDS.sub(" ", s)
    # Remove punctuation other than letters/digits/spaces
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_non_taxon(name: str) -> Optional[str]:
    """
    Return a reason string if `name` should be excluded as non-taxonomic,
    or None if it looks like a valid scientific name.

    Catches:
      • Life-stage terms (Scyphistoma, Medusa, Ephyra …)
      • Single abbreviated genus references ("C. andromeda")  — caller should
        resolve these rather than exclude; we return reason="abbreviation"
        to distinguish.
    """
    clean = name.strip()
    # Bare life stage
    if _canon(clean) in LIFE_STAGE_TERMS:
        return "life_stage"
    # First word is a life stage (e.g. "Scyphistoma polyp")
    first_word = clean.split()[0] if clean.split() else ""
    if _canon(first_word) in LIFE_STAGE_TERMS:
        return "life_stage"
    # Abbreviated genus: single capital + period (optionally followed by epithet)
    if _ABBREV_GENUS_RE.match(clean):
        return "abbreviation"
    return None


def _name_key(occ: dict) -> str:
    """Canonical species name for grouping."""
    name = (occ.get("validName") or occ.get("recordedName") or
            occ.get("Recorded Name", ""))
    return _canon(str(name))


# ─────────────────────────────────────────────────────────────────────────────
#  Main: dedup_occurrences
# ─────────────────────────────────────────────────────────────────────────────

def dedup_occurrences(
    occurrences: list[dict],
    keep_secondary: bool = True,
    filter_non_taxon: bool = True,
) -> tuple[list[dict], int]:
    """
    Remove duplicate and non-taxonomic records from an occurrence list.

    Three-stage pipeline
    ─────────────────────
    Stage 0 — Non-taxon filter
        Records whose name is a life-stage term are removed (or flagged).
        Records with abbreviated genus are logged as warnings (not removed —
        the LLM abbreviation-expansion prompt handles these upstream).

    Stage 1 — Exact-key dedup  (same as original)
        Key = (canon_name, canon_locality)
        Resolution: Primary > Secondary > Uncertain; tie-break by evidence length.

    Stage 2 — Locality-containment merge
        For records with the same species, check if one locality key is a
        substring of another's locality key. The record with the longer
        (more specific) locality absorbs the shorter one.

    Returns (deduplicated_list, n_removed).
    """
    _priority = {"primary": 0, "secondary": 1, "uncertain": 2, "": 3}

    def _ev_len(occ: dict) -> int:
        return len(str(
            occ.get("Raw Text Evidence") or occ.get("rawTextEvidence", "")
        ))

    # ── Stage 0: life-stage filter ────────────────────────────────────────────
    clean_occs: list[dict] = []
    n_removed = 0
    for occ in occurrences:
        if not isinstance(occ, dict):
            continue
        name = str(occ.get("recordedName") or occ.get("validName") or
                   occ.get("Recorded Name", "")).strip()
        reason = _is_non_taxon(name)
        if reason == "life_stage" and filter_non_taxon:
            logger.info(
                "[dedup] Excluded non-taxonomic record: '%s' (reason=%s)", name, reason
            )
            n_removed += 1
            continue
        if reason == "abbreviation":
            logger.warning(
                "[dedup] Abbreviated name not expanded upstream: '%s' — kept but flagged", name
            )
            occ["taxonomicStatus"] = occ.get("taxonomicStatus") or "unresolved_abbreviation"
        clean_occs.append(occ)

    # ── Stage 1: exact-key dedup ──────────────────────────────────────────────
    seen: dict[str, dict] = {}
    for occ in clean_occs:
        name_k = _name_key(occ)
        loc_k  = _loc_key(occ.get("verbatimLocality") or "")
        k      = f"{name_k}||{loc_k}"

        if k not in seen:
            seen[k] = occ
        else:
            existing  = seen[k]
            prio_new  = _priority.get(str(occ.get("occurrenceType","")).lower(), 3)
            prio_old  = _priority.get(str(existing.get("occurrenceType","")).lower(), 3)
            if prio_new < prio_old:
                seen[k] = occ
            elif prio_new == prio_old and _ev_len(occ) > _ev_len(existing):
                seen[k] = occ
            n_removed += 1

    after_stage1 = list(seen.values())

    # ── Stage 2: locality-containment merge per species ───────────────────────
    # Group by canonical species name
    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for occ in after_stage1:
        groups[_name_key(occ)].append(occ)

    final: list[dict] = []
    for species, records in groups.items():
        if len(records) == 1:
            final.extend(records)
            continue

        # Sort by locality specificity (longer loc_key = more specific = preferred)
        records_sorted = sorted(
            records,
            key=lambda o: len(_loc_key(o.get("verbatimLocality") or "")),
            reverse=True,  # most specific first
        )

        merged: list[dict] = []
        absorbed: set[int] = set()

        for i, occ_i in enumerate(records_sorted):
            if i in absorbed:
                continue
            key_i = _loc_key(occ_i.get("verbatimLocality") or "")
            for j, occ_j in enumerate(records_sorted):
                if j == i or j in absorbed:
                    continue
                key_j = _loc_key(occ_j.get("verbatimLocality") or "")
                # occ_j's locality root tokens are all contained within occ_i's
                # e.g. key_j="narara" ⊂ key_i="narara gulf kutch"
                if key_j and key_i and key_j in key_i:
                    logger.info(
                        "[dedup] Locality containment: '%s' ⊂ '%s' — absorbing record",
                        occ_j.get("verbatimLocality",""),
                        occ_i.get("verbatimLocality",""),
                    )
                    # Prefer higher occurrenceType priority between the two
                    prio_i = _priority.get(
                        str(occ_i.get("occurrenceType","")).lower(), 3)
                    prio_j = _priority.get(
                        str(occ_j.get("occurrenceType","")).lower(), 3)
                    if prio_j < prio_i:
                        # absorbing record has lower priority — swap winner
                        occ_i = occ_j
                    absorbed.add(j)
                    n_removed += 1
            merged.append(occ_i)

        final.extend(merged)

    logger.info(
        "[dedup] %d → %d records (%d removed: life-stage=%d, exact-dup=%d, containment=%d)",
        len(occurrences), len(final), n_removed,
        len(occurrences) - len(clean_occs),                    # stage 0
        len(clean_occs) - len(after_stage1),                   # stage 1
        len(after_stage1) - len(final),                         # stage 2
    )
    return final, n_removed


# ─────────────────────────────────────────────────────────────────────────────
#  Standalone test
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    TEST_DATA = [
        {"recordedName": "Cassiopea andromeda", "verbatimLocality": "Gulf of Kutch, Gujarat",      "occurrenceType": "Primary"},
        {"recordedName": "Cassiopea andromeda", "verbatimLocality": "Gulf of Kutch, India",         "occurrenceType": "Primary"},
        {"recordedName": "Cassiopea andromeda", "verbatimLocality": "Narara",                       "occurrenceType": "Primary"},
        {"recordedName": "Cassiopea andromeda (Forsskål, 1775)", "verbatimLocality": "Gulf of Kutch, India", "occurrenceType": "Primary"},
        {"recordedName": "Cassiopea andromeda", "verbatimLocality": "Arambhada coast",              "occurrenceType": "Primary"},
        {"recordedName": "Scyphistoma",          "verbatimLocality": "Narara, Gulf of Kutch",        "occurrenceType": "Primary"},
        {"recordedName": "Scyphistoma",          "verbatimLocality": "intertidal area of dead coral reef", "occurrenceType": "Primary"},
        {"recordedName": "C. andromeda",         "verbatimLocality": "Arambhada coast",              "occurrenceType": "Primary"},
    ]
    result, removed = dedup_occurrences(TEST_DATA)
    print(f"\n{len(TEST_DATA)} → {len(result)} records ({removed} removed)\n")
    for r in result:
        print(f"  {r['recordedName']:<40} | {r['verbatimLocality']}")


# ─────────────────────────────────────────────────────────────────────────────
#  Stage 2 addendum: regional locality suppression
# ─────────────────────────────────────────────────────────────────────────────

import re as _re

_REGIONAL_PATTERNS = _re.compile(
    r"^(of\s+|bay\s+|gulf\s+|sea\s+|ocean\b|coast\s+of\b|waters\s+of\b"
    r"|not\s+reported\b|unknown\b|^n/a$)",
    _re.IGNORECASE,
)


def _is_regional(loc_key: str) -> bool:
    """
    True if the locality key is a water-body or regional descriptor rather
    than a named collection site.  Used to preferentially drop region-level
    duplicates when a site-level record for the same species exists.

    Examples:
      "of kutch"    → True   (Gulf of Kutch after stop-word stripping)
      "gulf mannar" → True
      "not reported"→ True
      "arambhada"   → False  (named collection site)
      "narara"      → False
    """
    stripped = loc_key.strip()
    if not stripped:
        return True
    return bool(_REGIONAL_PATTERNS.match(stripped))


def suppress_regional_duplicates(
    occurrences: list[dict],
) -> tuple[list[dict], int]:
    """
    Stage 3 (optional, run after dedup_occurrences):
    For each species, if site-level records exist (is_regional=False),
    remove any region-level duplicates (is_regional=True) for the SAME species.

    This collapses "Cassiopea andromeda @ Gulf of Kutch, Gujarat" when
    "Cassiopea andromeda @ Narara" and "Cassiopea andromeda @ Arambhada"
    already exist in the set.
    """
    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for occ in occurrences:
        groups[_name_key(occ)].append(occ)

    final: list[dict] = []
    removed = 0
    for species, recs in groups.items():
        if len(recs) == 1:
            final.extend(recs); continue

        site_level   = [r for r in recs if not _is_regional(_loc_key(r.get("verbatimLocality","")))]
        region_level = [r for r in recs if     _is_regional(_loc_key(r.get("verbatimLocality","")))]

        if site_level and region_level:
            for reg in region_level:
                logger.info(
                    "[dedup/suppress] Region-level record absorbed: '%s' (site records exist)",
                    reg.get("verbatimLocality",""),
                )
                removed += 1
            final.extend(site_level)
        else:
            final.extend(recs)

    return final, removed
