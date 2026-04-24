"""
biotrace_v5_integration_patch.py  —  BioTrace v5.5
────────────────────────────────────────────────────────────────────────────
Integration guide and drop-in patches for biotrace_v5.py.

THREE CHANGES TO MAKE IN biotrace_v5.py
────────────────────────────────────────

PATCH A — Replace old verifier with UnifiedTaxonVerifier
PATCH B — Route post-extraction through staging tab (not direct commit)
PATCH C — Filter __candidate_ entries right after extraction

Copy the code blocks below into biotrace_v5.py at the indicated locations.
"""

# ════════════════════════════════════════════════════════════════════════════
# PATCH A  — Replace species verification pipeline
# Location: near the top of biotrace_v5.py where verifiers are initialised
# ════════════════════════════════════════════════════════════════════════════

PATCH_A_IMPORTS = """
# ── Unified verifier (v5.5) ───────────────────────────────────────────────
from biotrace_unified_verifier import UnifiedTaxonVerifier, filter_candidates

# Initialise once at module level (caches in same SQLite DB)
_UNIFIED_VERIFIER: UnifiedTaxonVerifier | None = None

def get_unified_verifier(meta_db_path: str) -> UnifiedTaxonVerifier:
    global _UNIFIED_VERIFIER
    if _UNIFIED_VERIFIER is None:
        _UNIFIED_VERIFIER = UnifiedTaxonVerifier(
            cache_db   = meta_db_path,
            min_score  = 0.60,
            kingdom    = "Animalia",
            use_gnparser = True,
            use_gbif   = True,
            use_col    = True,
            use_itis   = True,   # requires: pip install pytaxize
        )
    return _UNIFIED_VERIFIER
"""

PATCH_A_USAGE = """
# In the extraction section, REPLACE the old verify_occurrence_names() call:
#
#   OLD:
#     from species_verifier import verify_occurrence_names
#     results = verify_occurrence_names(results)
#
#   NEW:
    results = filter_candidates(results, log_cb=log_cb)   # remove __candidate_ entries
    verifier = get_unified_verifier(META_DB_PATH)
    results  = verifier.verify_and_enrich(results, log_cb=log_cb)
#
# The unified verifier replaces ALL of:
#   species_verifier.verify_occurrence_names()
#   biotrace_gbif_verifier.gbif_verify_batch()
#   biotrace_col_client.COLClient lookups
# It coordinates them internally with caching.
"""


# ════════════════════════════════════════════════════════════════════════════
# PATCH B  — Replace direct DB write with staging tab
# Location: wherever results are saved after verification
# ════════════════════════════════════════════════════════════════════════════

PATCH_B_CODE = """
# ── Stage for HITL review instead of writing directly ────────────────────
from biotrace_hitl_staging_tab import stage_records_for_hitl, render_hitl_staging_tab

# After verification completes, stage results:
n_staged = stage_records_for_hitl(META_DB_PATH, results)
log_cb(f"[HITL] {n_staged} records staged for review in HITL tab")
st.success(f"✅ {n_staged} records ready for review in the **Staging Review** tab.")

# In the tabs section of your Streamlit app, add a new tab:
# tabs = st.tabs(["Upload", "Extract", "Staging Review", "Map", "Export", ...])
# with tabs[2]:   # Staging Review tab index
#     render_hitl_staging_tab(
#         meta_db_path = META_DB_PATH,
#         kg_db_path   = KG_DB_PATH,    # pass "" if not using
#         mb_db_path   = MB_DB_PATH,    # pass "" if not using
#         wiki_root    = WIKI_ROOT,     # pass "" if not using
#     )
"""


# ════════════════════════════════════════════════════════════════════════════
# PATCH C  — Inline candidate filter (minimal, no new import needed)
# Location: immediately after extract_occurrences() returns, before dedup
# ════════════════════════════════════════════════════════════════════════════

PATCH_C_CODE = """
import re as _re

def _remove_candidate_placeholders(records: list[dict]) -> tuple[list[dict], int]:
    \"\"\"
    Remove __candidate_* placeholders that leak from the agent retry loop
    and any records with empty / garbage names.

    Call this immediately after extract_occurrences() and before dedup.
    \"\"\"
    _CAND_RE   = _re.compile(r\"^_+candidate_\", _re.IGNORECASE)
    _JUNK_RE   = _re.compile(r'^[\\[{\\\"\\d]')  # JSON artifact as name

    clean, removed = [], 0
    for rec in records:
        if not isinstance(rec, dict):
            removed += 1
            continue
        name = str(rec.get("recordedName") or rec.get("Recorded Name", "")).strip()
        if _CAND_RE.match(name) or not name or (len(name) < 4 and _JUNK_RE.match(name)):
            removed += 1
            continue
        clean.append(rec)
    return clean, removed

# Usage immediately after raw extraction:
# results, n_removed = _remove_candidate_placeholders(results)
# if n_removed:
#     log_cb(f"[Filter] Removed {n_removed} candidate/placeholder records")
"""


# ════════════════════════════════════════════════════════════════════════════
# PATCH D  — gnparser name cleaner for pre-verification use
# Use this to strip author/year from names before querying taxonomy APIs
# ════════════════════════════════════════════════════════════════════════════

PATCH_D_CODE = """
def parse_canonical_names(names: list[str]) -> dict[str, str]:
    \"\"\"
    Use gnparser REST API to strip author+year from a list of names.
    Returns {original: canonical_without_author}.

    Example:
      'Cassiopea andromeda (Forsskål, 1775)' → 'Cassiopea andromeda'
      'Hydatina zonata Lightfoot, 1786'       → 'Hydatina zonata'

    Falls back to the original name if gnparser is unreachable.
    \"\"\"
    if not names:
        return {}
    try:
        import requests
        r = requests.post(
            "https://gnparser.globalnames.org/api/v1",
            json={"names": names, "with_details": False},
            timeout=12,
        )
        r.raise_for_status()
        result = {}
        for item in r.json():
            orig = item.get("verbatim", "")
            canon = (item.get("canonicalFull", {}).get("value", "")
                     or item.get("canonical", {}).get("value", "")
                     or orig)
            result[orig] = canon
        return result
    except Exception:
        return {n: n for n in names}
"""

if __name__ == "__main__":
    print("This file documents integration patches — not executable directly.")
    print("Apply PATCH_A, PATCH_B, PATCH_C, PATCH_D to biotrace_v5.py.")
    print()
    print("PATCH A: Replace old verifier imports + calls")
    print("PATCH B: Route results through staging tab instead of direct DB write")
    print("PATCH C: Add candidate filter immediately after extraction")
    print("PATCH D: Use gnparser to strip author/year from names")
