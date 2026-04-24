#!/usr/bin/env python3
"""
apply_biotrace_patches.py  —  BioTrace v5.4 Patch Applicator
────────────────────────────────────────────────────────────────────────────
Applies all 7 critical fixes to biotrace_v5.py in-place.

Usage:
    python apply_biotrace_patches.py               # patches biotrace_v5.py
    python apply_biotrace_patches.py --dry-run     # preview only, no writes
    python apply_biotrace_patches.py --backup      # also keeps .bak copy

Each patch is idempotent: running multiple times is safe (already-applied
patches are detected via their 'PATCHED:' sentinel and skipped).

Patches applied:
  P1 — Fix GNA Finder dict→string extraction        (line ~796)
  P2 — Filter __candidate_* from thinker dedup      (line ~866)
  P3 — Purge __candidate_* from tracker before render (line ~2390)
  P4 — Checklist mode toggle in UI                  (line ~2160)
  P5 — Pass checklist_mode to suppress_regional     (line ~1704)
  P6 — Fix PDF double-save (backup gate)            (line ~2118)
  P7 — Use file hash for pdfs_v5 filename           (line ~2216)
  P8 — Wire GBIF + HITL approval gate               (line ~2320)
  P9 — Wire agent loop for self-correction          (line ~2287)
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Callable


TARGET_FILE = Path("biotrace_v5.py")
SENTINEL    = "# PATCHED:"


@dataclass
class Patch:
    name:        str
    description: str
    old:         str
    new:         str


# ─────────────────────────────────────────────────────────────────────────────
#  PATCH DEFINITIONS
#  Each patch replaces an exact string from biotrace_v5.py.
#  'old' must be a unique substring of the file.
#  'new' includes a '# PATCHED: <name>' sentinel on the first line.
# ─────────────────────────────────────────────────────────────────────────────

PATCHES: list[Patch] = [

    # ── P1: Fix GNA Finder returning list[dict] instead of list[str] ──────────
    Patch(
        name="P1-gna-finder-dict-fix",
        description="Fix GNA Finder extracting dicts instead of name strings (line ~796)",
        old="""\
          gna_names = find_species_with_gnfinder(chunk_text[:8000])
            if gna_names:
                found.extend(gna_names)
                log_cb(f"    [GNA Finder] {len(gna_names)} names detected")""",
        new="""\
          # PATCHED: P1-gna-finder-dict-fix — find_names_in_text returns list[dict]
            gna_raw = find_species_with_gnfinder(chunk_text[:8000])
            if gna_raw:
                if gna_raw and isinstance(gna_raw[0], dict):
                    gna_names = [
                        d.get("scientificName") or d.get("verbatimName", "")
                        for d in gna_raw
                        if (d.get("scientificName") or d.get("verbatimName", "")).strip()
                    ]
                else:
                    gna_names = [str(n) for n in gna_raw if str(n).strip()]
                found.extend(gna_names)
                log_cb(f"    [GNA Finder] {len(gna_names)} names detected")""",
    ),

    # ── P2: Filter __candidate_* from thinker deduplication loop ──────────────
    Patch(
        name="P2-candidate-filter-thinker",
        description="Filter __candidate_* placeholder names from thinker output (line ~866)",
        old="""\
    seen:   set[str]  = set()
    unique: list[str] = []
    for n in found:
        n_strip = n.strip()
        if n_strip and n_strip not in seen:
            seen.add(n_strip)
            unique.append(n_strip)

    if unique:
        log_cb(f"    🔍 thinker: {len(unique)} unique candidate names")
    return unique""",
        new="""\
    # PATCHED: P2-candidate-filter-thinker — drop NER placeholder IDs and non-taxon strings
    _CAND_ID_RE = re.compile(r"^__candidate_\d+_\d+$")
    _TAXON_START_RE = re.compile(r"^[A-Z][a-z]{2,}")

    seen:   set[str]  = set()
    unique: list[str] = []
    for n in found:
        n_strip = n.strip()
        if (n_strip
                and n_strip not in seen
                and not _CAND_ID_RE.match(n_strip)
                and _TAXON_START_RE.match(n_strip)):
            seen.add(n_strip)
            unique.append(n_strip)

    if unique:
        log_cb(f"    🔍 thinker: {len(unique)} unique candidate names")
    return unique""",
    ),

    # ── P3: Purge __candidate_* from progress tracker before final render ──────
    Patch(
        name="P3-tracker-purge",
        description="Purge __candidate_* from tracker before final progress render (line ~2390)",
        old="""\
            with progress_placeholder.container():
                render_species_progress_panel(log_inst.tracker)
            
            log_cb(f"[DB] {n} records saved (session {session_id})")

            # ── Step 9: v5 knowledge systems ─────────────────────────────────""",
        new="""\
            # PATCHED: P3-tracker-purge — remove NER placeholder IDs before render
            if hasattr(log_inst, 'tracker') and hasattr(log_inst.tracker, 'species'):
                _CAND_PURGE_RE = re.compile(r"^__candidate_\\d+_\\d+$")
                log_inst.tracker.species = {
                    k: v for k, v in log_inst.tracker.species.items()
                    if not _CAND_PURGE_RE.match(str(k))
                }

            with progress_placeholder.container():
                render_species_progress_panel(log_inst.tracker)
            
            log_cb(f"[DB] {n} records saved (session {session_id})")

            # ── Step 9: v5 knowledge systems ─────────────────────────────────""",
    ),

    # ── P4: Add Checklist Mode + HITL toggles to extraction UI ────────────────
    Patch(
        name="P4-checklist-hitl-toggles",
        description="Add checklist mode and HITL approval toggles to Extract tab (line ~2160)",
        old="""\
    col_a, col_b, col_c = st.columns([3,1,1])
    with col_a:
        doc_title = st.text_input(
            "Document Title / Citation (auto-filled from metadata):",
            value=st.session_state.get('auto_title', '') if uploaded else ''
        )
    with col_b:
        primary_only = st.checkbox("Primary records only", value=False)
    with col_c:
        do_split_loc = st.checkbox("Split localities", value=True,
                                   help="Expand 'Site A, B and C' → 3 records")""",
        new="""\
    # PATCHED: P4-checklist-hitl-toggles — add checklist mode and HITL approval
    col_a, col_b, col_c = st.columns([3,1,1])
    with col_a:
        doc_title = st.text_input(
            "Document Title / Citation (auto-filled from metadata):",
            value=st.session_state.get('auto_title', '') if uploaded else ''
        )
    with col_b:
        primary_only = st.checkbox("Primary records only", value=False)
        is_checklist = st.checkbox(
            "📋 Checklist paper mode",
            value=False,
            help="Keeps 'cf.', 'sp.', and authority forms as separate entries. "
                 "Use for annotated checklists where the table lists them distinctly.",
            key="is_checklist",
        )
    with col_c:
        do_split_loc = st.checkbox("Split localities", value=True,
                                   help="Expand 'Site A, B and C' → 3 records")
        use_hitl = st.checkbox(
            "🔬 Approve before saving",
            value=True,
            help="HITL gate: review + approve species before they enter DB/KG/Memory.",
            key="use_hitl_approval",
        )""",
    ),

    # ── P5: Pass checklist_mode to suppress_regional_duplicates ───────────────
    Patch(
        name="P5-checklist-mode-suppress",
        description="Pass checklist_mode flag to suppress_regional_duplicates (line ~1704)",
        old="""\
    results, n_suppressed = suppress_regional_duplicates(results)
    if n_suppressed:
        log_cb(f"[Dedup/Stage3] Suppressed {n_suppressed} regional-level duplicates")""",
        new="""\
    # PATCHED: P5-checklist-mode-suppress — honour checklist mode in stage-3 dedup
    _checklist_mode = st.session_state.get("is_checklist", False) if 'st' in dir() else False
    results, n_suppressed = suppress_regional_duplicates(
        results, checklist_mode=_checklist_mode
    )
    if n_suppressed:
        log_cb(f"[Dedup/Stage3] Suppressed {n_suppressed} regional-level duplicates"
               f" (checklist_mode={_checklist_mode})")""",
    ),

    # ── P6: Fix PDF backup double-save (gate with exists check) ───────────────
    Patch(
        name="P6-pdf-backup-gate",
        description="Gate PDF backup write with os.path.exists check (line ~2118)",
        old="""\
            backup_dir = os.path.join(DATA_DIR, "backup_manuscripts")
            os.makedirs(backup_dir, exist_ok=True)
            clean_name = re.sub(r'[\\\\/*?:"<>|]', "", st.session_state['auto_title'])
            file_ext = os.path.splitext(uploaded.name)[1]
            backup_path = os.path.join(backup_dir, f"{clean_name}{file_ext}")
            
            with open(backup_path, "wb") as f:
                f.write(uploaded.getvalue())
            st.session_state['backup_path'] = backup_path""",
        new="""\
            # PATCHED: P6-pdf-backup-gate — use original filename; skip if already saved
            backup_dir = os.path.join(DATA_DIR, "backup_manuscripts")
            os.makedirs(backup_dir, exist_ok=True)
            # Use the original uploaded filename (not auto_title which may be blank)
            safe_backup_name = re.sub(r'[\\\\/*?:"<>|]', "_", uploaded.name)
            backup_path = os.path.join(backup_dir, safe_backup_name)
            if not os.path.exists(backup_path):
                with open(backup_path, "wb") as f:
                    f.write(uploaded.getvalue())
                logger.info("[upload] Backup saved: %s", backup_path)
            else:
                logger.debug("[upload] Backup already exists, skipping: %s", backup_path)
            st.session_state['backup_path'] = backup_path""",
    ),

    # ── P7: Use file hash (not timestamp) for pdfs_v5 filename ────────────────
    Patch(
        name="P7-pdf-hash-filename",
        description="Use content hash not timestamp for pdfs_v5 filename (line ~2216)",
        old="""\
            safe_title = re.sub(r'[\\\\/*?:"<>|]', "", clean_title)
            filename = f"{safe_title}_{ts}{suffix}"
            tmp_path = os.path.join(PDF_DIR, filename)""",
        new="""\
            # PATCHED: P7-pdf-hash-filename — content hash prevents re-extraction duplicates
            safe_title = re.sub(r'[\\\\/*?:"<>|]', "_", clean_title or Path(uploaded.name).stem)
            # Pre-compute hash here so we can use it in the filename
            _pre_hash = hashlib.sha256(uploaded.getvalue()).hexdigest()[:8]
            filename = f"{safe_title}_{_pre_hash}{suffix}"
            tmp_path = os.path.join(PDF_DIR, filename)""",
    ),

    # ── P8: Wire GBIF + HITL approval gate (after primary filter, before geocode)
    Patch(
        name="P8-gbif-hitl-gate",
        description="Insert GBIF verification + HITL approval gate after primary filter (line ~2330)",
        old="""\
            # [ENHANCEMENT: biotrace_col_client] — Stage 5: COL taxonomy enrichment""",
        new="""\
            # PATCHED: P8-gbif-hitl-gate — GBIF verification + HITL approval before DB insert
            if st.session_state.get("use_hitl_approval", True):
                try:
                    from biotrace_gbif_verifier import gbif_verify_batch, render_approval_table
                    log_cb("[GBIF] Verifying species against GBIF Backbone Taxonomy…")
                    occurrences = gbif_verify_batch(occurrences, min_confidence=80)
                    n_auto = sum(1 for o in occurrences if o.get("gbifApproved"))
                    log_cb(f"[GBIF] {n_auto}/{len(occurrences)} auto-approved")

                    approved = render_approval_table(occurrences)
                    if approved is None:
                        st.stop()   # wait for biologist to confirm
                    occurrences = approved
                    log_cb(f"[HITL] {len(occurrences)} species approved for DB/KG/Memory")
                except ImportError:
                    log_cb("[GBIF] biotrace_gbif_verifier.py not found — skipping HITL gate",
                           "warn")

            # [ENHANCEMENT: biotrace_col_client] — Stage 5: COL taxonomy enrichment""",
    ),

    # ── P9: Wire agent loop for self-correcting extraction ────────────────────
    Patch(
        name="P9-agent-loop",
        description="Wire agent extraction loop for self-correction (line ~2287)",
        old="""\
            log_cb("[Extract] Running v5.3 hierarchical extraction…")
            occurrences = extract_occurrences(
                md_text, doc_title, provider, model_sel,
                api_key, ollama_url, log_cb,
                chunk_strategy   = chunk_strategy,
                chunk_chars      = chunk_chars,
                overlap_chars    = overlap_chars,
                batch_mode       = False,
                citation_string  = citation_str,
                use_hierarchical = use_hierarchical,
                use_scientific   = use_scientific,      # ← ADD THIS LINE
                use_thinker      = use_thinker_cb,
                use_auto_loc_ner = use_auto_loc and _LOC_NER_AVAILABLE,
                geonames_db      = GEONAMES_DB,
            )""",
        new="""\
            # PATCHED: P9-agent-loop — self-correcting extraction with species-count check
            log_cb("[Extract] Running v5.3 hierarchical extraction…")

            def _run_standard_extraction(text_input):
                return extract_occurrences(
                    text_input, doc_title, provider, model_sel,
                    api_key, ollama_url, log_cb,
                    chunk_strategy   = chunk_strategy,
                    chunk_chars      = chunk_chars,
                    overlap_chars    = overlap_chars,
                    batch_mode       = False,
                    citation_string  = citation_str,
                    use_hierarchical = use_hierarchical,
                    use_scientific   = use_scientific,
                    use_thinker      = use_thinker_cb,
                    use_auto_loc_ner = use_auto_loc and _LOC_NER_AVAILABLE,
                    geonames_db      = GEONAMES_DB,
                )

            try:
                from biotrace_agent_loop import agent_extract_with_correction
                _llm_partial = lambda p: call_llm(p, provider, model_sel, api_key, ollama_url)
                occurrences = agent_extract_with_correction(
                    full_text  = md_text,
                    extract_fn = _run_standard_extraction,
                    llm_fn     = _llm_partial,
                    log_cb     = log_cb,
                    max_retries = 2,
                )
            except ImportError:
                log_cb("[Agent] biotrace_agent_loop.py not found — standard extraction", "warn")
                occurrences = _run_standard_extraction(md_text)""",
    ),

]


# ─────────────────────────────────────────────────────────────────────────────
#  PATCH ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _is_applied(content: str, patch: Patch) -> bool:
    """A patch is considered already applied if its sentinel comment is present."""
    return f"# PATCHED: {patch.name}" in content


def _apply_patch(content: str, patch: Patch, verbose: bool = True) -> tuple[str, bool]:
    """
    Apply a single patch. Returns (new_content, was_applied).
    If 'old' string not found, logs a warning and returns content unchanged.
    """
    if _is_applied(content, patch):
        if verbose:
            print(f"  ⏭️  {patch.name}: already applied, skipping")
        return content, False

    if patch.old not in content:
        print(f"  ⚠️  {patch.name}: target string NOT found — check for version mismatch")
        print(f"     First 80 chars of old: {repr(patch.old[:80])}")
        return content, False

    new_content = content.replace(patch.old, patch.new, 1)
    if verbose:
        print(f"  ✅ {patch.name}: applied — {patch.description}")
    return new_content, True


def apply_all_patches(
    target:  Path   = TARGET_FILE,
    dry_run: bool   = False,
    backup:  bool   = True,
    verbose: bool   = True,
) -> int:
    """
    Apply all patches to target file. Returns count of patches applied.
    """
    if not target.exists():
        print(f"❌ Target file not found: {target}")
        sys.exit(1)

    content = target.read_text(encoding="utf-8")
    applied = 0

    print(f"\n{'DRY RUN — ' if dry_run else ''}Applying {len(PATCHES)} patches to {target}\n")
    print("─" * 60)

    for patch in PATCHES:
        new_content, was_applied = _apply_patch(content, patch, verbose=verbose)
        if was_applied:
            content = new_content
            applied += 1

    print("─" * 60)
    print(f"\n{applied}/{len(PATCHES)} patches applied.")

    if applied and not dry_run:
        if backup:
            bak = target.with_suffix(".py.bak")
            shutil.copy(target, bak)
            print(f"📦 Backup saved: {bak}")
        target.write_text(content, encoding="utf-8")
        print(f"✅ Patched file written: {target}")
    elif dry_run:
        print("ℹ️  Dry run — no files written.")

    return applied


# ─────────────────────────────────────────────────────────────────────────────
#  ALSO: sidebar status update (adds new modules to the status panel)
# ─────────────────────────────────────────────────────────────────────────────

SIDEBAR_PATCH = Patch(
    name="SIDEBAR-new-module-status",
    description="Add GBIF verifier + agent loop to sidebar status panel",
    old="""\
    _status(True,  "biotrace_relation_extractor.py",        "biotrace_relation_extractor.py",        is_local=True)
    _status(True,  "biotrace_kg_spatio_temporal.py",        "biotrace_kg_spatio_temporal.py",        is_local=True)""",
    new="""\
    _status(True,  "biotrace_relation_extractor.py",        "biotrace_relation_extractor.py",        is_local=True)
    _status(True,  "biotrace_kg_spatio_temporal.py",        "biotrace_kg_spatio_temporal.py",        is_local=True)
    # PATCHED: SIDEBAR-new-module-status
    _status(True,  "biotrace_gbif_verifier.py",             "biotrace_gbif_verifier.py",             is_local=True)
    _status(True,  "biotrace_agent_loop.py",                "biotrace_agent_loop.py",                is_local=True)""",
)
PATCHES.append(SIDEBAR_PATCH)


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Apply BioTrace v5.4 patches to biotrace_v5.py"
    )
    parser.add_argument(
        "--target", type=Path, default=TARGET_FILE,
        help=f"Path to biotrace_v5.py (default: {TARGET_FILE})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to disk"
    )
    parser.add_argument(
        "--no-backup", action="store_true",
        help="Skip creating .bak backup file"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List all available patches and exit"
    )
    args = parser.parse_args()

    if args.list:
        print(f"\n{'#':<4} {'Name':<35} Description")
        print("─" * 90)
        for i, p in enumerate(PATCHES, 1):
            print(f"{i:<4} {p.name:<35} {p.description}")
        sys.exit(0)

    n = apply_all_patches(
        target  = args.target,
        dry_run = args.dry_run,
        backup  = not args.no_backup,
    )
    sys.exit(0 if n >= 0 else 1)
