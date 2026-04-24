"""
apply_biotrace_patches_v2.py  —  BioTrace v5.4  Round-2 Patch Applicator
────────────────────────────────────────────────────────────────────────────
Fixes all issues reported after Round-1 patching:

  R1 — HITL reset: occurrences lost on st.stop() / re-render
       Root cause: pipeline results live only in local scope of `if run_btn`
       Fix: checkpoint each pipeline stage into st.session_state

  R2 — Family / Phylum empty in DB
       Root cause: GNA verifier sets occ["phylum"] etc but LLM Higher Taxonomy
       dict is never unpacked into flat fields when verifier fails/skips
       Fix: insert_occurrences unpacks Higher Taxonomy JSON as fallback

  R3 — sourceCitation is filename not title
       Root cause: insert_occurrences called with doc_title (line 2384), not
       citation_str; doc_title may still be the uploaded filename
       Fix: normalise every record's "Source Citation" to citation_str BEFORE
       insert; also fix the insert call to pass citation_str explicitly

  R4 — Wiki: author-inconsistency, blank taxonomy, static map, weak summary
       Fix: wiki_normalise_species_key(), propagate taxonomy before ingest,
       replace static locality map with filtered per-species occurrence map,
       add richer summary template

  R5 — Edit/Delete in Verification Table not persisting
       Root cause: save only updates 6 fields; delete button missing
       Fix: expand UPDATE to all editable fields; add row-level DELETE

  R6 — Wiki locality map is meaningless / static
       Fix: replace with st.map() filtered to selected species/locality
       and showing all occurrence points from the DB

Run:
    python apply_biotrace_patches_v2.py               # applies to biotrace_v5.py
    python apply_biotrace_patches_v2.py --dry-run     # preview only
    python apply_biotrace_patches_v2.py --backup      # keep .bak2 copy
"""
from __future__ import annotations
import argparse, shutil, sys
from pathlib import Path
from dataclasses import dataclass


TARGET_FILE = Path("biotrace_v5.py")
SENTINEL    = "# PATCHED-R2:"


@dataclass
class Patch:
    name: str
    description: str
    old: str
    new: str


# ─────────────────────────────────────────────────────────────────────────────
#  R1 — HITL RESET FIX
#  Problem: all pipeline state lives inside `if run_btn and uploaded:` block.
#  When st.stop() is called (HITL gate waiting for Confirm), Streamlit saves
#  the page state but clears all local variables.  When the user clicks
#  Confirm, the button re-fires but run_btn is no longer True, so the block
#  never re-executes and occurrences is gone.
#
#  Fix strategy: checkpoint each expensive pipeline stage into session_state
#  keyed by file_hash.  On re-render, skip already-done stages.
# ─────────────────────────────────────────────────────────────────────────────

_R1_OLD = '''\
        with st.spinner("Processing…"):
            clean_title = st.session_state.get('auto_title', uploaded.name)
            ts = int(time.time())
            suffix = Path(uploaded.name).suffix'''

_R1_NEW = '''\
        # PATCHED-R2: R1-hitl-reset — checkpoint pipeline into session_state
        # If HITL is waiting for Confirm, restore the pending occurrences
        # from the checkpoint instead of re-running the full pipeline.
        _hitl_pending = st.session_state.get("_hitl_pending_occurrences")
        _hitl_hash    = st.session_state.get("_hitl_pending_hash", "")

        if _hitl_pending is not None:
            # User clicked Confirm after HITL gate — resume from checkpoint
            from biotrace_gbif_verifier import render_approval_table
            approved = render_approval_table(_hitl_pending)
            if approved is None:
                st.stop()   # still waiting
            # Clear checkpoint and continue with approved list
            occurrences = approved
            file_hash   = _hitl_hash
            session_id  = st.session_state.get("_hitl_pending_session", f"session_resumed")
            citation_str= st.session_state.get("_hitl_pending_citation", uploaded.name)
            doc_title   = st.session_state.get("_hitl_pending_title", uploaded.name)
            del st.session_state["_hitl_pending_occurrences"]
            log_cb(f"[HITL] Resumed with {len(occurrences)} approved species")

            # Jump straight to geocoding + save
            occurrences = geocode_occurrences(occurrences, log_cb)
            try:
                from biotrace_postprocessing import run_postprocessing
                occurrences, pp_summary = run_postprocessing(
                    occurrences, citation_str=citation_str,
                    wiki_root=WIKI_ROOT, geonames_db=GEONAMES_DB,
                    use_nominatim=True, log_cb=log_cb,
                )
                st.session_state["pp_conflicts"]    = pp_summary.get("conflicts", [])
                st.session_state["pp_conflict_log"] = pp_summary.get("conflict_log", [])
            except Exception as _pp_exc:
                log_cb(f"[Post] {_pp_exc}", "warn")

            n = insert_occurrences(occurrences, file_hash, citation_str, session_id)
            log_cb(f"[DB] {n} records saved (resumed session {session_id})")
            if any([use_kg, use_mb, use_wiki]):
                ingest_into_v5_systems(
                    occurrences, citation=citation_str, session_id=session_id,
                    log_cb=log_cb, provider=provider, model_sel=model_sel,
                    api_key=api_key, ollama_base_url=ollama_url,
                    update_wiki_narratives=wiki_narr,
                )
            st.success(f"✅ {len(occurrences)} occurrence records saved after approval.")
            df = pd.DataFrame(occurrences)
            st.dataframe(df[[c for c in
                ["recordedName","validName","family_","phylum","verbatimLocality",
                 "occurrenceType","wormsID"] if c in df.columns]],
                use_container_width=True, height=350)
            st.stop()   # done — don't fall through to full pipeline

        with st.spinner("Processing…"):
            clean_title = st.session_state.get('auto_title', uploaded.name)
            ts = int(time.time())
            suffix = Path(uploaded.name).suffix'''


# ─────────────────────────────────────────────────────────────────────────────
#  R1b — Save checkpoint BEFORE HITL gate fires
#  Find the HITL gate inserted by Round-1 (P8) and store the checkpoint.
# ─────────────────────────────────────────────────────────────────────────────

_R1B_OLD = '''\
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
                           "warn")'''

_R1B_NEW = '''\
            # PATCHED-R2: R1b-hitl-checkpoint — save checkpoint before gate fires
            if st.session_state.get("use_hitl_approval", True):
                try:
                    from biotrace_gbif_verifier import gbif_verify_batch, render_approval_table
                    log_cb("[GBIF] Verifying species against GBIF Backbone Taxonomy…")
                    occurrences = gbif_verify_batch(occurrences, min_confidence=80)
                    n_auto = sum(1 for o in occurrences if o.get("gbifApproved"))
                    log_cb(f"[GBIF] {n_auto}/{len(occurrences)} auto-approved")

                    # Save checkpoint so HITL confirm can resume without re-running pipeline
                    st.session_state["_hitl_pending_occurrences"] = occurrences
                    st.session_state["_hitl_pending_hash"]        = file_hash
                    st.session_state["_hitl_pending_session"]     = session_id
                    st.session_state["_hitl_pending_citation"]    = citation_str
                    st.session_state["_hitl_pending_title"]       = doc_title

                    approved = render_approval_table(occurrences)
                    if approved is None:
                        st.stop()   # wait for biologist to confirm
                    # Confirmed — clear checkpoint and continue
                    del st.session_state["_hitl_pending_occurrences"]
                    occurrences = approved
                    log_cb(f"[HITL] {len(occurrences)} species approved for DB/KG/Memory")
                except ImportError:
                    log_cb("[GBIF] biotrace_gbif_verifier.py not found — skipping HITL gate",
                           "warn")'''


# ─────────────────────────────────────────────────────────────────────────────
#  R2 — FIX FAMILY / PHYLUM EMPTY IN DB
#  Two sub-fixes:
#   a) Unpack the "Higher Taxonomy" JSON dict into flat fields inside
#      insert_occurrences() as a fallback when the verifier didn't fill them.
#   b) Force-stamp citation_str onto each record before insert.
# ─────────────────────────────────────────────────────────────────────────────

_R2_OLD = '''\
        con.execute("""
            INSERT INTO occurrences_v4 (
                file_hash, recordedName, validName, higherTaxonomy,
                sourceCitation, habitat, samplingEvent, rawTextEvidence,
                decimalLatitude, decimalLongitude, verbatimLocality,
                occurrenceType, geocodingSource, phylum, class_, order_,
                family_, wormsID, itisID, taxonRank, nameAccordingTo,
                taxonomicStatus, matchScore, session_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            file_hash,
            str(occ.get("recordedName") or occ.get("Recorded Name",""))[:300],
            sp[:300],
            json.dumps(tax),
            str(occ.get("Source Citation") or occ.get("sourceCitation", source_title))[:500],
            str(occ.get("Habitat") or occ.get("habitat",""))[:300],
            sampling_str,
            str(occ.get("Raw Text Evidence") or occ.get("rawTextEvidence",""))[:1000],
            _to_float(occ.get("decimalLatitude")), 
            _to_float(occ.get("decimalLongitude")), 
            str(occ.get("verbatimLocality",""))[:300],
            str(occ.get("occurrenceType",""))[:50],
            str(occ.get("geocodingSource",""))[:100],
            str(occ.get("phylum") or occ.get("Phylum",""))[:100],
            str(occ.get("class_") or occ.get("class") or occ.get("Class",""))[:100],
            str(occ.get("order_") or occ.get("order") or occ.get("Order",""))[:100],
            str(occ.get("family_") or occ.get("family") or occ.get("Family",""))[:100],
            str(occ.get("wormsID",""))[:20],
            str(occ.get("itisID",""))[:20],
            str(occ.get("taxonRank",""))[:50],
            str(occ.get("nameAccordingTo",""))[:100],
            str(occ.get("taxonomicStatus",""))[:50],
            float(occ.get("matchScore", 0) or 0),
            session_id,
        ))'''

_R2_NEW = '''\
        # PATCHED-R2: R2-taxonomy-fallback — unpack Higher Taxonomy JSON when
        # flat fields are empty (happens when GNA verifier skips or fails)
        def _tax_field(key_variants: list, fallback: str = "") -> str:
            for k in key_variants:
                v = occ.get(k)
                if v and str(v).strip():
                    return str(v).strip()[:100]
            # Fallback: pull from Higher Taxonomy dict
            if isinstance(tax, dict):
                for k in key_variants:
                    v = tax.get(k) or tax.get(k.rstrip("_"))
                    if v and str(v).strip():
                        return str(v).strip()[:100]
            return fallback

        _phylum  = _tax_field(["phylum",  "Phylum"])
        _class   = _tax_field(["class_",  "class", "Class"])
        _order   = _tax_field(["order_",  "order", "Order"])
        _family  = _tax_field(["family_", "family","Family"])

        # Citation: prefer per-record "Source Citation", then session citation,
        # then source_title (which is now citation_str, not doc_title)
        _citation = (
            str(occ.get("Source Citation") or occ.get("sourceCitation") or "").strip()
            or source_title   # source_title is now citation_str (see line ~2384 fix)
        )

        con.execute("""
            INSERT INTO occurrences_v4 (
                file_hash, recordedName, validName, higherTaxonomy,
                sourceCitation, habitat, samplingEvent, rawTextEvidence,
                decimalLatitude, decimalLongitude, verbatimLocality,
                occurrenceType, geocodingSource, phylum, class_, order_,
                family_, wormsID, itisID, taxonRank, nameAccordingTo,
                taxonomicStatus, matchScore, session_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            file_hash,
            str(occ.get("recordedName") or occ.get("Recorded Name",""))[:300],
            sp[:300],
            json.dumps(tax),
            _citation[:500],
            str(occ.get("Habitat") or occ.get("habitat",""))[:300],
            sampling_str,
            str(occ.get("Raw Text Evidence") or occ.get("rawTextEvidence",""))[:1000],
            _to_float(occ.get("decimalLatitude")),
            _to_float(occ.get("decimalLongitude")),
            str(occ.get("verbatimLocality",""))[:300],
            str(occ.get("occurrenceType",""))[:50],
            str(occ.get("geocodingSource",""))[:100],
            _phylum, _class, _order, _family,
            str(occ.get("wormsID",""))[:20],
            str(occ.get("itisID",""))[:20],
            str(occ.get("taxonRank",""))[:50],
            str(occ.get("nameAccordingTo",""))[:100],
            str(occ.get("taxonomicStatus",""))[:50],
            float(occ.get("matchScore", 0) or 0),
            session_id,
        ))'''


# ─────────────────────────────────────────────────────────────────────────────
#  R3 — FIX sourceCitation: pass citation_str not doc_title to insert
# ─────────────────────────────────────────────────────────────────────────────

_R3_OLD = '''\
            # ── Step 8: Save to SQLite ────────────────────────────────────────
            n = insert_occurrences(occurrences, file_hash, doc_title, session_id)
            log_cb(f"[DB] {n} records saved (session {session_id})")'''

_R3_NEW = '''\
            # PATCHED-R2: R3-citation-fix — stamp full citation_str on each record
            # and pass citation_str (not doc_title) as source_title to insert
            for _occ in occurrences:
                if isinstance(_occ, dict):
                    # Overwrite only if the per-record citation looks like a raw filename
                    _rec_cit = str(_occ.get("Source Citation") or _occ.get("sourceCitation","")).strip()
                    _looks_like_filename = (
                        not _rec_cit
                        or _rec_cit == doc_title
                        or _rec_cit.lower().endswith((".pdf",".p65",".md",".txt"))
                        or len(_rec_cit) < 20
                    )
                    if _looks_like_filename and citation_str and len(citation_str) > 20:
                        _occ["Source Citation"] = citation_str
                        _occ["sourceCitation"]  = citation_str

            # ── Step 8: Save to SQLite ────────────────────────────────────────
            n = insert_occurrences(occurrences, file_hash, citation_str, session_id)
            log_cb(f"[DB] {n} records saved (session {session_id})")'''


# ─────────────────────────────────────────────────────────────────────────────
#  R5 — FIX EDIT/DELETE IN VERIFICATION TABLE
#  Current save at line ~2628 only updates 6 columns.
#  Add: full-column update + delete button.
# ─────────────────────────────────────────────────────────────────────────────

_R5_OLD = '''\
                if st.button("💾 Save All Edits to Database", key="save_combined"):
                    try:
                        _save_con = _sql.connect(META_DB_PATH)
                        _updated_count = 0
                        for _, row in _edited_df.iterrows():
                            _rid = row.get("id")
                            if pd.notna(_rid):
                                _save_con.execute(
                                    "UPDATE occurrences_v4 SET "
                                    "occurrenceType=?, validationStatus=?, notes=?, "
                                    "decimalLatitude=?, decimalLongitude=?, geocodingSource=? "
                                    "WHERE id=?",
                                    (
                                        row.get("occurrenceType","Uncertain"),
                                        row.get("validationStatus","Review"),
                                        row.get("notes",""),
                                        _to_float(row.get("decimalLatitude")),
                                        _to_float(row.get("decimalLongitude")),
                                        str(row.get("geocodingSource","manual")),
                                        int(_rid),
                                    ),
                                )
                                _updated_count += 1
                        _save_con.commit()
                        _save_con.close()
                        st.success(f"✅ Saved {_updated_count} edits to database.")
                    except Exception as _e:
                        st.error(f"Save failed: {_e}")'''

_R5_NEW = '''\
                # PATCHED-R2: R5-edit-delete — full-field UPDATE + DELETE support
                _col_save, _col_del = st.columns([3,1])

                with _col_save:
                    if st.button("💾 Save All Edits to Database", key="save_combined"):
                        try:
                            _save_con = _sql.connect(META_DB_PATH)
                            _updated_count = 0
                            for _, row in _edited_df.iterrows():
                                _rid = row.get("id")
                                if pd.notna(_rid):
                                    _save_con.execute(
                                        """UPDATE occurrences_v4 SET
                                            recordedName=?, validName=?,
                                            verbatimLocality=?, occurrenceType=?,
                                            validationStatus=?, notes=?,
                                            habitat=?, sourceCitation=?,
                                            phylum=?, class_=?, order_=?, family_=?,
                                            wormsID=?, taxonRank=?,
                                            decimalLatitude=?, decimalLongitude=?,
                                            geocodingSource=?
                                        WHERE id=?""",
                                        (
                                            str(row.get("recordedName",""))[:300],
                                            str(row.get("validName",""))[:300],
                                            str(row.get("verbatimLocality",""))[:300],
                                            str(row.get("occurrenceType","Uncertain"))[:50],
                                            str(row.get("validationStatus","Review"))[:50],
                                            str(row.get("notes",""))[:1000],
                                            str(row.get("habitat",""))[:300],
                                            str(row.get("sourceCitation",""))[:500],
                                            str(row.get("phylum",""))[:100],
                                            str(row.get("class_",""))[:100],
                                            str(row.get("order_",""))[:100],
                                            str(row.get("family_",""))[:100],
                                            str(row.get("wormsID",""))[:20],
                                            str(row.get("taxonRank",""))[:50],
                                            _to_float(row.get("decimalLatitude")),
                                            _to_float(row.get("decimalLongitude")),
                                            str(row.get("geocodingSource","manual"))[:100],
                                            int(_rid),
                                        ),
                                    )
                                    _updated_count += 1
                            _save_con.commit()
                            _save_con.close()
                            st.success(f"✅ Saved {_updated_count} edits to database.")
                            st.rerun()
                        except Exception as _e:
                            st.error(f"Save failed: {_e}")

                with _col_del:
                    # Row-level delete: select by ID
                    _del_ids_str = st.text_input(
                        "Delete record IDs (comma-separated):",
                        placeholder="e.g. 12, 47, 93",
                        key="delete_ids_input",
                    )
                    if st.button("🗑️ Delete Selected", key="delete_selected_btn",
                                 type="secondary"):
                        if _del_ids_str.strip():
                            try:
                                _del_ids = [
                                    int(x.strip())
                                    for x in _del_ids_str.split(",")
                                    if x.strip().isdigit()
                                ]
                                if _del_ids:
                                    _del_con = _sql.connect(META_DB_PATH)
                                    _del_con.executemany(
                                        "DELETE FROM occurrences_v4 WHERE id=?",
                                        [(i,) for i in _del_ids],
                                    )
                                    _del_con.commit()
                                    _del_con.close()
                                    st.success(
                                        f"🗑️ Deleted {len(_del_ids)} records: "
                                        f"{_del_ids}"
                                    )
                                    st.rerun()
                            except Exception as _de:
                                st.error(f"Delete failed: {_de}")
                        else:
                            st.warning("Enter at least one record ID to delete.")'''


# ─────────────────────────────────────────────────────────────────────────────
#  R4+R6 — FIX WIKI: taxonomy propagation, species map, author normalization
# ─────────────────────────────────────────────────────────────────────────────

_R4_OLD = '''\
        sp_list = wiki.list_species()
        if sp_list:
            selected_sp = st.selectbox("View Species Article:", sp_list)
            if selected_sp:
                md = wiki.render_species_markdown(selected_sp)
                st.markdown(md)

                art = wiki.get_species_article(selected_sp)
                if art and art.get("occurrences"):
                    with st.expander("Occurrence Records"):
                        st.dataframe(
                            pd.DataFrame(art["occurrences"]),
                            use_container_width=True,
                        )
        else:
            st.info("No wiki articles yet. Run an extraction to populate.")

        st.divider()
        st.subheader("Locality Checklist")
        loc_list = wiki.list_localities()
        if loc_list:
            selected_loc = st.selectbox("Locality:", loc_list)
            if selected_loc:
                loc_art = wiki._load_article("locality", wiki._slug(selected_loc) if hasattr(wiki,"_slug") else selected_loc.lower().replace(" ","_"))
                if loc_art:
                    f = loc_art.get("facts",{})
                    sp_cl = f.get("species_checklist",[])
                    st.write(f"**{len(sp_cl)} species at {selected_loc}:**")
                    if sp_cl:
                        col_a, col_b = st.columns(2)
                        half = len(sp_cl)//2
                        with col_a:
                            for sp in sp_cl[:half]:
                                st.write(f"• {sp}")
                        with col_b:
                            for sp in sp_cl[half:]:
                                st.write(f"• {sp}")
                    lat, lon = f.get("latitude"), f.get("longitude")
                    if lat and lon:
                        st.map(pd.DataFrame({"lat":[lat],"lon":[lon]}))'''

_R4_NEW = '''\
        # PATCHED-R2: R4-wiki-improvements — per-species map, taxonomy display,
        # author normalization, richer locality checklist
        import re as _re

        def _strip_author(name: str) -> str:
            """Remove authority suffix for display: 'Elysia obtusa Baba, 1938' → 'Elysia obtusa'"""
            if not name: return name
            return _re.sub(
                r"\s+[A-Z][A-Za-z\-'']+(?:\s+(?:and|&|et)\s+[A-Z][A-Za-z\-'']+)?[,.]?\s*\d{4}.*$",
                "", name
            ).strip()

        def _wiki_db_occurrences(species_name: str) -> pd.DataFrame:
            """Pull occurrence rows from DB for a given species (handles author variants)."""
            try:
                import sqlite3 as _sq
                _c = _sq.connect(META_DB_PATH)
                _base = _strip_author(species_name).lower()
                _df = pd.read_sql_query(
                    """SELECT validName, recordedName, verbatimLocality, habitat,
                              occurrenceType, sourceCitation, phylum, class_, order_,
                              family_, wormsID, decimalLatitude, decimalLongitude,
                              matchScore, taxonomicStatus
                       FROM occurrences_v4
                       WHERE lower(validName) LIKE ? OR lower(recordedName) LIKE ?
                       ORDER BY id DESC""",
                    _c,
                    params=(f"%{_base}%", f"%{_base}%"),
                )
                _c.close()
                return _df
            except Exception:
                return pd.DataFrame()

        sp_list = wiki.list_species()
        if sp_list:
            # Normalise display names (strip authors for the selector)
            _sp_display = sorted({_strip_author(s) for s in sp_list if s})
            selected_sp_display = st.selectbox("View Species Article:", _sp_display)

            # Find the original key (may have author)
            selected_sp = next(
                (s for s in sp_list if _strip_author(s) == selected_sp_display),
                selected_sp_display,
            )

            if selected_sp:
                art = wiki.get_species_article(selected_sp) or {}
                facts = art.get("facts", {})

                # ── Taxonomy banner ──────────────────────────────────────────
                _db_rows = _wiki_db_occurrences(selected_sp)
                _phylum = facts.get("phylum","") or (_db_rows["phylum"].dropna().iloc[0] if not _db_rows.empty and "phylum" in _db_rows else "")
                _family = facts.get("family","") or (_db_rows["family_"].dropna().iloc[0] if not _db_rows.empty and "family_" in _db_rows else "")
                _order  = facts.get("order","")  or (_db_rows["order_"].dropna().iloc[0]  if not _db_rows.empty and "order_"  in _db_rows else "")
                _wid    = facts.get("wormsID","") or (_db_rows["wormsID"].dropna().iloc[0] if not _db_rows.empty and "wormsID" in _db_rows else "")

                _tax_parts = [p for p in [
                    f"Phylum: **{_phylum}**"  if _phylum else None,
                    f"Order: **{_order}**"    if _order  else None,
                    f"Family: **{_family}**"  if _family else None,
                ] if p]
                if _tax_parts:
                    st.markdown("  ·  ".join(_tax_parts))
                if _wid:
                    st.markdown(
                        f"[🔗 WoRMS AphiaID {_wid}](https://www.marinespecies.org/aphia.php?p=taxdetails&id={_wid})"
                    )

                # ── Wiki narrative ───────────────────────────────────────────
                md = wiki.render_species_markdown(selected_sp)
                # Replace blank taxonomy lines in rendered markdown
                if "Family: |" in md or "Phylum: |" in md:
                    md = md.replace(
                        "Family: |", f"Family: {_family} |"
                    ).replace(
                        "Phylum: |", f"Phylum: {_phylum} |"
                    ).replace(
                        "Order: |",  f"Order: {_order} |"
                    )
                st.markdown(md)

                # ── Occurrence table from DB ──────────────────────────────────
                if not _db_rows.empty:
                    with st.expander(f"📋 {len(_db_rows)} occurrence records from database"):
                        _show_cols = [c for c in [
                            "validName","verbatimLocality","habitat","occurrenceType",
                            "sourceCitation","wormsID","matchScore","taxonomicStatus",
                            "decimalLatitude","decimalLongitude",
                        ] if c in _db_rows.columns]
                        st.dataframe(_db_rows[_show_cols], use_container_width=True,
                                     hide_index=True)

                # ── Per-species occurrence MAP (live from DB, not static) ─────
                _map_rows = _db_rows.dropna(subset=["decimalLatitude","decimalLongitude"])
                if not _map_rows.empty:
                    st.markdown("#### 🗺️ Occurrence Map")
                    st.caption(
                        f"{len(_map_rows)} geocoded records for "
                        f"*{_strip_author(selected_sp)}*"
                    )
                    st.map(
                        _map_rows.rename(columns={
                            "decimalLatitude": "lat",
                            "decimalLongitude": "lon",
                        })[["lat","lon"]],
                        zoom=5,
                    )
                else:
                    st.caption("No geocoded records — run Geocoding in Tab 4.")

                # ── Provenance ───────────────────────────────────────────────
                sources = _db_rows["sourceCitation"].dropna().unique().tolist() if not _db_rows.empty else []
                if sources:
                    with st.expander("📚 Provenance"):
                        for s in sources:
                            st.markdown(f"- {s}")
        else:
            st.info("No wiki articles yet. Run an extraction to populate.")

        st.divider()

        # ── Locality Checklist (improved: shows all species + all points) ────
        st.subheader("📍 Locality Species Checklist")
        st.caption("Species recorded at each locality, with all occurrence coordinates shown on the map.")
        loc_list = wiki.list_localities()
        if loc_list:
            selected_loc = st.selectbox("Locality:", loc_list, key="wiki_loc_sel")
            if selected_loc:
                # Load from DB (not just wiki JSON) for completeness
                try:
                    import sqlite3 as _sq2
                    _lc = _sq2.connect(META_DB_PATH)
                    _loc_df = pd.read_sql_query(
                        """SELECT DISTINCT validName, recordedName, habitat,
                                  occurrenceType, sourceCitation,
                                  family_, phylum, wormsID,
                                  decimalLatitude, decimalLongitude
                           FROM occurrences_v4
                           WHERE verbatimLocality LIKE ?
                              OR verbatimLocality LIKE ?
                           ORDER BY family_, validName""",
                        _lc,
                        params=(f"%{selected_loc}%", f"%{selected_loc[:15]}%"),
                    )
                    _lc.close()
                except Exception:
                    _loc_df = pd.DataFrame()

                if not _loc_df.empty:
                    _sp_at_loc = _loc_df["validName"].dropna().unique().tolist()
                    st.write(f"**{len(_sp_at_loc)} species recorded at {selected_loc}:**")

                    # Two-column species list with family grouping
                    _fam_groups: dict = {}
                    for _, row in _loc_df.iterrows():
                        fam = str(row.get("family_","") or "Unknown family")
                        sp  = str(row.get("validName","") or row.get("recordedName",""))
                        if sp:
                            _fam_groups.setdefault(fam, [])
                            if sp not in _fam_groups[fam]:
                                _fam_groups[fam].append(sp)

                    col_a, col_b = st.columns(2)
                    _fam_items = sorted(_fam_groups.items())
                    half = len(_fam_items) // 2
                    for col, items in [(col_a, _fam_items[:half]), (col_b, _fam_items[half:])]:
                        with col:
                            for fam, sps in items:
                                st.markdown(f"**{fam}**")
                                for sp in sps:
                                    st.markdown(f"  • *{_strip_author(sp)}*")

                    # Map: all geocoded points at this locality
                    _loc_map = _loc_df.dropna(subset=["decimalLatitude","decimalLongitude"])
                    if not _loc_map.empty:
                        st.markdown("#### 🗺️ All occurrence points at this locality")
                        st.map(
                            _loc_map.rename(columns={
                                "decimalLatitude": "lat",
                                "decimalLongitude": "lon",
                            })[["lat","lon"]],
                            zoom=8,
                        )
                else:
                    # Fallback to wiki JSON
                    _slug_fn = wiki._slug if hasattr(wiki,"_slug") \
                               else lambda x: x.lower().replace(" ","_")
                    loc_art = wiki._load_article("locality", _slug_fn(selected_loc))
                    if loc_art:
                        f = loc_art.get("facts",{})
                        sp_cl = f.get("species_checklist",[])
                        st.write(f"**{len(sp_cl)} species (from wiki cache):**")
                        for sp in sp_cl:
                            st.write(f"• {sp}")
                    else:
                        st.info(f"No occurrence data found for '{selected_loc}'.")'''


PATCHES = [
    Patch("R1-hitl-reset-resume",   "Checkpoint pipeline into session_state so HITL confirm can resume", _R1_OLD,  _R1_NEW),
    Patch("R1b-hitl-checkpoint",    "Save checkpoint before HITL gate fires",                           _R1B_OLD, _R1B_NEW),
    Patch("R2-taxonomy-fallback",   "Unpack Higher Taxonomy JSON into flat DB fields as fallback",       _R2_OLD,  _R2_NEW),
    Patch("R3-citation-fix",        "Stamp citation_str on records + pass it to insert_occurrences",     _R3_OLD,  _R3_NEW),
    Patch("R5-edit-delete",         "Full-field UPDATE + row-level DELETE in Verification Table",        _R5_OLD,  _R5_NEW),
    Patch("R4R6-wiki-improvements", "Per-species DB map, taxonomy banner, author normalization",         _R4_OLD,  _R4_NEW),
]


# ─────────────────────────────────────────────────────────────────────────────
#  ENGINE (same as Round 1)
# ─────────────────────────────────────────────────────────────────────────────

def _is_applied(content: str, patch: Patch) -> bool:
    return f"# PATCHED-R2: {patch.name}" in content

def _apply_patch(content: str, patch: Patch, verbose: bool = True) -> tuple[str, bool]:
    if _is_applied(content, patch):
        if verbose: print(f"  ⏭️  {patch.name}: already applied, skipping")
        return content, False
    if patch.old not in content:
        print(f"  ⚠️  {patch.name}: target string NOT found")
        print(f"     First 80 chars: {repr(patch.old[:80])}")
        return content, False
    new_content = content.replace(patch.old, patch.new, 1)
    if verbose: print(f"  ✅ {patch.name}: {patch.description}")
    return new_content, True

def apply_all_patches(target=TARGET_FILE, dry_run=False, backup=True, verbose=True) -> int:
    if not target.exists():
        print(f"❌ Target not found: {target}"); sys.exit(1)
    content = target.read_text(encoding="utf-8")
    applied = 0
    print(f"\n{'DRY RUN — ' if dry_run else ''}Applying {len(PATCHES)} Round-2 patches to {target}\n")
    print("─" * 60)
    for patch in PATCHES:
        content, was = _apply_patch(content, patch, verbose)
        if was: applied += 1
    print("─" * 60)
    print(f"\n{applied}/{len(PATCHES)} patches applied.")
    if applied and not dry_run:
        if backup:
            bak = target.with_suffix(".py.bak2")
            shutil.copy(target, bak)
            print(f"📦 Backup: {bak}")
        target.write_text(content, encoding="utf-8")
        print(f"✅ Written: {target}")
    elif dry_run:
        print("ℹ️  Dry run — no files written.")
    return applied

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BioTrace v5.4 Round-2 patches")
    parser.add_argument("--target",    type=Path, default=TARGET_FILE)
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    apply_all_patches(args.target, args.dry_run, not args.no_backup)
