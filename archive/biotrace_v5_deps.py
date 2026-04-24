#!/usr/bin/env python3
"""
biotrace_v5_deps.py  —  BioTrace v5.2 Dependency Audit & Bootstrap
────────────────────────────────────────────────────────────────────────────
Run FIRST before launching biotrace_v5.py.

Two completely separate categories:

  CATEGORY A — pip packages   (`pip install <name>`)
  CATEGORY B — local modules  (plain .py files placed alongside biotrace_v5.py;
                                NOT on PyPI — `pip install` WILL NOT WORK)

Usage:
    python3 biotrace_v5_deps.py              # full audit
    python3 biotrace_v5_deps.py --fix        # auto-install missing pip packages
    python3 biotrace_v5_deps.py --test-apis  # ping WoRMS/GNA (internet)
    python3 biotrace_v5_deps.py --self-test  # module smoke tests
"""
from __future__ import annotations
import argparse, importlib, os, subprocess, sys
from dataclasses import dataclass
from typing import Optional

R="\033[0m"; G="\033[92m"; Y="\033[93m"; RE="\033[91m"; B="\033[94m"
BOLD="\033[1m"; CY="\033[96m"
def c(col,t): return f"{col}{t}{R}"
def ok(m):   print(f"  {c(G,'OK ')} {m}")
def warn(m): print(f"  {c(Y,'WRN')} {m}")
def err(m):  print(f"  {c(RE,'ERR')} {m}")
def info(m): print(f"  {c(B,'   ')} {m}")
def head(m): print(f"\n{c(BOLD+CY, m)}")

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY A  —  pip packages
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class PipDep:
    import_name: str
    pip_name:    str
    required:    bool = True
    notes:       str  = ""
    status:      str  = "unknown"
    version:     str  = ""

PIP_DEPS: list[PipDep] = [
    # stdlib
    PipDep("sqlite3",   "stdlib",               True),
    PipDep("json",      "stdlib",               True),
    PipDep("pathlib",   "stdlib",               True),
    PipDep("re",        "stdlib",               True),
    PipDep("logging",   "stdlib",               True),
    # core data
    PipDep("pandas",    "pandas",               True,  "dataframes + CSV export"),
    PipDep("numpy",     "numpy",                True,  "array ops"),
    PipDep("requests",  "requests",             True,  "GNA + WoRMS REST"),
    # schema enforcement
    PipDep("pydantic",  "pydantic",             True,  "Pydantic v2 schema (required for v5.2)"),
    PipDep("json_repair","json-repair",         False, "5-stage JSON auto-repair (recommended)"),
    # NLP
    PipDep("spacy",     "spacy",                False, "GPE locality NER + PERSON suppression"),
    PipDep("nltk",      "nltk",                 False, "Trigram scorer (NetiNeti-style)"),
    PipDep("transformers","transformers",       False, "HuggingFace NER pipeline"),
    # graph + viz
    PipDep("networkx",  "networkx",             True,  "Knowledge Graph"),
    PipDep("plotly",    "plotly",               False, "Graph charts"),
    PipDep("pyvis",     "pyvis",                False, "Interactive KG HTML export"),
    # memory bank
    PipDep("sklearn",   "scikit-learn",         True,  "TF-IDF recall in Memory Bank"),
    PipDep("scipy",     "scipy",                False, "Community detection"),
    PipDep("rapidfuzz", "rapidfuzz",            False, "Fuzzy locality / pincode matching"),
    # geocoding
    PipDep("geopy",     "geopy",                False, "Nominatim 1 req/sec"),
    # UI
    PipDep("streamlit", "streamlit",            True,  "Web UI"),
    # PDF parsers
    PipDep("pymupdf4llm","pymupdf4llm",         False, "PDF→Markdown (recommended)"),
    PipDep("markitdown","markitdown",           False, "Alt PDF parser (Microsoft)"),
    PipDep("fitz",      "pymupdf",              False, "Raw PDF fallback (PyMuPDF)"),
    # LLM clients
    PipDep("ollama",    "ollama",               False, "Local Ollama LLM client"),
    PipDep("openai",    "openai",               False, "OpenAI GPT"),
    PipDep("anthropic", "anthropic",            False, "Anthropic / Ollama-compat"),
    PipDep("google.generativeai","google-generativeai",False,"Gemini"),
    # OCR
    PipDep("pytesseract","pytesseract",         False, "Tesseract OCR wrapper"),
    PipDep("pdf2image", "pdf2image",            False, "PDF rasteriser for OCR"),
    # PDF metadata (v5.3)
    PipDep("habanero",   "habanero",             False, "Crossref REST — paper metadata"),
    PipDep("pypdf",      "pypdf",                False, "PDF DOI + title extraction"),
    PipDep("semanticscholar","semanticscholar",  False, "Semantic Scholar API client (optional)"),
]

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY B  —  local .py module files  (NOT pip packages)
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class LocalMod:
    filename:    str
    import_name: str
    required:    bool = True
    notes:       str  = ""
    present:     bool = False

LOCAL_MODS: list[LocalMod] = [
    # v4 support
    LocalMod("species_verifier.py",        "species_verifier",        True,  "GNA+WoRMS verifier"),
    LocalMod("geocoding_cascade.py",       "geocoding_cascade",       True,  "4-stage geocoding pipeline"),
    LocalMod("coord_utils.py",             "coord_utils",             True,  "DMS parser + bbox validation"),
    LocalMod("pincode_geocoder.py",        "pincode_geocoder",        False, "Indian pincode → lat/lon"),
    LocalMod("nominatim_geocoder.py",      "nominatim_geocoder",      False, "Nominatim enriched geocoder"),
    # v5.0
    LocalMod("biotrace_knowledge_graph.py","biotrace_knowledge_graph",True,  "NetworkX GraphRAG store"),
    LocalMod("biotrace_memory_bank.py",    "biotrace_memory_bank",    True,  "FTS5+TF-IDF memory atoms"),
    LocalMod("biotrace_wiki.py",           "biotrace_wiki",           True,  "LLM-Wiki article store"),
    # v5.1
    LocalMod("biotrace_chunker.py",        "biotrace_chunker",        False, "Section-aware chunker"),
    LocalMod("biotrace_gnv.py",            "biotrace_gnv",            False, "Enhanced GNA+dedup+splitter"),
    LocalMod("biotrace_ocr.py",            "biotrace_ocr",            False, "DocTR/Tesseract/multimodal OCR"),
    # v5.2 (new)
    LocalMod("biotrace_ner.py",            "biotrace_ner",            True,  "BHL 3-phase TNR engine"),
    LocalMod("biotrace_locality_ner.py",   "biotrace_locality_ner",   True,  "Locality NER + admin expander"),
    LocalMod("biotrace_schema.py",         "biotrace_schema",         True,  "Pydantic v2 + JSON repair"),
    LocalMod("biotrace_v5_enhancements.py","biotrace_v5_enhancements",True,  "Verification table + UI tabs"),
    # v5.3 (new)
    LocalMod("biotrace_pdf_meta.py",          "biotrace_pdf_meta",          False, "S2+Crossref metadata + PDF rename"),
    LocalMod("biotrace_hierarchical_chunker.py","biotrace_hierarchical_chunker",True, "3-level hierarchical late-chunker"),
]

COMPAT = [
    ("GraphRAG pipeline",   ["networkx","pandas","requests"],    "core"),
    ("Memory Bank TF-IDF",  ["sklearn","numpy"],                 "TF-IDF recall"),
    ("Locality expansion",  ["geopy"],                           "Nominatim 1/sec"),
    ("Pydantic schema",     ["pydantic"],                        "v2 required"),
    ("Streamlit UI",        ["streamlit","pandas"],              "core UI"),
    ("PyVis KG export",     ["pyvis","networkx"],                "interactive graph"),
]


def audit_pip(auto_fix: bool) -> dict[str, PipDep]:
    head("CATEGORY A — pip packages  (install with: pip install <name>)")
    results: dict[str, PipDep] = {}
    to_install: list[str] = []

    for d in PIP_DEPS:
        if d.pip_name == "stdlib":
            try:
                importlib.import_module(d.import_name)
                d.status = "ok"; d.version = "stdlib"
                ok(f"{d.import_name:<34} stdlib")
            except ImportError:
                d.status = "missing"
                err(f"{d.import_name:<34} STDLIB MISSING — severe")
            results[d.import_name] = d
            continue

        try:
            mod = importlib.import_module(d.import_name)
            d.version = getattr(mod, "__version__", "?")
            d.status  = "ok"
            suffix = f"  [{d.notes}]" if d.notes else ""
            ok(f"{d.import_name:<34} v{d.version}{suffix}")
        except ImportError:
            d.status = "missing"
            if d.required:
                err(f"{d.import_name:<34} MISSING  →  pip install {d.pip_name}")
                to_install.append(d.pip_name)
            else:
                note = f"  [{d.notes}]" if d.notes else ""
                warn(f"{d.import_name:<34} optional →  pip install {d.pip_name}{note}")
        results[d.import_name] = d

    if to_install and auto_fix:
        head("AUTO-INSTALLING MISSING pip PACKAGES")
        cmd = [sys.executable,"-m","pip","install","--break-system-packages","-q"] + to_install
        print("  $", " ".join(cmd))
        r = subprocess.run(cmd, capture_output=True, text=True)
        (ok if r.returncode==0 else err)(
            "Packages installed." if r.returncode==0 else f"Failed:\n{r.stderr[:500]}"
        )
    return results


def audit_local(cwd: str = ".") -> dict[str, LocalMod]:
    head("CATEGORY B — local .py modules  (copy into project folder)")
    print(c(Y, f"  These are NOT pip packages.  Place them in: {os.path.abspath(cwd)}/"))
    print(c(Y,  "  Running `pip install biotrace_chunker.py` WILL NOT WORK.\n"))
    results: dict[str, LocalMod] = {}
    missing_req: list[str] = []

    for m in LOCAL_MODS:
        path = os.path.join(cwd, m.filename)
        file_present = os.path.exists(path)
        m.present = file_present
        imp_ok = False

        if file_present:
            try:
                importlib.import_module(m.import_name)
                imp_ok = True
            except ImportError as e:
                warn(f"{m.filename:<46} file present but import failed: {e}")
            except Exception as e:
                warn(f"{m.filename:<46} import error: {e}")

        if file_present and imp_ok:
            suffix = f"  [{m.notes}]" if m.notes else ""
            ok(f"{m.filename:<46} importable{suffix}")
        elif file_present and not imp_ok:
            pass   # warn already printed
        elif m.required:
            err(f"{m.filename:<46} MISSING  →  copy into {os.path.abspath(cwd)}/")
            missing_req.append(m.filename)
        else:
            note = f"  [{m.notes}]" if m.notes else ""
            warn(f"{m.filename:<46} optional →  copy into project folder{note}")

        results[m.import_name] = m

    return results


def check_compat(pip_res: dict) -> list[str]:
    head("COMPATIBILITY MATRIX")
    issues = []
    for name, needed, note in COMPAT:
        missing = [n for n in needed if pip_res.get(n, PipDep("","")).status != "ok"]
        if missing:
            warn(f"{name:<30} missing pip: {', '.join(missing)}")
            issues.append(name)
        else:
            ok(f"{name:<30} {note}")
    return issues


def check_ollama(url: str = "http://localhost:11434") -> bool:
    head("OLLAMA CONNECTIVITY")
    try:
        import requests
        r = requests.get(f"{url}/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models",[])]
        ok(f"Reachable at {url}  ({len(models)} model(s) loaded)")
        if models:
            ok("Models: " + ", ".join(models[:6]))
        else:
            warn("No models.  Run:  ollama pull gemma3")
        return True
    except Exception as e:
        warn(f"Offline ({e})")
        info("Start:  ollama serve")
    return False


def check_apis():
    head("TAXONOMY API CONNECTIVITY")
    try:
        import requests
        r = requests.get("https://verifier.globalnames.org/api/v1/verifications",
                         params={"names":"Acanthurus triostegus","data_sources":"169"},timeout=10)
        ok("GNA Verifier online") if r.status_code==200 else warn(f"GNA {r.status_code}")
    except Exception as e:
        err(f"GNA offline: {e}")
    try:
        import requests
        r = requests.get("https://www.marinespecies.org/rest/AphiaRecordsByName/Acanthurus triostegus",
                         params={"like":"false","marine_only":"false"},timeout=10)
        ok(f"WoRMS online (AphiaID {r.json()[0].get('AphiaID','?')})") if r.status_code==200 else warn(f"WoRMS {r.status_code}")
    except Exception as e:
        err(f"WoRMS offline: {e}")


def self_test():
    head("MODULE SMOKE TESTS")
    _occ = [{"validName":"Acanthurus triostegus","recordedName":"Acanthurus triostegus",
              "family_":"Acanthuridae","order_":"Acanthuriformes","phylum":"Chordata",
              "class_":"Actinopterygii","wormsID":"219635","taxonomicStatus":"accepted",
              "verbatimLocality":"Gulf of Mannar","decimalLatitude":9.1,"decimalLongitude":79.1,
              "Habitat":"Coral reef","occurrenceType":"Primary","Source Citation":"Test 2024"}]

    for label, fn in [
        ("KnowledgeGraph",    lambda: _test_kg(_occ)),
        ("MemoryBank",        lambda: _test_mb(_occ)),
        ("LLM-Wiki",          lambda: _test_wiki(_occ)),
        ("Pydantic schema",   lambda: _test_schema()),
        ("TNR regex",         lambda: _test_ner()),
        ("Locality seg.",     lambda: _test_loc()),
        ("Verif. table",      lambda: _test_enh(_occ)),
        ("Hier. chunker",     lambda: _test_hier()),
        ("PDF meta",          lambda: _test_pdf_meta()),
    ]:
        try:
            msg = fn()
            ok(f"{label:<22} {msg}")
        except Exception as e:
            err(f"{label:<22} {e}")


def _test_kg(occ):
    from biotrace_knowledge_graph import BioTraceKnowledgeGraph
    kg = BioTraceKnowledgeGraph(":memory:")
    kg.ingest_occurrences(occ)
    s = kg.stats()
    return f"{s['total_nodes']} nodes, {s['total_edges']} edges"

def _test_mb(occ):
    from biotrace_memory_bank import BioTraceMemoryBank
    mb = BioTraceMemoryBank(":memory:")
    r  = mb.store_occurrences(occ, session_id="smoke")
    mb.recall("coral reef")
    return f"{r['inserted']} atom stored, recall OK"

def _test_wiki(occ):
    import tempfile
    from biotrace_wiki import BioTraceWiki
    with tempfile.TemporaryDirectory() as td:
        w = BioTraceWiki(td)
        w.update_from_occurrences(occ, citation="Test 2024")
        return f"{len(w.list_species())} article(s)"

def _test_schema():
    import json
    from biotrace_schema import parse_llm_response
    raw = json.dumps([{"Recorded Name":"Acanthurus triostegus",
                       "Sampling Event":{"date":"March 2022","depth_m":"10m"},
                       "occurrenceType":"primary","Source Citation":"T 2024"}])
    recs, _ = parse_llm_response(raw, chunk_id=1)
    assert recs and recs[0].occurrenceType=="Primary" and recs[0].depthM=="10"
    return f"coercion OK (occurrenceType={recs[0].occurrenceType}, depth={recs[0].depthM})"

def _test_ner():
    from biotrace_ner import regex_scan
    text = "We collected Holothuria scabra and Siganus javus at the reef."
    cands = regex_scan(text)
    assert any("Holothuria scabra" in c.canonical for c in cands)
    return f"{len(cands)} candidate(s) found"

def _test_loc():
    from biotrace_locality_ner import segregate_locality_string
    assert segregate_locality_string("Narara Island, Gulf of Kutch") == ["Narara Island, Gulf of Kutch"]
    assert len(segregate_locality_string("Narara, Pirotan, Beyt Dwarka")) == 3
    return "hierarchical=1, multi-site=3 — correct"

def _test_enh(occ):
    from biotrace_v5_enhancements import occurrences_to_verification_df
    df = occurrences_to_verification_df(occ)
    assert "Flag" in df.columns and len(df)==1
    return f"{len(df.columns)} columns, {len(df)} row"

def _test_hier():
    import tempfile, os
    from biotrace_hierarchical_chunker import HierarchicalChunker
    sample = "# Results\n\nHolothuria scabra was collected from Narara Island.\nSiganus javus was also found at the same site."
    with tempfile.TemporaryDirectory() as td:
        hc = HierarchicalChunker(db_path=os.path.join(td,"c.db"))
        dh = hc.ingest(sample, source_label="test")
        s  = hc.doc_stats(dh)
        batches = list(hc.extraction_batches(dh))
        hc.close()
    return f"{s['sections']} sections, {s['sentences']} sents, {len(batches)} batches"

def _test_pdf_meta():
    from biotrace_pdf_meta import PaperMetaFetcher, PaperMeta, availability_report
    avail = availability_report()
    # Test citation string + rename logic (no network)
    m = PaperMeta(title="Marine Fauna of Gulf of Kutch",
                  authors=["Pillai RSK","Kumar A"],year="1985",
                  doi="10.1234/test",journal="JMBAI",volume="27",pages="1-42")
    assert "1985" in m.citation_string
    assert "Pillai" in m.safe_filename_stem
    assert "Marine" in m.safe_filename_stem
    return f"citation OK, stem={m.safe_filename_stem}"


def print_summary(pip_res, local_res, issues):
    head("SUMMARY")
    n_pip_ok  = sum(1 for d in pip_res.values()   if d.status=="ok")
    n_req_pip = sum(1 for d in pip_res.values()   if d.required and d.status=="ok")
    n_tot_pip = sum(1 for d in pip_res.values()   if d.required)
    n_loc_ok  = sum(1 for m in local_res.values() if m.present)
    n_req_loc = sum(1 for m in local_res.values() if m.required and m.present)
    n_tot_loc = sum(1 for m in local_res.values() if m.required)

    print(f"  pip packages:  {n_pip_ok}/{len(pip_res)} available  (required: {n_req_pip}/{n_tot_pip})")
    print(f"  local modules: {n_loc_ok}/{len(local_res)} present    (required: {n_req_loc}/{n_tot_loc})")
    print(f"  compat issues: {len(issues)}")

    if n_req_pip==n_tot_pip and n_req_loc==n_tot_loc:
        print(f"\n  {c(G+BOLD,'BioTrace v5.2 ready.')}  Run:  streamlit run biotrace_v5.py")
    else:
        print(f"\n  {c(Y+BOLD,'Action needed:')}")
        miss_pip = [d.pip_name for d in pip_res.values()
                    if d.required and d.status=="missing" and d.pip_name!="stdlib"]
        if miss_pip:
            print(f"    pip install {' '.join(miss_pip)}")
        miss_loc = [m.filename for m in local_res.values() if m.required and not m.present]
        if miss_loc:
            print(f"    Copy into project folder:")
            for f in miss_loc:
                print(f"      {f}")
    print()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix",       action="store_true")
    ap.add_argument("--test-apis", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    print(c(BOLD+CY,
        "╔══════════════════════════════════════════╗\n"
        "║  BioTrace v5.2 — Dependency Auditor     ║\n"
        "╚══════════════════════════════════════════╝"))
    print(f"  Python {sys.version.split()[0]}  |  CWD: {os.path.abspath('.')}\n")

    pip_res   = audit_pip(args.fix)
    local_res = audit_local()
    issues    = check_compat(pip_res)
    check_ollama()

    if args.test_apis:
        check_apis()
    if args.self_test:
        self_test()

    print_summary(pip_res, local_res, issues)
