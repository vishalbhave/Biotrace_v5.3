#!/usr/bin/env python3
"""
biotrace_v5_deps.py  —  BioTrace v5.4 Dependency Audit & Bootstrap
────────────────────────────────────────────────────────────────────────────
Run FIRST before launching biotrace_v5.py.

WHAT THIS SCRIPT DOES
─────────────────────
1. Audits all required pip packages and local modules.
2. Flags duplicate / stale files in the project directory.
3. Verifies CUDA availability and torch device state.
4. Optionally installs missing pip packages (--fix).
5. Optionally pings external APIs (--test-apis).
6. Optionally runs module smoke tests (--self-test).

TWO COMPLETELY SEPARATE CATEGORIES
───────────────────────────────────
  CATEGORY A — pip packages   (pip install <name>)
  CATEGORY B — local modules  (.py files alongside biotrace_v5.py;
                               NOT on PyPI — pip install WILL NOT WORK)

DUPLICATE / STALE FILES (safe to delete)
─────────────────────────────────────────
  biotrace_hitl_geocoding_new.py  →  superseded by biotrace_hitl_geocoding.py
  "Taxo extractor.py"             →  duplicate of taxo_extractor.py (space in name)
  biotrace_cocoindex_flow.py      →  experimental, not imported by biotrace_v5.py

CUDA 13 NOTE
────────────
  spaCy and taxonerd are NOT listed — they fail on CUDA 13 (driver ≥530)
  because their compiled CUDA extensions target earlier runtimes.
  NER is handled entirely by biotrace_hf_ner.py (transformers + torch).
  PyTorch must be installed with the cu121 wheel:

    pip install torch --index-url https://download.pytorch.org/whl/cu121

Usage:
    python3 biotrace_v5_deps.py              # full audit
    python3 biotrace_v5_deps.py --fix        # auto-install missing pip packages
    python3 biotrace_v5_deps.py --test-apis  # ping WoRMS / COL / GNA
    python3 biotrace_v5_deps.py --self-test  # module smoke tests
    python3 biotrace_v5_deps.py --dupes      # show duplicate/stale files only
"""
from __future__ import annotations

import importlib
import importlib.metadata
import os
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import NamedTuple


# ─────────────────────────────────────────────────────────────────────────────
#  ANSI colours
# ─────────────────────────────────────────────────────────────────────────────
_IS_TTY = sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _IS_TTY else text

OK   = lambda t: _c("32", t)
WARN = lambda t: _c("33", t)
ERR  = lambda t: _c("31", t)
BOLD = lambda t: _c("1",  t)
DIM  = lambda t: _c("2",  t)


# ─────────────────────────────────────────────────────────────────────────────
#  CATEGORY A — pip packages
# ─────────────────────────────────────────────────────────────────────────────

class PipPkg(NamedTuple):
    import_name: str   # name used in `import <name>`
    pip_name:    str   # name used in `pip install <name>`
    required:    bool  # False = optional/graceful degradation
    note:        str   # shown on failure

PIP_PACKAGES: list[PipPkg] = [
    # ── Core runtime ─────────────────────────────────────────────────────────
    PipPkg("streamlit",          "streamlit>=1.35.0",          True,  "Main UI framework"),
    PipPkg("pandas",             "pandas>=2.1.0",              True,  "Data manipulation"),
    PipPkg("pydantic",           "pydantic>=2.5.0",            True,  "Schema validation (v2 required)"),
    PipPkg("requests",           "requests>=2.31.0",           True,  "HTTP for geocoding / APIs"),
    PipPkg("httpx",              "httpx>=0.27.0",              True,  "Async HTTP for COL client"),

    # ── HuggingFace / NLP (replaces spaCy + taxonerd) ────────────────────────
    PipPkg("torch",              "torch",                      True,
           "Install with cu121 wheels: pip install torch --index-url "
           "https://download.pytorch.org/whl/cu121"),
    PipPkg("transformers",       "transformers>=4.40.0",       True,  "BERT NER models"),
    PipPkg("tokenizers",         "tokenizers>=0.19.0",         True,  "Fast tokenisation"),
    PipPkg("accelerate",         "accelerate>=0.28.0",         True,  "device_map=auto, multi-GPU"),
    PipPkg("huggingface_hub",    "huggingface-hub>=0.22.0",    True,  "Model downloads"),
    PipPkg("sentencepiece",      "sentencepiece>=0.2.0",       True,  "BioBERT tokeniser dependency"),
    PipPkg("safetensors",        "safetensors>=0.4.2",         True,  "Fast model weight loading"),

    # ── PDF parsing ───────────────────────────────────────────────────────────
    PipPkg("pymupdf4llm",        "pymupdf4llm>=0.0.17",        True,  "PDF → Markdown (fast)"),
    PipPkg("markitdown",         "markitdown>=0.0.1",          False, "Lightweight PDF parser"),
    PipPkg("docling",            "docling>=1.20.0",            False, "IBM structured PDF parser"),
    PipPkg("pytesseract",        "pytesseract>=0.3.10",        False, "OCR fallback"),

    # ── LLM clients ───────────────────────────────────────────────────────────
    PipPkg("anthropic",          "anthropic>=0.25.0",          False, "Anthropic API"),
    PipPkg("openai",             "openai>=1.20.0",             False, "OpenAI API"),
    PipPkg("ollama",             "ollama>=0.2.0",              False, "Local Ollama client"),
    PipPkg("google.generativeai","google-generativeai>=0.5.0", False, "Gemini API"),

    # ── Geocoding ─────────────────────────────────────────────────────────────
    PipPkg("geopy",              "geopy>=2.4.1",               True,  "Nominatim geocoder"),

    # ── Visualisation ─────────────────────────────────────────────────────────
    PipPkg("plotly",             "plotly>=5.20.0",             False, "Interactive charts"),

    # ── Utilities ─────────────────────────────────────────────────────────────
    PipPkg("tqdm",               "tqdm>=4.66.0",               False, "Progress bars"),
    PipPkg("dotenv",             "python-dotenv>=1.0.0",       False, "Load .env files"),
]

# ── INTENTIONALLY EXCLUDED — documented so future contributors don't re-add ──
EXCLUDED_PACKAGES = [
    ("spacy",     "CUDA 13 incompatible — compiled extensions fail on driver ≥530"),
    ("taxonerd",  "Depends on spaCy; same CUDA 13 issue. Replaced by biotrace_hf_ner.py"),
    ("doctr",     "Optional heavy OCR — install separately: pip install python-doctr[torch]"),
]


# ─────────────────────────────────────────────────────────────────────────────
#  CATEGORY B — local modules
# ─────────────────────────────────────────────────────────────────────────────

class LocalMod(NamedTuple):
    filename:  str   # .py filename expected alongside biotrace_v5.py
    required:  bool
    role:      str   # one-line description

LOCAL_MODULES: list[LocalMod] = [
    # ── Core patches (always required) ───────────────────────────────────────
    LocalMod("biotrace_geocoding_lifestage_patch.py", True,
             "Patch A: Nominatim India bias | Patch B: LLM life-stage guard"),
    LocalMod("biotrace_locality_guard_patch.py",      True,
             "Rejects morphology/habitat strings from verbatimLocality"),
    LocalMod("biotrace_dedup_patch.py",               True,
             "3-stage dedup: exact-key → containment → regional suppression"),
    LocalMod("biotrace_traiter_prepass.py",           True,
             "Stage 0: rule-based span annotation before LLM (no spaCy required)"),

    # ── Enhancement modules ───────────────────────────────────────────────────
    LocalMod("biotrace_col_client.py",                True,
             "Catalogue of Life REST API client + SQLite cache"),
    LocalMod("biotrace_relation_extractor.py",        True,
             "DeepKE-inspired cross-sentence relation extraction"),
    LocalMod("biotrace_kg_spatio_temporal.py",        True,
             "Hyper-Extract-inspired SpatioTemporal KG (SQLite FTS5)"),
    LocalMod("biotrace_hf_ner.py",                    True,
             "Pure transformers NER — replaces spaCy/taxonerd (CUDA 13 safe)"),

    # ── v5 core modules ───────────────────────────────────────────────────────
    LocalMod("biotrace_gnv.py",                       False, "GNV verifier + LocalitySplitter"),
    LocalMod("biotrace_schema.py",                    False, "Pydantic schema + 5-attempt JSON repair"),
    LocalMod("biotrace_chunker.py",                   False, "Document chunker (section/paragraph/fixed)"),
    LocalMod("biotrace_hierarchical_chunker.py",      False, "3-level hierarchical chunker (v5.3)"),
    LocalMod("biotrace_hitl_geocoding.py",            False, "HITL geocoding tab + 4-store sync"),
    LocalMod("biotrace_knowledge_graph.py",           False, "BioTraceKnowledgeGraph (node/edge store)"),
    LocalMod("biotrace_memory_bank.py",               False, "BioTraceMemoryBank (session atoms)"),
    LocalMod("biotrace_wiki.py",                      False, "BioTraceWiki (locality article store)"),
    LocalMod("biotrace_locality_ner.py",              False, "LocalityNER + GeoNames enrichment"),
    LocalMod("biotrace_ner.py",                       False, "TaxonNER (rule-based, no spaCy)"),
    LocalMod("biotrace_pdf_meta.py",                  False, "PaperMetaFetcher (S2 + Crossref)"),
    LocalMod("biotrace_v5_enhancements.py",           False, "UI tabs: TNR, Locality, Schema, Ollama selector"),
    LocalMod("biotrace_ocr.py",                       False, "OCRPipeline (DocTR + Tesseract + multimodal)"),
    LocalMod("geocoding_cascade.py",                  False, "GeocodingCascade (GeoNames + Nominatim)"),
    LocalMod("species_verifier.py",                   False, "WoRMS/GBIF/ITIS name verification"),
    LocalMod("coord_utils.py",                        False, "Coordinate utilities"),
    LocalMod("title_extractor.py",                    False, "PDF title extraction"),
    LocalMod("taxo_extractor.py",                     False, "Standalone taxonomy extractor"),
]

# ─────────────────────────────────────────────────────────────────────────────
#  DUPLICATE / STALE files in the project directory
# ─────────────────────────────────────────────────────────────────────────────

DUPLICATE_FILES: list[dict] = [
    {
        "filename": "biotrace_hitl_geocoding_new.py",
        "reason":   "Superseded by biotrace_hitl_geocoding.py (full 4-store sync edition). "
                    "Safe to DELETE.",
        "action":   "DELETE",
    },
    {
        "filename": "Taxo extractor.py",    # space in name
        "reason":   "Duplicate of taxo_extractor.py with a space in the filename. "
                    "The underscore version is the canonical one. Safe to DELETE.",
        "action":   "DELETE",
    },
    {
        "filename": "biotrace_cocoindex_flow.py",
        "reason":   "Experimental CocoIndex integration. Not imported by biotrace_v5.py. "
                    "Archive or DELETE if not actively developing CocoIndex flow.",
        "action":   "ARCHIVE",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
#  External API endpoints
# ─────────────────────────────────────────────────────────────────────────────

API_CHECKS = [
    ("WoRMS",          "https://www.marinespecies.org/rest/AphiaRecordByName/Cassiopea?like=true&marine_only=true"),
    ("COL",            "https://api.catalogueoflife.org/nameusage/search?q=Cassiopea+andromeda&limit=1"),
    ("GNA Finder",     "https://finder.globalnames.org/api/v1/find"),
    ("Nominatim",      "https://nominatim.openstreetmap.org/search?q=Narara+Gujarat&format=json&limit=1"),
]


# ─────────────────────────────────────────────────────────────────────────────
#  Audit functions
# ─────────────────────────────────────────────────────────────────────────────

def _check_importable(import_name: str) -> bool:
    try:
        importlib.import_module(import_name)
        return True
    except Exception:
        return False


def _pip_version(pip_name: str) -> str:
    """Return installed version string or empty string."""
    pkg = pip_name.split(">=")[0].split("==")[0].strip()
    try:
        return importlib.metadata.version(pkg)
    except Exception:
        return ""


def _local_exists(project_dir: Path, filename: str) -> bool:
    return (project_dir / filename).exists()


def audit_pip(fix: bool = False) -> tuple[int, int]:
    """Audit pip packages. Returns (missing_required, missing_optional)."""
    print(BOLD("\n── CATEGORY A: pip packages ──────────────────────────────────────────────"))
    miss_req = miss_opt = 0

    for pkg in PIP_PACKAGES:
        ok  = _check_importable(pkg.import_name)
        ver = _pip_version(pkg.pip_name) if ok else ""
        if ok:
            print(f"  {OK('✅')} {pkg.import_name:<28} {DIM(ver)}")
        else:
            label = "REQUIRED" if pkg.required else "optional"
            line  = f"  {ERR('❌') if pkg.required else WARN('⚠️')} {pkg.import_name:<28} {label}  — {pkg.note}"
            print(line)
            if pkg.required:
                miss_req += 1
            else:
                miss_opt += 1
            if fix:
                _install(pkg.pip_name)

    # Show excluded packages
    print(DIM(f"\n  Intentionally excluded (do not install):"))
    for name, reason in EXCLUDED_PACKAGES:
        print(DIM(f"    ✗ {name:<20} — {reason}"))

    return miss_req, miss_opt


def _install(pip_spec: str) -> None:
    pkg = pip_spec.split(">=")[0].strip()
    print(f"  → Installing {pkg}…")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", pip_spec, "-q"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print(f"    {OK('✅')} Installed {pkg}")
    else:
        print(f"    {ERR('❌')} Install failed: {result.stderr.strip()[:120]}")


def audit_local(project_dir: Path) -> tuple[int, int]:
    """Audit local module files. Returns (missing_required, missing_optional)."""
    print(BOLD("\n── CATEGORY B: local .py modules ────────────────────────────────────────"))
    miss_req = miss_opt = 0

    for mod in LOCAL_MODULES:
        exists = _local_exists(project_dir, mod.filename)
        if exists:
            print(f"  {OK('✅')} {mod.filename:<48} {DIM(mod.role[:40])}")
        else:
            label = "REQUIRED" if mod.required else "optional"
            icon  = ERR("❌") if mod.required else WARN("⚠️")
            print(f"  {icon} {mod.filename:<48} {label} — {mod.role[:40]}")
            if mod.required:
                miss_req += 1
            else:
                miss_opt += 1

    return miss_req, miss_opt


def audit_cuda() -> None:
    """Report PyTorch + CUDA state."""
    print(BOLD("\n── CUDA / PyTorch state ─────────────────────────────────────────────────"))
    try:
        import torch
        print(f"  torch version : {OK(torch.__version__)}")
        cuda_ok = torch.cuda.is_available()
        if cuda_ok:
            dev_name = torch.cuda.get_device_name(0)
            capability = torch.cuda.get_device_capability(0)
            print(f"  CUDA          : {OK('available')} — {dev_name} (compute {capability[0]}.{capability[1]})")
        else:
            print(f"  CUDA          : {WARN('not available')} — using CPU")
            print(f"  Fix           : pip install torch --index-url https://download.pytorch.org/whl/cu121")

        # MPS (Apple)
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            print(f"  MPS (Apple)   : {OK('available')}")

    except ImportError:
        print(f"  torch         : {ERR('NOT INSTALLED')}")
        print(f"  Fix           : pip install torch --index-url https://download.pytorch.org/whl/cu121")

    # Check that spaCy is NOT installed (would cause CUDA 13 conflicts)
    try:
        import spacy
        print(f"  {WARN('⚠️  spaCy is installed')} (v{spacy.__version__}) — "
              f"may conflict with CUDA 13 drivers. Consider uninstalling.")
    except ImportError:
        print(f"  spaCy         : {OK('not installed')} (correct — CUDA 13 safe)")

    try:
        import taxonerd
        print(f"  {WARN('⚠️  taxonerd is installed')} — may conflict with CUDA 13. Consider uninstalling.")
    except ImportError:
        print(f"  taxonerd      : {OK('not installed')} (correct — replaced by biotrace_hf_ner.py)")


def audit_hf_models() -> None:
    """Check HuggingFace model availability."""
    print(BOLD("\n── HuggingFace NER models ───────────────────────────────────────────────"))
    models = [
        ("NoYo25/BiodivBERT",               "Primary — biodiversity entity NER"),
        ("nleguillarme/en_ner_eco_biobert",  "Secondary — ecological taxa NER (taxonerd weights)"),
        ("dmis-lab/biobert-base-cased-v1.2", "Fallback — generic BioBERT"),
    ]
    try:
        from huggingface_hub import model_info
        for model_id, role in models:
            try:
                info = model_info(model_id, timeout=5)
                print(f"  {OK('✅')} {model_id:<45} {DIM(role)}")
            except Exception as exc:
                print(f"  {WARN('⚠️')} {model_id:<45} {WARN('unavailable')} — {exc}")
    except ImportError:
        print(f"  {WARN('⚠️')} huggingface_hub not installed — cannot check model availability")
        print(f"     Run: pip install huggingface-hub")


def audit_duplicates(project_dir: Path) -> None:
    """Report duplicate / stale files."""
    print(BOLD("\n── Duplicate / stale files ─────────────────────────────────────────────"))
    any_found = False
    for entry in DUPLICATE_FILES:
        path = project_dir / entry["filename"]
        if path.exists():
            any_found = True
            icon = ERR("❌") if entry["action"] == "DELETE" else WARN("⚠️")
            print(f"  {icon} {entry['filename']}")
            print(f"     Action : {entry['action']}")
            print(f"     Reason : {entry['reason']}")
        else:
            print(f"  {OK('✅')} {entry['filename']} — {DIM('not present (already cleaned)')}")
    if not any_found:
        print(f"  {OK('✅')} No duplicate/stale files found.")


def test_apis() -> None:
    """Ping external APIs and report status codes."""
    print(BOLD("\n── API connectivity tests ───────────────────────────────────────────────"))
    for name, url in API_CHECKS:
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "BioTrace/5.4 (dep-audit)"},
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                print(f"  {OK('✅')} {name:<20} HTTP {resp.status}")
        except Exception as exc:
            print(f"  {ERR('❌')} {name:<20} {exc}")


def self_test() -> None:
    """Quick smoke tests for critical modules."""
    print(BOLD("\n── Self-tests ───────────────────────────────────────────────────────────"))

    # 1. biotrace_dedup_patch
    try:
        from biotrace_dedup_patch import dedup_occurrences, suppress_regional_duplicates, _is_non_taxon
        r = _is_non_taxon("Scyphistoma")
        assert r == "life_stage", f"Expected life_stage, got {r}"
        print(f"  {OK('✅')} biotrace_dedup_patch — _is_non_taxon OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_dedup_patch — {exc}")

    # 2. biotrace_geocoding_lifestage_patch
    try:
        from biotrace_geocoding_lifestage_patch import PROMPT_LIFESTAGE_GUARD, scan_genus_context
        ctx = scan_genus_context("Cassiopea andromeda was found at Narara.")
        assert "C" in ctx and ctx["C"] == "Cassiopea", f"scan_genus_context failed: {ctx}"
        assert len(PROMPT_LIFESTAGE_GUARD) > 100
        print(f"  {OK('✅')} biotrace_geocoding_lifestage_patch — scan_genus_context OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_geocoding_lifestage_patch — {exc}")

    # 3. biotrace_locality_guard_patch
    try:
        from biotrace_locality_guard_patch import post_parse_locality_filter, _classify_locality
        assert _classify_locality("Umbrella circular, 10 cm diameter") == "morphology"
        assert _classify_locality("intertidal area of dead coral reef") == "habitat"
        assert _classify_locality("Narara, Gulf of Kutch") is None
        print(f"  {OK('✅')} biotrace_locality_guard_patch — classifier OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_locality_guard_patch — {exc}")

    # 4. biotrace_traiter_prepass
    try:
        from biotrace_traiter_prepass import run_prepass, format_annotations_for_prompt
        result = run_prepass("Cassiopea andromeda was found at 10 cm depth in Narara reef.")
        assert "Cassiopea andromeda" in result.taxa or len(result.taxa) > 0
        assert len(result.measurements) > 0
        print(f"  {OK('✅')} biotrace_traiter_prepass — run_prepass OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_traiter_prepass — {exc}")

    # 5. biotrace_hf_ner (import only — model download not triggered)
    try:
        import biotrace_hf_ner
        assert hasattr(biotrace_hf_ner, "BiodiVizPipeline")
        assert hasattr(biotrace_hf_ner, "_binomial_regex_fallback")
        fb = biotrace_hf_ner._binomial_regex_fallback(
            "Cassiopea andromeda was observed at Narara. Holothuria scabra also present."
        )
        assert any("Cassiopea" in s for s in fb), f"Fallback missed Cassiopea: {fb}"
        print(f"  {OK('✅')} biotrace_hf_ner — import + regex fallback OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_hf_ner — {exc}")

    # 6. biotrace_col_client (import only — no API call)
    try:
        import biotrace_col_client
        assert hasattr(biotrace_col_client, "lookup_col")
        assert hasattr(biotrace_col_client, "enrich_records_with_col")
        print(f"  {OK('✅')} biotrace_col_client — import OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_col_client — {exc}")

    # 7. biotrace_kg_spatio_temporal (import only)
    try:
        import biotrace_kg_spatio_temporal
        assert hasattr(biotrace_kg_spatio_temporal, "BioTraceSpatioTemporalKG")
        print(f"  {OK('✅')} biotrace_kg_spatio_temporal — import OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_kg_spatio_temporal — {exc}")

    # 8. biotrace_relation_extractor (import only)
    try:
        import biotrace_relation_extractor
        assert hasattr(biotrace_relation_extractor, "extract_relations")
        print(f"  {OK('✅')} biotrace_relation_extractor — import OK")
    except Exception as exc:
        print(f"  {ERR('❌')} biotrace_relation_extractor — {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description="BioTrace v5.4 dependency audit and bootstrap",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--fix",       action="store_true", help="Auto-install missing pip packages")
    parser.add_argument("--test-apis", action="store_true", help="Ping WoRMS / COL / GNA / Nominatim")
    parser.add_argument("--self-test", action="store_true", help="Run module smoke tests")
    parser.add_argument("--dupes",     action="store_true", help="Show duplicate/stale files only")
    parser.add_argument("--hf-models", action="store_true", help="Check HuggingFace model availability")
    args = parser.parse_args()

    project_dir = Path(__file__).parent

    print(BOLD("BioTrace v5.4 — Dependency Audit"))
    print(DIM(f"Project dir : {project_dir}"))
    print(DIM(f"Python      : {sys.version.split()[0]}"))

    if args.dupes:
        audit_duplicates(project_dir)
        return 0

    # Always run
    miss_a_req, miss_a_opt = audit_pip(fix=args.fix)
    miss_b_req, miss_b_opt = audit_local(project_dir)
    audit_cuda()
    audit_duplicates(project_dir)

    if args.hf_models:
        audit_hf_models()

    if args.test_apis:
        test_apis()

    if args.self_test:
        self_test()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(BOLD("\n── Summary ──────────────────────────────────────────────────────────────"))
    total_req = miss_a_req + miss_b_req
    total_opt = miss_a_opt + miss_b_opt

    if total_req == 0:
        print(OK("  ✅ All required dependencies satisfied."))
    else:
        print(ERR(f"  ❌ {total_req} required dependencies missing."))
        if not args.fix:
            print(ERR("     Run with --fix to auto-install missing pip packages."))

    if total_opt > 0:
        print(WARN(f"  ⚠️  {total_opt} optional modules missing (graceful degradation active)."))

    print(DIM(
        "\n  Install order:\n"
        "    1. conda env create -f environment.yml   (or)\n"
        "       pip install torch --index-url https://download.pytorch.org/whl/cu121\n"
        "    2. pip install -r requirements.txt\n"
        "    3. python3 biotrace_v5_deps.py --self-test\n"
        "    4. streamlit run biotrace_v5.py\n"
    ))

    return 1 if total_req > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
