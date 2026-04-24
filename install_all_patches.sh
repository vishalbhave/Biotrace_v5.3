#!/usr/bin/env bash
# install_all_patches.sh — Apply all BioTrace patches in correct order
# Usage: bash install_all_patches.sh [path/to/biotrace_v5.py]
TARGET="${1:-biotrace_v5.py}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  BioTrace v5.4 Patch Installer"
echo "  Target: $TARGET"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "── Round 1 (Bug fixes: candidates, dedup, PDF, HITL wiring) ──"
python3 apply_biotrace_patches.py --target "$TARGET" --no-backup

echo ""
echo "── Round 2 (HITL reset, taxonomy, citation, wiki, edit/delete) ──"
python3 apply_biotrace_patches_v2.py --target "$TARGET" --no-backup

echo ""
echo "── pip dependencies for new modules ──"
pip install pygbif --quiet && echo "  ✅ pygbif"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Done. Restart your Streamlit server."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
