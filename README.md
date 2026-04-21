# BioTrace — Biodiversity Extractor

Enhanced Darwin Core literature mining tool for marine biologists.

## Quick Start

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Features

### Multi-Provider LLM
| Provider | How to use |
|----------|-----------|
| **Ollama (local)** | Start `ollama serve`. Available models are fetched automatically. |
| **OpenAI** | Paste your `sk-...` API key in the sidebar. |
| **Gemini** | Paste your `AIza...` API key in the sidebar. |

### Multi-PDF Pipeline
- Upload multiple PDFs at once — each is processed independently
- Results stored in `biodiversity_data/extractions/` as separate CSVs
- Catalogued in a local SQLite database (`biodiversity_data/metadata.db`)

### Duplicate Detection (two layers)
1. **File-level hash** — SHA-256 fingerprint of each PDF. If already processed, cached results are loaded instantly.
2. **Chunk-level vector cache** — each text chunk is embedded (all-MiniLM-L6-v2) and stored in ChromaDB. Identical/near-duplicate chunks across different documents skip the LLM call entirely.

### Three Tabs
- **Extract** — upload & run, per-file result tabs, live log console, session map
- **Library** — browse all past extractions, retrieve individual CSVs
- **Global Map** — CartoDB dark map of every georeferenced record across all archived PDFs, colour-coded by source file

## Data Layout

```
biodiversity_data/
├── metadata.db              # SQLite: all processed PDFs
├── vector_store/            # ChromaDB: embedded chunks
└── extractions/
    ├── Paper_A_<hash>_taxa.csv
    ├── Paper_A_<hash>_sites.csv
    ├── Paper_B_<hash>_taxa.csv
    └── Paper_B_<hash>_sites.csv
```
