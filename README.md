# BioTrace — Biodiversity Extractor

Enhanced Darwin Core literature mining tool for marine biologists.

## Quick Start

```bash
pip install -r requirements.txt
streamlit run biotrace_v5.py
```

## Features

### Multi-Provider LLM
| Provider | How to use |
|----------|-----------|
| **Ollama (local)** | Start `ollama serve`. Available models are fetched automatically. |
| **OpenAI** | Paste your `sk-...` API key in the sidebar. |
| **Gemini** | Paste your `AIza...` API key in the sidebar. |

### Progressive Learning Cache (Human-in-the-Loop)
BioTrace utilizes an intermediate SQLite database (`biodiversity_data/reference_cache.db`) to permanently save your geocoding and taxonomy edits.
- Once you approve a record in the **HITL (Human-in-the-Loop) tab**, it is added to the cache.
- Future documents will instantly pull coordinates and valid names from your local cache, bypassing external APIs entirely.

### Offline Zonal Geocoding (Geopandas)
To avoid overpass/nominatim API limits, place Geofabrik `.gpkg` files in the `geodata/` folder:
1. Download regional extracts (e.g., `Western_Zone.gpkg`, `Southern_Zone.gpkg`) from Geofabrik.
2. Put them in `./geodata/`.
3. BioTrace uses `geopandas` and `rapidfuzz` to search these local polygons offline first!

### Advanced Taxonomy Verification
- **GNfinder**: BioTrace hits the GNA REST API to accurately extract binomials directly from text blocks.
- **gnparser**: Scientific names are parsed into exact components (`genus`, `species`, `authorship`, `year`).
- **pytaxize fallback**: Verification cascades through WoRMS, Global Names, and finally GBIF and COL using `pytaxize`.

## Data Layout

```text
biodiversity_data/
├── reference_cache.db       # SQLite: Human-verified progressive learning cache
├── metadata_v5.db           # SQLite: Catalog of all processed PDFs
├── geonames_india.db        # SQLite: GeoNames lookup
├── wiki/                    # LLM-Wiki persistent knowledge articles
├── memory_bank.db           # Spatio-temporal relational memory bank
├── vector_store/            # ChromaDB: Embedded chunks
└── extractions_v5/          # Output Darwin Core CSVs
geodata/                     # Zonal OSM .gpkg files for offline geocoding
archive/                     # Deprecated patch scripts
```
