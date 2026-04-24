"""
BioTrace Reference Database for Progressive Learning.
Provides an intermediate, human-verified cache for taxonomy, geographic, and habitat data.
Prevents redundant API calls by saving approved HITL (Human-in-the-Loop) edits.
"""
import sqlite3
import os
import json
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.path.join("biodiversity_data", "reference_cache.db")

def _get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db():
    """Initialize the intermediate reference database with required tables."""
    conn = _get_connection()
    c = conn.cursor()

    # Geographic Table
    # Stores point coordinates and optional geojson polygons for localities
    c.execute('''
        CREATE TABLE IF NOT EXISTS geographic (
            locality TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            geojson_polygon TEXT,
            approved_by TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Taxonomy Table
    # Stores validated names, their parsed components, and full taxonomic hierarchy
    c.execute('''
        CREATE TABLE IF NOT EXISTS taxonomy (
            verbatim_name TEXT PRIMARY KEY,
            valid_name TEXT,
            taxonomic_status TEXT,
            phylum TEXT,
            class_ TEXT,
            order_ TEXT,
            family_ TEXT,
            genus TEXT,
            species TEXT,
            authorship TEXT,
            year TEXT,
            worms_id TEXT,
            gbif_id TEXT,
            col_id TEXT,
            approved_by TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Habitat / Environment Table
    # Stores environmental variables associated with a species
    c.execute('''
        CREATE TABLE IF NOT EXISTS habitat (
            species TEXT PRIMARY KEY,
            temperature TEXT,
            ph TEXT,
            salinity TEXT,
            description TEXT,
            approved_by TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()

# ---------------------------------------------------------
# GEOGRAPHIC CACHE
# ---------------------------------------------------------
def get_geographic_cache(locality: str) -> dict:
def get_geographic_cache(locality: str) -> dict:
    conn = _get_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT lat, lon, geojson_polygon FROM geographic WHERE locality = ?", (locality,))
        row = c.fetchone()
        if row:
            return {
                "lat": row[0],
                "lon": row[1],
                "geojson_polygon": json.loads(row[2]) if row[2] else None
            }
    finally:
        conn.close()
    return {}
    if row:
        return {
            "lat": row[0],
            "lon": row[1],
            "geojson_polygon": json.loads(row[2]) if row[2] else None
        }
    return {}

def save_geographic_cache(locality: str, lat: float, lon: float, geojson_polygon: dict = None, approved_by: str = "HITL"):
    conn = _get_connection()
    c = conn.cursor()
    poly_str = json.dumps(geojson_polygon) if geojson_polygon else None
    c.execute("""
        INSERT OR REPLACE INTO geographic (locality, lat, lon, geojson_polygon, approved_by)
        VALUES (?, ?, ?, ?, ?)
    """, (locality, lat, lon, poly_str, approved_by))
    conn.commit()
    conn.close()

# ---------------------------------------------------------
# TAXONOMY CACHE
# ---------------------------------------------------------
def get_taxonomy_cache(verbatim_name: str) -> dict:
    conn = _get_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM taxonomy WHERE verbatim_name = ?", (verbatim_name,))
    row = c.fetchone()
    conn.close()
    if row:
        return dict(row)
    return {}

def save_taxonomy_cache(data: dict, approved_by: str = "HITL"):
    conn = _get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO taxonomy (
            verbatim_name, valid_name, taxonomic_status,
            phylum, class_, order_, family_, genus, species, authorship, year,
            worms_id, gbif_id, col_id, approved_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("verbatim_name"), data.get("valid_name"), data.get("taxonomic_status"),
        data.get("phylum"), data.get("class_"), data.get("order_"), data.get("family_"),
        data.get("genus"), data.get("species"), data.get("authorship"), data.get("year"),
        data.get("worms_id"), data.get("gbif_id"), data.get("col_id"), approved_by
    ))
    conn.commit()
    conn.close()

# ---------------------------------------------------------
# HABITAT CACHE
# ---------------------------------------------------------
def get_habitat_cache(species: str) -> dict:
    conn = _get_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM habitat WHERE species = ?", (species,))
    row = c.fetchone()
    conn.close()
    if row:
        return dict(row)
    return {}

def save_habitat_cache(species: str, data: dict, approved_by: str = "HITL"):
    conn = _get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO habitat (
            species, temperature, ph, salinity, description, approved_by
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        species, data.get("temperature"), data.get("ph"), data.get("salinity"),
        data.get("description"), approved_by
    ))
    conn.commit()
    conn.close()

# ---------------------------------------------------------
# QUEUE MANAGEMENT (Pending Edits before Cache)
# ---------------------------------------------------------
# We use in-memory queue or JSON for unapproved HITL items
def load_all_cache(table_name="geographic"):
    conn = _get_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute(f"SELECT * FROM {table_name} ORDER BY timestamp DESC")
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"Error loading {table_name} cache: {e}")
        rows = []
    finally:
        conn.close()
    return rows

def delete_cache_record(table_name: str, key_column: str, key_value: str):
    allowed_tables = {"geographic": "locality", "taxonomy": "verbatim_name", "habitat": "species"}
    if table_name not in allowed_tables or key_column != allowed_tables[table_name]:
        raise ValueError(f"Invalid table or column: {table_name}.{key_column}")
    conn = _get_connection()
    c = conn.cursor()
    try:
        c.execute(f"DELETE FROM {table_name} WHERE {key_column} = ?", (key_value,))
        conn.commit()
    except Exception as e:
        logger.error(f"Error deleting from {table_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
