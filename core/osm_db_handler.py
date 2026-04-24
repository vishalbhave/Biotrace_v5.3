import os
import sqlite3
import logging
from typing import Optional, Tuple, Dict, Any

try:
    import geopandas as gpd
    import pyogrio
    from shapely import wkt
    from shapely.geometry import Point
    _GPD_AVAILABLE = True
except ImportError:
    _GPD_AVAILABLE = False

try:
    from rapidfuzz import fuzz
    from rapidfuzz import process as fuzz_process
    _FUZZ_AVAILABLE = True
except ImportError:
    _FUZZ_AVAILABLE = False

logger = logging.getLogger("biotrace.osm_db")

class OSMDatabaseHandler:
    def __init__(self, db_path: str = "geodata/india_data.gpkg"):
        self.db_path = db_path
        self._name_cache = []
        self._cache_loaded = False

    def _get_connection(self):
        if not os.path.exists(self.db_path):
            return None
        if self.db_path.endswith('.zip'):
            # sqlite3 cannot connect to zip files natively
            logger.warning("[OSM Handler] Cannot run native sqlite queries on zipped db: %s", self.db_path)
            return None
        return sqlite3.connect(self.db_path)

    def _get_gdal_path(self):
        """Returns the path prefixed with /vsizip/ if it is a zip file."""
        if self.db_path.endswith('.zip'):
            return f"/vsizip/{self.db_path}"
        return self.db_path

    def optimize_db(self):
        """Creates optimized indexes on the name column for fast querying."""
        conn = self._get_connection()
        if not conn: return
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_osm_name ON multipolygons(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_osm_amenity ON multipolygons(amenity)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_osm_building ON multipolygons(building)")
            conn.commit()
            logger.info("[OSM Handler] Database indices optimized.")
        except Exception as e:
            logger.debug("[OSM Handler] Could not optimize db: %s", e)
        finally:
            conn.close()

    def _load_name_cache(self):
        """Loads all non-null names into memory for rapidfuzz fuzzy matching."""
        if self._cache_loaded: return
        conn = self._get_connection()
        if not conn: return
        try:
            cursor = conn.execute("SELECT DISTINCT name FROM multipolygons WHERE name IS NOT NULL")
            self._name_cache = [row[0] for row in cursor.fetchall()]
            self._cache_loaded = True
            logger.info("[OSM Handler] Loaded %d unique names into cache for fuzzy matching.", len(self._name_cache))
        except Exception as e:
            logger.warning("[OSM Handler] Failed to load name cache: %s", e)
        finally:
            conn.close()

    def search_locality(self, locality: str) -> Optional[Dict[str, Any]]:
        """
        Executes a 3-stage search for a locality in the GeoPackage:
        1. Exact match on 'name'.
        2. Tag-based filtering (name match where amenity or building is NOT NULL).
        3. Fuzzy match against cached names.

        Returns a dict containing 'lat', 'lon', and 'geojson' if found.
        """
        if not _GPD_AVAILABLE or not os.path.exists(self.db_path):
            logger.debug("[OSM Handler] Dependencies missing or DB %s not found.", self.db_path)
            return None

        result = None
        locality_lower = locality.lower()

        try:
            # We use pyogrio to execute SQL directly against the GPKG for lightning fast results
            # Stage 1: Exact Match
            sql_exact = f"SELECT * FROM multipolygons WHERE lower(name) = '{locality_lower}' LIMIT 1"
            df = gpd.read_file(self._get_gdal_path(), engine="pyogrio", sql=sql_exact)

            if not df.empty:
                return self._extract_data(df, "exact")

            # Stage 2: Tag-based Filtering
            sql_tags = f"SELECT * FROM multipolygons WHERE lower(name) LIKE '%{locality_lower}%' AND (amenity IS NOT NULL OR building IS NOT NULL) LIMIT 1"
            df = gpd.read_file(self._get_gdal_path(), engine="pyogrio", sql=sql_tags)

            if not df.empty:
                return self._extract_data(df, "tag_filter")

            # Stage 3: Fuzzy Match
            if _FUZZ_AVAILABLE:
                self._load_name_cache()
                if self._name_cache:
                    best_match = fuzz_process.extractOne(locality, self._name_cache, scorer=fuzz.ratio)
                    if best_match and best_match[1] > 85: # Threshold
                        matched_name = best_match[0].replace("'", "''") # escape quotes
                        sql_fuzzy = f"SELECT * FROM multipolygons WHERE name = '{matched_name}' LIMIT 1"
                        df = gpd.read_file(self._get_gdal_path(), engine="pyogrio", sql=sql_fuzzy)
                        if not df.empty:
                            return self._extract_data(df, f"fuzzy_{best_match[1]:.0f}")

        except Exception as e:
            logger.error("[OSM Handler] Search failed: %s", e)

        return None

    def _extract_data(self, df, match_type: str) -> Dict[str, Any]:
        geom = df.iloc[0].geometry
        return {
            "lat": geom.centroid.y,
            "lon": geom.centroid.x,
            "geojson": geom.__geo_interface__,
            "match_type": match_type
        }

    def get_spatial_context(self, lat: float, lon: float) -> Dict[str, str]:
        """
        Reverse-lookups the surrounding context (district/state) for a point
        by checking which boundary polygons contain it.
        """
        context = {"state": "", "district": ""}
        if not _GPD_AVAILABLE or not os.path.exists(self.db_path):
            return context

        try:
            # Read only administrative boundaries to save time
            sql_admin = "SELECT name, admin_level, geom FROM multipolygons WHERE boundary='administrative'"
            admin_df = gpd.read_file(self._get_gdal_path(), engine="pyogrio", sql=sql_admin)

            pt = Point(lon, lat)

            # Find polygons containing the point
            containing = admin_df[admin_df.geometry.contains(pt)]

            if not containing.empty:
                if "admin_level" in containing.columns:
                    # Convert to numeric safely and sort (lower number = larger area usually, e.g. state=4, district=6)
                    # We sort descending so the highest number (most specific) is first
                    containing["admin_level_num"] = containing["admin_level"].apply(lambda x: int(x) if str(x).isdigit() else 0)
                    containing = containing.sort_values(by="admin_level_num", ascending=False)

                names = containing["name"].dropna().tolist()
                if len(names) > 0:
                    context["district"] = names[0] # Most specific
                if len(names) > 1:
                    context["state"] = names[1] # Broader
        except Exception as e:
            logger.warning("[OSM Handler] Spatial context error: %s", e)

        return context
