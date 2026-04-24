import os
import glob
import sqlite3
import logging
from typing import Optional, Tuple, Dict, Any, List

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
    def __init__(self, data_dir: str = "geodata"):
        self.data_dir = data_dir
        self.db_paths = self._discover_files()
        self._name_caches = {}
        self._caches_loaded = {db_path: False for db_path in self.db_paths}

    def _discover_files(self) -> List[str]:
        if not os.path.exists(self.data_dir):
            return []
        files = glob.glob(os.path.join(self.data_dir, "*.gpkg")) + \
                glob.glob(os.path.join(self.data_dir, "*.gpkg.zip"))
        return files

    def _get_connection(self, db_path: str):
        if not os.path.exists(db_path):
            return None
        if db_path.endswith('.zip'):
            # sqlite3 cannot connect to zip files natively
            logger.warning("[OSM Handler] Cannot run native sqlite queries on zipped db: %s", db_path)
            return None
        return sqlite3.connect(db_path)

    def _get_gdal_path(self, db_path: str):
        """Returns the path prefixed with /vsizip/ if it is a zip file."""
        if db_path.endswith('.zip'):
            return f"/vsizip/{db_path}"
        return db_path

    def optimize_db(self):
        """Creates optimized indexes on the name column for fast querying."""
        for db_path in self.db_paths:
            conn = self._get_connection(db_path)
            if not conn: continue
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_osm_name ON multipolygons(name)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_osm_amenity ON multipolygons(amenity)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_osm_building ON multipolygons(building)")
                conn.commit()
                logger.info("[OSM Handler] Database indices optimized for %s.", db_path)
            except Exception as e:
                logger.debug("[OSM Handler] Could not optimize db %s: %s", db_path, e)
            finally:
                conn.close()

    def _load_name_cache(self, db_path: str):
        """Loads all non-null names into memory for rapidfuzz fuzzy matching."""
        if self._caches_loaded.get(db_path, False): return
        conn = self._get_connection(db_path)
        if not conn: return
        try:
            cursor = conn.execute("SELECT DISTINCT name FROM multipolygons WHERE name IS NOT NULL")
            self._name_caches[db_path] = [row[0] for row in cursor.fetchall()]
            self._caches_loaded[db_path] = True
            logger.info("[OSM Handler] Loaded %d unique names into cache for fuzzy matching from %s.", len(self._name_caches[db_path]), db_path)
        except Exception as e:
            logger.warning("[OSM Handler] Failed to load name cache for %s: %s", db_path, e)
        finally:
            conn.close()

    def search_locality(self, locality: str) -> Optional[Dict[str, Any]]:
        """
        Executes a 3-stage search for a locality in the GeoPackages:
        1. Exact match on 'name'.
        2. Tag-based filtering (name match where amenity or building is NOT NULL).
        3. Fuzzy match against cached names.

        Returns a dict containing 'lat', 'lon', 'geojson', and 'source_file' if found.
        """
        if not _GPD_AVAILABLE or not self.db_paths:
            return None

        locality_lower = locality.lower()

        for db_path in self.db_paths:
            try:
                gdal_path = self._get_gdal_path(db_path)

                # We use pyogrio to execute SQL directly against the GPKG for lightning fast results
                # Stage 1: Exact Match
                locality_escaped = locality_lower.replace("\'", "\'\'")
                sql_exact = f"SELECT * FROM multipolygons WHERE lower(name) = \'{locality_escaped}\' LIMIT 1"
                df = gpd.read_file(gdal_path, engine="pyogrio", sql=sql_exact)

                if not df.empty:
                    return self._extract_data(df, "exact", os.path.basename(db_path))

                # Stage 2: Tag-based Filtering
                sql_tags = f"SELECT * FROM multipolygons WHERE lower(name) LIKE \'%{locality_escaped}%\' AND (amenity IS NOT NULL OR building IS NOT NULL) LIMIT 1"
                df = gpd.read_file(gdal_path, engine="pyogrio", sql=sql_tags)

                if not df.empty:
                    return self._extract_data(df, "tag_filter", os.path.basename(db_path))

                # Stage 3: Fuzzy Match
                if _FUZZ_AVAILABLE:
                    self._load_name_cache(db_path)
                    cache = self._name_caches.get(db_path, [])
                    if cache:
                        best_match = fuzz_process.extractOne(locality, cache, scorer=fuzz.ratio)
                        if best_match and best_match[1] > 85: # Threshold
                            matched_name = best_match[0].replace("'", "''") # escape quotes
                            sql_fuzzy = f"SELECT * FROM multipolygons WHERE name = '{matched_name}' LIMIT 1"
                            df = gpd.read_file(gdal_path, engine="pyogrio", sql=sql_fuzzy)
                            if not df.empty:
                                return self._extract_data(df, f"fuzzy_{best_match[1]:.0f}", os.path.basename(db_path))

            except Exception as e:
                logger.error("[OSM Handler] Search failed in %s: %s", db_path, e)

        return None

    def _extract_data(self, df, match_type: str, source_file: str) -> Dict[str, Any]:
        geom = df.iloc[0].geometry
        return {
            "lat": geom.centroid.y,
            "lon": geom.centroid.x,
            "geojson": geom.__geo_interface__,
            "match_type": match_type,
            "source_file": source_file
        }

    def get_spatial_context(self, lat: float, lon: float) -> Dict[str, str]:
        """
        Reverse-lookups the surrounding context (district/state) for a point
        by checking which boundary polygons contain it.
        """
        context = {"state": "", "district": ""}
        if not _GPD_AVAILABLE or not self.db_paths:
            return context

        try:
            for db_path in self.db_paths:
                # Read only administrative boundaries to save time
                sql_admin = "SELECT name, admin_level, geom FROM multipolygons WHERE boundary='administrative'"
                admin_df = gpd.read_file(self._get_gdal_path(db_path), engine="pyogrio", sql=sql_admin)

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
                    break # Assuming context is found in one DB
        except Exception as e:
            logger.warning("[OSM Handler] Spatial context error: %s", e)

        return context
