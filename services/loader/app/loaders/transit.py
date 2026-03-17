"""Transit GTFS loader - downloads and parses GTFS feeds into PostGIS.

Loads stop and route data for 6 major Canadian cities from their
respective transit agency GTFS static feeds.
"""

import csv
import io
import logging
import zipfile
from typing import Dict, List, Optional, Tuple

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 120.0

# ---------------------------------------------------------------------------
# GTFS feed URLs for 6 Canadian cities
# ---------------------------------------------------------------------------
GTFS_FEEDS: Dict[str, str] = {
    "Toronto": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7795b45e-e65a-4465-81fc-c36b9dfff169/resource/cfb6b2b8-6191-41e3-bda1-b175c51148cb/download/TTC_GTFS.zip",
    "Vancouver": "https://gtfs.translink.ca/v2/gtfs-static.zip",
    "Montreal": "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip",
    "Calgary": "https://data.calgary.ca/download/npk7-z3bj/application%2Fx-zip-compressed",
    "Ottawa": "https://www.octranspo.com/files/google_transit.zip",
    "Edmonton": "https://data.edmonton.ca/api/views/mwnh-mfra/rows.csv?accessType=DOWNLOAD",
}

# Edmonton's GTFS URL may need adjustment; fallback to an alternative
EDMONTON_GTFS_ALT = "https://gtfs.edmonton.ca/TMGTFSRealTimeWebService/GTFS/GTFS.zip"


def _download_gtfs(city: str, url: str) -> Optional[bytes]:
    """Download a GTFS zip file for a city."""
    try:
        logger.info("Downloading GTFS feed for %s from %s", city, url)
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.content
    except httpx.HTTPStatusError as exc:
        logger.error(
            "GTFS download HTTP %d for %s: %s",
            exc.response.status_code,
            city,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("GTFS download failed for %s: %s", city, exc)
    return None


def _parse_stops(zip_bytes: bytes) -> List[Dict[str, str]]:
    """Extract stops from stops.txt inside a GTFS zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            if "stops.txt" not in zf.namelist():
                logger.warning("No stops.txt found in GTFS zip")
                return []

            csv_text = zf.read("stops.txt").decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(csv_text))
            return list(reader)
    except Exception:
        logger.exception("Failed to parse stops.txt")
        return []


def _parse_routes(zip_bytes: bytes) -> List[Dict[str, str]]:
    """Extract routes from routes.txt inside a GTFS zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            if "routes.txt" not in zf.namelist():
                logger.warning("No routes.txt found in GTFS zip")
                return []

            csv_text = zf.read("routes.txt").decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(csv_text))
            return list(reader)
    except Exception:
        logger.exception("Failed to parse routes.txt")
        return []


def _ensure_schema(conn) -> None:
    """Create the transit schema and tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS transit")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS transit.stops (
                id SERIAL PRIMARY KEY,
                city TEXT NOT NULL,
                stop_id TEXT NOT NULL,
                stop_name TEXT,
                stop_lat DOUBLE PRECISION,
                stop_lon DOUBLE PRECISION,
                geom GEOMETRY(Point, 4326),
                UNIQUE (city, stop_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS transit.routes (
                id SERIAL PRIMARY KEY,
                city TEXT NOT NULL,
                route_id TEXT NOT NULL,
                route_short_name TEXT,
                route_long_name TEXT,
                route_type INTEGER,
                UNIQUE (city, route_id)
            )
        """)

        # Spatial index on stops
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transit_stops_geom
            ON transit.stops USING GIST (geom)
        """)

        # Index on city for fast filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transit_stops_city
            ON transit.stops (city)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transit_routes_city
            ON transit.routes (city)
        """)


def _load_city_stops(cur, city: str, stops: List[Dict[str, str]]) -> int:
    """Insert stops for a city into transit.stops. Returns count inserted."""
    inserted = 0
    for stop in stops:
        stop_id = stop.get("stop_id", "").strip()
        stop_name = stop.get("stop_name", "").strip() or None
        lat_str = stop.get("stop_lat", "").strip()
        lon_str = stop.get("stop_lon", "").strip()

        if not stop_id:
            continue

        try:
            lat = float(lat_str) if lat_str else None
            lon = float(lon_str) if lon_str else None
        except ValueError:
            lat = lon = None

        geom_expr = "NULL"
        geom_params: list = []
        if lat is not None and lon is not None:
            geom_expr = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"
            geom_params = [lon, lat]

        cur.execute("SAVEPOINT stop_sp")
        try:
            cur.execute(
                f"""
                INSERT INTO transit.stops
                    (city, stop_id, stop_name, stop_lat, stop_lon, geom)
                VALUES
                    (%s, %s, %s, %s, %s, {geom_expr})
                ON CONFLICT (city, stop_id) DO UPDATE SET
                    stop_name = EXCLUDED.stop_name,
                    stop_lat = EXCLUDED.stop_lat,
                    stop_lon = EXCLUDED.stop_lon,
                    geom = EXCLUDED.geom
                """,
                [city, stop_id, stop_name, lat, lon, *geom_params],
            )
            cur.execute("RELEASE SAVEPOINT stop_sp")
            inserted += 1
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT stop_sp")
            logger.warning("Skipping stop %s/%s: %s", city, stop_id, exc)
            continue

    return inserted


def _load_city_routes(cur, city: str, routes: List[Dict[str, str]]) -> int:
    """Insert routes for a city into transit.routes. Returns count inserted."""
    inserted = 0
    for route in routes:
        route_id = route.get("route_id", "").strip()
        short_name = route.get("route_short_name", "").strip() or None
        long_name = route.get("route_long_name", "").strip() or None
        route_type_str = route.get("route_type", "").strip()

        if not route_id:
            continue

        try:
            route_type = int(route_type_str) if route_type_str else None
        except ValueError:
            route_type = None

        cur.execute("SAVEPOINT route_sp")
        try:
            cur.execute(
                """
                INSERT INTO transit.routes
                    (city, route_id, route_short_name, route_long_name, route_type)
                VALUES
                    (%s, %s, %s, %s, %s)
                ON CONFLICT (city, route_id) DO UPDATE SET
                    route_short_name = EXCLUDED.route_short_name,
                    route_long_name = EXCLUDED.route_long_name,
                    route_type = EXCLUDED.route_type
                """,
                [city, route_id, short_name, long_name, route_type],
            )
            cur.execute("RELEASE SAVEPOINT route_sp")
            inserted += 1
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT route_sp")
            logger.warning("Skipping route %s/%s: %s", city, route_id, exc)
            continue

    return inserted


def load_transit() -> Dict[str, Dict[str, int]]:
    """Load GTFS data for all configured cities.

    Returns:
        Dict of city -> {"stops": count, "routes": count}
    """
    results: Dict[str, Dict[str, int]] = {}

    with get_connection() as conn:
        _ensure_schema(conn)

        with conn.cursor() as cur:
            for city, url in GTFS_FEEDS.items():
                logger.info("Processing GTFS for %s...", city)

                # Edmonton may need alternative URL
                zip_bytes = _download_gtfs(city, url)
                if zip_bytes is None and city == "Edmonton":
                    logger.info("Trying Edmonton alternative GTFS URL...")
                    zip_bytes = _download_gtfs(city, EDMONTON_GTFS_ALT)

                if zip_bytes is None:
                    logger.error("Failed to download GTFS for %s", city)
                    results[city] = {"stops": 0, "routes": 0}
                    continue

                # Check if it's actually a zip file
                if not zip_bytes[:4] == b"PK\x03\x04":
                    logger.error(
                        "Downloaded file for %s is not a valid zip", city
                    )
                    results[city] = {"stops": 0, "routes": 0}
                    continue

                # Parse GTFS files
                stops = _parse_stops(zip_bytes)
                routes = _parse_routes(zip_bytes)

                logger.info(
                    "%s: parsed %d stops, %d routes",
                    city,
                    len(stops),
                    len(routes),
                )

                # Load into PostGIS
                stop_count = _load_city_stops(cur, city, stops)
                route_count = _load_city_routes(cur, city, routes)

                results[city] = {"stops": stop_count, "routes": route_count}
                logger.info(
                    "%s: loaded %d stops, %d routes",
                    city,
                    stop_count,
                    route_count,
                )

    return results
