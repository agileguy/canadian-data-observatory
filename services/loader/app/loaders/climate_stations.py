"""Climate station metadata loader - fetches ECCC station inventory into PostGIS."""

import csv
import io
import logging
from pathlib import Path

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

# ECCC station inventory CSV (all climate stations with metadata)
STATION_INVENTORY_URL = (
    "https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_Donnees/"
    "Station%20Inventory%20EN.csv"
)

CACHE_DIR = Path("/app/cache")

# Province name to 2-char code mapping
PROVINCE_NAME_TO_CODE = {
    "ALBERTA": "AB",
    "BRITISH COLUMBIA": "BC",
    "MANITOBA": "MB",
    "NEW BRUNSWICK": "NB",
    "NEWFOUNDLAND": "NL",
    "NORTHWEST TERRITORIES": "NT",
    "NOVA SCOTIA": "NS",
    "NUNAVUT": "NU",
    "ONTARIO": "ON",
    "PRINCE EDWARD ISLAND": "PE",
    "QUEBEC": "QC",
    "SASKATCHEWAN": "SK",
    "YUKON TERRITORY": "YT",
}


def _download_inventory(dest: Path) -> str:
    """Download the ECCC station inventory CSV and return its text content."""
    if dest.exists():
        logger.info("Using cached station inventory: %s", dest.name)
        return dest.read_text(encoding="utf-8-sig")

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading station inventory from ECCC...")
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(STATION_INVENTORY_URL)
        resp.raise_for_status()
        content = resp.text
        dest.write_text(content, encoding="utf-8")
    logger.info("Downloaded station inventory (%.1f KB)", len(content) / 1024)
    return content


def _parse_inventory_csv(content: str) -> list[dict]:
    """Parse the ECCC station inventory CSV into a list of station dicts.

    The CSV has header rows that need to be skipped (lines starting with
    non-data content). The actual data starts after a header row containing
    'Name', 'Province', 'Climate ID', etc.
    """
    stations = []
    lines = content.splitlines()

    # Find the header row - it contains 'Name' and 'Province'
    header_idx = None
    for i, line in enumerate(lines):
        if "Name" in line and "Province" in line and "Climate ID" in line:
            header_idx = i
            break

    if header_idx is None:
        # Try alternative: skip initial comment lines and use first valid CSV row
        logger.warning("Could not find expected header row, trying line-by-line parse")
        return stations

    # Parse from header row onward
    csv_text = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        try:
            name = row.get("Name", "").strip()
            province = row.get("Province", "").strip()
            climate_id = row.get("Climate ID", "").strip()
            latitude = row.get("Latitude (Decimal Degrees)", "").strip()
            longitude = row.get("Longitude (Decimal Degrees)", "").strip()
            elevation = row.get("Elevation (m)", "").strip()
            wmo_id = row.get("WMO ID", "").strip()
            tc_id = row.get("TC ID", "").strip()
            first_year = row.get("First Year", "").strip()
            last_year = row.get("Last Year", "").strip()

            if not climate_id or not latitude or not longitude:
                continue

            lat = float(latitude)
            lon = float(longitude)

            # Skip invalid coordinates
            if lat == 0 and lon == 0:
                continue

            province_code = PROVINCE_NAME_TO_CODE.get(province.upper())

            stations.append({
                "station_id": climate_id,
                "station_name": name,
                "province_code": province_code,
                "latitude": lat,
                "longitude": lon,
                "elevation_m": float(elevation) if elevation else None,
                "wmo_id": wmo_id or None,
                "tc_id": tc_id or None,
                "first_year": int(first_year) if first_year else None,
                "last_year": int(last_year) if last_year else None,
            })
        except (ValueError, KeyError):
            continue

    return stations


def load_climate_stations() -> int:
    """Download ECCC station inventory and load into geo.climate_stations.

    Returns the number of stations loaded.
    """
    cache_file = CACHE_DIR / "station_inventory_en.csv"
    content = _download_inventory(cache_file)
    stations = _parse_inventory_csv(content)

    if not stations:
        logger.error("No stations parsed from inventory CSV")
        return 0

    logger.info("Parsed %d stations from inventory", len(stations))

    # Deduplicate by station_id (CSV may have duplicate entries for
    # stations with multiple data types - hourly, daily, monthly)
    seen = {}
    for s in stations:
        sid = s["station_id"]
        if sid not in seen:
            seen[sid] = s
        else:
            # Keep the entry with the latest last_year
            existing = seen[sid]
            if (s.get("last_year") or 0) > (existing.get("last_year") or 0):
                seen[sid] = s

    unique_stations = list(seen.values())
    logger.info("Unique stations after dedup: %d", len(unique_stations))

    loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for s in unique_stations:
                try:
                    # Determine if station is active (has data in recent years)
                    is_active = s["last_year"] is not None and s["last_year"] >= 2020

                    cur.execute(
                        """
                        INSERT INTO geo.climate_stations
                            (station_id, station_name, province_code, latitude, longitude,
                             elevation_m, geom, wmo_id, tc_id, first_year, last_year, is_active, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                                %s, %s, %s, %s, %s, now())
                        ON CONFLICT (station_id) DO UPDATE SET
                            station_name = EXCLUDED.station_name,
                            province_code = EXCLUDED.province_code,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            elevation_m = EXCLUDED.elevation_m,
                            geom = EXCLUDED.geom,
                            wmo_id = EXCLUDED.wmo_id,
                            tc_id = EXCLUDED.tc_id,
                            first_year = EXCLUDED.first_year,
                            last_year = EXCLUDED.last_year,
                            is_active = EXCLUDED.is_active,
                            updated_at = now()
                        """,
                        (
                            s["station_id"], s["station_name"], s["province_code"],
                            s["latitude"], s["longitude"], s["elevation_m"],
                            s["longitude"], s["latitude"],  # MakePoint(lon, lat)
                            s["wmo_id"], s["tc_id"],
                            s["first_year"], s["last_year"], is_active,
                        ),
                    )
                    loaded += 1
                except Exception:
                    logger.exception("Failed to insert station %s", s["station_id"])

    logger.info("Loaded %d climate stations into PostGIS", loaded)
    return loaded
