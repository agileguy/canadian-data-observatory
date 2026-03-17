"""Census boundary loader - downloads StatCan boundary shapefiles into PostGIS."""

import json
import logging
import os
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

CACHE_DIR = Path("/app/cache")

# StatCan 2021 Census boundary file URLs
BOUNDARY_SOURCES = {
    "provinces": {
        "url": "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000a21a_e.zip",
        "table": "geo.provinces",
    },
    "cmas": {
        "url": "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lcma_000a21a_e.zip",
        "table": "geo.cmas",
    },
    "census_divisions": {
        "url": "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lcd_000a21a_e.zip",
        "table": "geo.census_divisions",
    },
}

# Province UID to 2-char code mapping (StatCan PRUID -> province_code)
PRUID_TO_CODE = {
    "10": "NL", "11": "PE", "12": "NS", "13": "NB", "24": "QC",
    "35": "ON", "46": "MB", "47": "SK", "48": "AB", "59": "BC",
    "60": "YT", "61": "NT", "62": "NU",
}


def _download_file(url: str, dest: Path) -> Path:
    """Download a file if not already cached."""
    if dest.exists():
        logger.info("Using cached file: %s", dest.name)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s ...", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
    logger.info("Downloaded %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return dest


def _extract_shapefile(zip_path: Path) -> str:
    """Extract zip and return path to the .shp file inside."""
    extract_dir = zip_path.parent / zip_path.stem
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp file found in {zip_path}")
    return str(shp_files[0])


def _load_provinces(gdf: gpd.GeoDataFrame) -> int:
    """Load province boundaries into geo.provinces."""
    loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in gdf.iterrows():
                pruid = str(row.get("PRUID", "")).strip()
                code = PRUID_TO_CODE.get(pruid)
                if not code:
                    logger.warning("Unknown PRUID %s, skipping", pruid)
                    continue

                name_en = row.get("PRENAME", row.get("PRNAME", ""))
                name_fr = row.get("PRFNAME", row.get("PRNAME", ""))
                geojson = json.dumps(row.geometry.__geo_interface__)
                area = row.get("PRAREA", None)

                cur.execute(
                    """
                    INSERT INTO geo.provinces (province_code, name_en, name_fr, geom, area_km2, updated_at)
                    VALUES (%s, %s, %s, ST_Multi(ST_GeomFromGeoJSON(%s)), %s, now())
                    ON CONFLICT (province_code) DO UPDATE SET
                        name_en = EXCLUDED.name_en,
                        name_fr = EXCLUDED.name_fr,
                        geom = EXCLUDED.geom,
                        area_km2 = EXCLUDED.area_km2,
                        updated_at = now()
                    """,
                    (code, name_en, name_fr, geojson, area),
                )
                loaded += 1
    return loaded


def _load_cmas(gdf: gpd.GeoDataFrame) -> int:
    """Load CMA/CA boundaries into geo.cmas."""
    loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in gdf.iterrows():
                cma_uid = str(row.get("CMAUID", "")).strip()
                if not cma_uid:
                    continue

                cma_name = row.get("CMANAME", "")
                cma_type = str(row.get("CMATYPE", "")).strip()
                pruid = str(row.get("PRUID", "")).strip()
                province_code = PRUID_TO_CODE.get(pruid)
                geojson = json.dumps(row.geometry.__geo_interface__)
                area = row.get("CMAAREA", None) if "CMAAREA" in row.index else None

                # Population may not be in the boundary file
                pop = None

                cur.execute(
                    """
                    INSERT INTO geo.cmas (cma_uid, cma_name, province_code, cma_type, population_2021, geom, area_km2, updated_at)
                    VALUES (%s, %s, %s, %s, %s, ST_Multi(ST_GeomFromGeoJSON(%s)), %s, now())
                    ON CONFLICT (cma_uid) DO UPDATE SET
                        cma_name = EXCLUDED.cma_name,
                        province_code = EXCLUDED.province_code,
                        cma_type = EXCLUDED.cma_type,
                        geom = EXCLUDED.geom,
                        area_km2 = EXCLUDED.area_km2,
                        updated_at = now()
                    """,
                    (cma_uid, cma_name, province_code, cma_type, pop, geojson, area),
                )
                loaded += 1
    return loaded


def _load_census_divisions(gdf: gpd.GeoDataFrame) -> int:
    """Load census division boundaries into geo.census_divisions."""
    loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in gdf.iterrows():
                cd_uid = str(row.get("CDUID", "")).strip()
                if not cd_uid:
                    continue

                cd_name = row.get("CDNAME", "")
                cd_type = str(row.get("CDTYPE", "")).strip()
                pruid = str(row.get("PRUID", "")).strip()
                province_code = PRUID_TO_CODE.get(pruid)
                geojson = json.dumps(row.geometry.__geo_interface__)
                area = row.get("CDAREA", None) if "CDAREA" in row.index else None

                cur.execute(
                    """
                    INSERT INTO geo.census_divisions (cd_uid, cd_name, cd_type, province_code, geom, area_km2, updated_at)
                    VALUES (%s, %s, %s, %s, ST_Multi(ST_GeomFromGeoJSON(%s)), %s, now())
                    ON CONFLICT (cd_uid) DO UPDATE SET
                        cd_name = EXCLUDED.cd_name,
                        cd_type = EXCLUDED.cd_type,
                        province_code = EXCLUDED.province_code,
                        geom = EXCLUDED.geom,
                        area_km2 = EXCLUDED.area_km2,
                        updated_at = now()
                    """,
                    (cd_uid, cd_name, cd_type, province_code, geojson, area),
                )
                loaded += 1
    return loaded


def load_boundaries() -> dict:
    """Download and load all census boundary files.

    Returns a dict with counts of features loaded per layer.
    """
    results = {}

    for layer_name, source in BOUNDARY_SOURCES.items():
        logger.info("=== Loading %s boundaries ===", layer_name)

        zip_filename = source["url"].split("/")[-1]
        zip_path = CACHE_DIR / zip_filename

        try:
            _download_file(source["url"], zip_path)
            shp_path = _extract_shapefile(zip_path)

            logger.info("Reading shapefile: %s", shp_path)
            gdf = gpd.read_file(shp_path)
            logger.info("Read %d features from %s", len(gdf), layer_name)

            # Reproject to EPSG:4326 if needed
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                logger.info("Reprojecting from %s to EPSG:4326", gdf.crs)
                gdf = gdf.to_crs(epsg=4326)

            # Dispatch to layer-specific loader
            if layer_name == "provinces":
                count = _load_provinces(gdf)
            elif layer_name == "cmas":
                count = _load_cmas(gdf)
            elif layer_name == "census_divisions":
                count = _load_census_divisions(gdf)
            else:
                count = 0

            results[layer_name] = count
            logger.info("Loaded %d %s features into PostGIS", count, layer_name)

        except Exception:
            logger.exception("Failed to load %s boundaries", layer_name)
            results[layer_name] = 0

    return results
