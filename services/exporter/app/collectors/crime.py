"""Crime collector - fetches Canadian crime statistics from Statistics Canada.

Uses CSV bulk download from StatCan Table 35-10-0026-01 (Crime Severity Index)
rather than the stats_can library, which has pydantic compatibility issues.
Follows the same pattern as demographics.py.
"""

import asyncio
import csv
import io
import logging
import time
import zipfile
from typing import Any, Dict, Optional

import httpx
from prometheus_client import Gauge

from app.cache import RedisCache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# StatCan CSV download URL for CSI (Table 35-10-0026-01)
# ---------------------------------------------------------------------------
CSI_CSV_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/35100026-eng.zip"

# ---------------------------------------------------------------------------
# Prometheus Gauges (SRD naming convention)
# ---------------------------------------------------------------------------
crime_severity_index_gauge = Gauge(
    "cdo_crime_severity_index",
    "Crime Severity Index by province and type",
    ["province", "type"],
)
crime_rate_gauge = Gauge(
    "cdo_crime_rate_per_100k",
    "Crime rate per 100,000 population",
    ["province"],
)
crime_incidents_gauge = Gauge(
    "cdo_crime_incidents_total",
    "Total crime incidents by province and offence type",
    ["province", "offence_type"],
)
last_update_gauge = Gauge(
    "cdo_crime_last_update_timestamp",
    "Timestamp of last successful crime data update (unix epoch)",
)

# ---------------------------------------------------------------------------
# Province mappings
# ---------------------------------------------------------------------------
PROVINCE_CODES: Dict[str, str] = {
    "Canada": "CA",
    "Newfoundland and Labrador": "NL",
    "Prince Edward Island": "PE",
    "Nova Scotia": "NS",
    "New Brunswick": "NB",
    "Quebec": "QC",
    "Ontario": "ON",
    "Manitoba": "MB",
    "Saskatchewan": "SK",
    "Alberta": "AB",
    "British Columbia": "BC",
    "Yukon": "YT",
    "Northwest Territories": "NT",
    "Nunavut": "NU",
}

# Statistics/indicator strings in the CSV that map to our types
# Table 35-10-0026-01 has a "Statistics" column with values like:
# "Total Crime Severity Index", "Violent Crime Severity Index",
# "Non-violent Crime Severity Index", "Total crime rate"
CSI_STAT_MAP: Dict[str, str] = {
    "Total Crime Severity Index": "total",
    "Violent Crime Severity Index": "violent",
    "Non-violent Crime Severity Index": "nonviolent",
}


async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch crime data from StatCan, update Prometheus gauges.

    Uses Redis cache to avoid hammering StatCan API. Cache TTL is 7 days
    since crime statistics update annually.
    """
    cache_key = "crime:indicators"
    ttl = 604800  # 7 days

    # Try cache first
    cached = await cache.get(cache_key)
    if cached:
        logger.info("Crime data served from cache")
        _apply_cached(cached)
        return

    # Fetch fresh data from StatCan
    logger.info("Fetching fresh crime data from Statistics Canada")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_statcan)

        if data:
            _apply_cached(data)
            await cache.set(cache_key, data, ttl=ttl)
            last_update_gauge.set(time.time())
            logger.info("Crime data updated successfully: %d keys", len(data))
        else:
            logger.warning("StatCan crime fetch returned empty data")
    except Exception:
        logger.exception("Failed to fetch crime data from StatCan")


def _fetch_statcan() -> Optional[Dict[str, Any]]:
    """Synchronous fetch from Statistics Canada using CSV bulk download.

    Downloads the CSI table zip, extracts the CSV, and parses CSI values
    by province. Runs in a thread executor.
    """
    try:
        logger.debug("Downloading crime CSV from %s", CSI_CSV_URL)
        resp = httpx.get(CSI_CSV_URL, timeout=120.0, follow_redirects=True)
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                logger.error("No CSV file found in crime zip")
                return None
            csv_text = zf.read(csv_names[0]).decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(csv_text))
        results: Dict[str, Any] = {}
        best_date: Dict[str, str] = {}  # key -> best REF_DATE

        for row in reader:
            geo = row.get("GEO", "").strip()
            ref_date = row.get("REF_DATE", "").strip()
            value_str = row.get("VALUE", "").strip()
            statistics = row.get("Statistics", "").strip()

            if geo not in PROVINCE_CODES or not value_str:
                continue

            prov_code = PROVINCE_CODES[geo]

            try:
                value = float(value_str)
            except ValueError:
                continue

            # Map the Statistics field to our CSI types
            if statistics in CSI_STAT_MAP:
                csi_type = CSI_STAT_MAP[statistics]
                key = f"csi:{prov_code}:{csi_type}"
                if key not in best_date or ref_date > best_date[key]:
                    best_date[key] = ref_date
                    results[key] = value

            # Crime rate per 100k (look for "crime rate" in statistics)
            if "crime rate" in statistics.lower() or "Police-reported crime rate" in statistics:
                key = f"crime_rate:{prov_code}"
                if key not in best_date or ref_date > best_date[key]:
                    best_date[key] = ref_date
                    results[key] = value

        if results:
            logger.info(
                "Parsed crime data: %d indicators for latest year",
                len(results),
            )
        return results if results else None

    except httpx.HTTPError as exc:
        logger.error("HTTP error downloading crime CSV: %s", exc)
        return None
    except Exception:
        logger.exception("Failed to parse crime CSV")
        return None


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached data values to Prometheus gauges."""
    for prov_code in PROVINCE_CODES.values():
        # Crime Severity Index by type
        for csi_type in ("total", "violent", "nonviolent"):
            key = f"csi:{prov_code}:{csi_type}"
            if key in data:
                crime_severity_index_gauge.labels(
                    province=prov_code, type=csi_type
                ).set(data[key])

        # Crime rate per 100k
        key = f"crime_rate:{prov_code}"
        if key in data:
            crime_rate_gauge.labels(province=prov_code).set(data[key])
