"""Housing collector - fetches Canadian housing market data from Statistics Canada.

Uses CSV bulk download from StatCan tables rather than the stats_can library,
which has pydantic compatibility issues. Follows the same pattern as demographics.py.
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
# StatCan CSV download URL for NHPI (Table 18-10-0205-01)
# ---------------------------------------------------------------------------
NHPI_CSV_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/18100205-eng.zip"

# ---------------------------------------------------------------------------
# Prometheus Gauges (SRD naming convention)
# ---------------------------------------------------------------------------
housing_starts_gauge = Gauge(
    "cdo_housing_starts_total",
    "Housing starts by CMA and type",
    ["cma", "type"],
)
housing_price_index_gauge = Gauge(
    "cdo_housing_price_index",
    "New Housing Price Index (2017=100)",
    ["cma"],
)
avg_price_gauge = Gauge(
    "cdo_housing_avg_price_dollars",
    "Average residential property price in dollars",
    ["cma"],
)
vacancy_rate_gauge = Gauge(
    "cdo_housing_vacancy_rate_percent",
    "Rental vacancy rate (%)",
    ["cma"],
)
avg_rent_gauge = Gauge(
    "cdo_housing_avg_rent_dollars",
    "Average monthly rent in dollars",
    ["cma", "bedrooms"],
)
price_to_income_gauge = Gauge(
    "cdo_housing_price_to_income_ratio",
    "Housing price-to-income ratio",
    ["cma"],
)
last_update_gauge = Gauge(
    "cdo_housing_last_update_timestamp",
    "Timestamp of last successful housing data update (unix epoch)",
)

# ---------------------------------------------------------------------------
# Top 10 CMAs tracked
# ---------------------------------------------------------------------------
TOP_CMAS = [
    "Toronto",
    "Vancouver",
    "Montreal",
    "Calgary",
    "Edmonton",
    "Ottawa-Gatineau",
    "Winnipeg",
    "Halifax",
    "Victoria",
    "Hamilton",
]

# GEO strings in StatCan CSV that map to our CMA names
# The CSV uses full CMA names like "Ottawa-Gatineau, Ontario/Quebec"
GEO_TO_CMA: Dict[str, str] = {
    "Toronto, Ontario": "Toronto",
    "Vancouver, British Columbia": "Vancouver",
    "Montréal, Quebec": "Montreal",
    "Montreal, Quebec": "Montreal",
    "Calgary, Alberta": "Calgary",
    "Edmonton, Alberta": "Edmonton",
    "Ottawa-Gatineau, Ontario/Quebec": "Ottawa-Gatineau",
    "Ottawa-Gatineau, Ontario part, Ontario/Quebec": "Ottawa-Gatineau",
    "Winnipeg, Manitoba": "Winnipeg",
    "Halifax, Nova Scotia": "Halifax",
    "Victoria, British Columbia": "Victoria",
    "Hamilton, Ontario": "Hamilton",
}

# TODO: Housing starts data requires CMHC API or Table 34-10-0135-01 CSV.
# Using placeholder values based on CMHC 2024 estimates until CSV source identified.
PLACEHOLDER_STARTS: Dict[str, Dict[str, float]] = {
    "Toronto":         {"all": 45000, "single": 8000, "multi": 37000},
    "Vancouver":       {"all": 30000, "single": 4000, "multi": 26000},
    "Montreal":        {"all": 28000, "single": 5000, "multi": 23000},
    "Calgary":         {"all": 22000, "single": 7000, "multi": 15000},
    "Edmonton":        {"all": 18000, "single": 6000, "multi": 12000},
    "Ottawa-Gatineau": {"all": 12000, "single": 3500, "multi": 8500},
    "Winnipeg":        {"all": 6000, "single": 2000, "multi": 4000},
    "Halifax":         {"all": 5000, "single": 1500, "multi": 3500},
    "Victoria":        {"all": 4000, "single": 1000, "multi": 3000},
    "Hamilton":        {"all": 5500, "single": 1800, "multi": 3700},
}

# TODO: Vacancy rate data requires CMHC Rental Market Survey or Table 34-10-0127-01 CSV.
# Using placeholder values based on CMHC 2024 Q3 estimates.
PLACEHOLDER_VACANCY: Dict[str, float] = {
    "Toronto": 1.4, "Vancouver": 0.9, "Montreal": 2.1, "Calgary": 1.6,
    "Edmonton": 2.4, "Ottawa-Gatineau": 2.0, "Winnipeg": 1.8,
    "Halifax": 1.0, "Victoria": 1.2, "Hamilton": 1.5,
}

# TODO: Rent data requires CMHC Table 34-10-0133-01 CSV.
# Using placeholder values based on CMHC 2024 averages.
PLACEHOLDER_RENT: Dict[str, Dict[str, float]] = {
    "Toronto":         {"total": 1750, "1br": 1550, "2br": 1850},
    "Vancouver":       {"total": 1850, "1br": 1650, "2br": 2000},
    "Montreal":        {"total": 1100, "1br": 950, "2br": 1200},
    "Calgary":         {"total": 1450, "1br": 1300, "2br": 1550},
    "Edmonton":        {"total": 1250, "1br": 1100, "2br": 1350},
    "Ottawa-Gatineau": {"total": 1400, "1br": 1250, "2br": 1500},
    "Winnipeg":        {"total": 1100, "1br": 950, "2br": 1200},
    "Halifax":         {"total": 1400, "1br": 1200, "2br": 1500},
    "Victoria":        {"total": 1600, "1br": 1400, "2br": 1700},
    "Hamilton":        {"total": 1450, "1br": 1300, "2br": 1550},
}


async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch housing data from StatCan, update Prometheus gauges.

    Uses Redis cache to avoid hammering StatCan API. Cache TTL is 24h
    since housing data updates monthly at most.
    """
    cache_key = "housing:indicators"
    ttl = 86400  # 24 hours

    # Try cache first
    cached = await cache.get(cache_key)
    if cached:
        logger.info("Housing data served from cache")
        _apply_cached(cached)
        return

    # Fetch fresh data from StatCan
    logger.info("Fetching fresh housing data from Statistics Canada")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_statcan)

        if data:
            _apply_cached(data)
            await cache.set(cache_key, data, ttl=ttl)
            last_update_gauge.set(time.time())
            logger.info("Housing data updated successfully: %d keys", len(data))
        else:
            logger.warning("StatCan housing fetch returned empty data")
    except Exception:
        logger.exception("Failed to fetch housing data from StatCan")


def _download_csv(url: str) -> Optional[str]:
    """Download a StatCan CSV zip and return the extracted CSV text."""
    try:
        logger.debug("Downloading CSV from %s", url)
        resp = httpx.get(url, timeout=120.0, follow_redirects=True)
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                logger.error("No CSV file found in zip from %s", url)
                return None
            return zf.read(csv_names[0]).decode("utf-8-sig")

    except httpx.HTTPError as exc:
        logger.error("HTTP error downloading %s: %s", url, exc)
        return None
    except Exception:
        logger.exception("Failed to download/extract CSV from %s", url)
        return None


def _fetch_statcan() -> Optional[Dict[str, Any]]:
    """Synchronous fetch from Statistics Canada using CSV bulk download.

    Downloads the NHPI table, extracts values for top CMAs.
    Uses placeholder data for starts/vacancy/rent until CMHC CSV sources identified.
    """
    results: Dict[str, Any] = {}

    # --- NHPI (Table 18-10-0205-01) ---
    csv_text = _download_csv(NHPI_CSV_URL)
    if csv_text:
        reader = csv.DictReader(io.StringIO(csv_text))
        best_date: Dict[str, str] = {}

        for row in reader:
            geo = row.get("GEO", "").strip()
            ref_date = row.get("REF_DATE", "").strip()
            value_str = row.get("VALUE", "").strip()

            # Match GEO to our CMA names
            cma = None
            for geo_pattern, cma_name in GEO_TO_CMA.items():
                if geo_pattern.lower() in geo.lower() or geo.lower() in geo_pattern.lower():
                    cma = cma_name
                    break

            if not cma or not value_str:
                continue

            # Filter for "Total (house and land)" or similar aggregate
            nhpi_component = row.get("New housing price indexes", "").strip()
            if nhpi_component and "total" not in nhpi_component.lower():
                continue

            try:
                value = float(value_str)
            except ValueError:
                continue

            key = f"nhpi:{cma}"
            if cma not in best_date or ref_date > best_date[cma]:
                best_date[cma] = ref_date
                results[key] = value
                logger.debug("NHPI %s = %s (date: %s)", cma, value, ref_date)

    # --- Housing starts (placeholder) ---
    # TODO: Replace with CSV download from StatCan Table 34-10-0135-01
    for cma, types in PLACEHOLDER_STARTS.items():
        for stype, val in types.items():
            results[f"starts:{cma}:{stype}"] = val

    # --- Vacancy rates (placeholder) ---
    # TODO: Replace with CSV download from CMHC Table 34-10-0127-01
    for cma, val in PLACEHOLDER_VACANCY.items():
        results[f"vacancy:{cma}"] = val

    # --- Rent (placeholder) ---
    # TODO: Replace with CSV download from CMHC Table 34-10-0133-01
    for cma, bedrooms in PLACEHOLDER_RENT.items():
        for btype, val in bedrooms.items():
            results[f"rent:{cma}:{btype}"] = val

    return results if results else None


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached data values to Prometheus gauges."""
    for cma in TOP_CMAS:
        # Housing Price Index
        key = f"nhpi:{cma}"
        if key in data:
            housing_price_index_gauge.labels(cma=cma).set(data[key])

        # Average price
        key = f"avg_price:{cma}"
        if key in data:
            avg_price_gauge.labels(cma=cma).set(data[key])

        # Housing starts by type
        for stype in ("all", "single", "multi"):
            key = f"starts:{cma}:{stype}"
            if key in data:
                housing_starts_gauge.labels(cma=cma, type=stype).set(data[key])

        # Vacancy rates
        key = f"vacancy:{cma}"
        if key in data:
            vacancy_rate_gauge.labels(cma=cma).set(data[key])

        # Rents by bedroom type
        for btype in ("total", "1br", "2br"):
            key = f"rent:{cma}:{btype}"
            if key in data:
                avg_rent_gauge.labels(cma=cma, bedrooms=btype).set(data[key])

        # Price-to-income ratio (computed if both values available)
        price_key = f"avg_price:{cma}"
        income_key = f"income:{cma}"
        if price_key in data and income_key in data and data[income_key] > 0:
            ratio = data[price_key] / data[income_key]
            price_to_income_gauge.labels(cma=cma).set(ratio)
