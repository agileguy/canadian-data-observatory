"""Demographics collector - fetches Canadian population data from Statistics Canada.

Uses CSV bulk download from StatCan Table 17-10-0005-01 (Population estimates,
quarterly) rather than individual vector IDs, which is more reliable for this
particular table.
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
# StatCan CSV download URL for Table 17-10-0005-01
# ---------------------------------------------------------------------------
TABLE_CSV_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/17100005-eng.zip"

# Province GEO strings in the CSV mapped to our province codes
GEO_TO_CODE: Dict[str, str] = {
    "Canada": "CA",
    "Ontario": "ON",
    "Quebec": "QC",
    "British Columbia": "BC",
    "Alberta": "AB",
    "Manitoba": "MB",
    "Saskatchewan": "SK",
    "Nova Scotia": "NS",
    "New Brunswick": "NB",
    "Newfoundland and Labrador": "NL",
    "Prince Edward Island": "PE",
}

# ---------------------------------------------------------------------------
# Prometheus gauges
# ---------------------------------------------------------------------------
population_gauge = Gauge(
    "cdo_demographics_population_total",
    "Total population estimate",
    ["province"],
)
growth_rate_gauge = Gauge(
    "cdo_demographics_population_growth_rate_percent",
    "Population growth rate (%)",
    ["province"],
)
median_age_gauge = Gauge(
    "cdo_demographics_median_age_years",
    "Median age in years",
    ["province"],
)
births_gauge = Gauge(
    "cdo_demographics_births_total",
    "Total births",
    ["province"],
)
deaths_gauge = Gauge(
    "cdo_demographics_deaths_total",
    "Total deaths",
    ["province"],
)
net_migration_gauge = Gauge(
    "cdo_demographics_net_migration_total",
    "Net international migration",
    ["province"],
)
last_update_gauge = Gauge(
    "cdo_demographics_last_update_timestamp",
    "Timestamp of last successful demographics data update (unix epoch)",
)

# Redis cache settings
CACHE_KEY = "demographics:statcan"
CACHE_TTL = 604800  # 7 days


async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch population and demographic data from StatCan, update gauges.

    Uses Redis cache with a 7-day TTL since population estimates
    update quarterly.
    """
    cached = await cache.get(CACHE_KEY)
    if cached:
        logger.info("Demographics data served from cache")
        _apply_cached(cached)
        return

    logger.info("Fetching fresh demographics data from Statistics Canada")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_statcan_csv)

        if data:
            _apply_cached(data)
            await cache.set(CACHE_KEY, data, ttl=CACHE_TTL)
            last_update_gauge.set(time.time())
            logger.info(
                "Demographics data updated: %d indicators",
                len(data),
            )
        else:
            logger.warning("StatCan demographics fetch returned empty data")
    except Exception:
        logger.exception("Failed to fetch demographics data from StatCan")


def _fetch_statcan_csv() -> Optional[Dict[str, Any]]:
    """Download and parse StatCan Table 17-10-0005-01 CSV.

    Downloads the zip file, extracts the CSV, and parses population
    estimates by province. This is more reliable than individual vector
    IDs for this table.
    """
    try:
        logger.debug("Downloading demographics CSV from %s", TABLE_CSV_URL)
        resp = httpx.get(TABLE_CSV_URL, timeout=60.0, follow_redirects=True)
        resp.raise_for_status()

        # Extract CSV from zip
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                logger.error("No CSV file found in demographics zip")
                return None

            csv_data = zf.read(csv_names[0]).decode("utf-8-sig")

        # Parse CSV — find latest population estimates per province
        reader = csv.DictReader(io.StringIO(csv_data))

        # Track all values per province keyed by ref_date for growth calc
        all_values: Dict[str, Dict[str, float]] = {}

        for row in reader:
            geo = row.get("GEO", "").strip()
            ref_date = row.get("REF_DATE", "").strip()
            value_str = row.get("VALUE", "").strip()

            if geo not in GEO_TO_CODE:
                continue

            code = GEO_TO_CODE[geo]

            if not value_str:
                continue

            try:
                value = float(value_str)
            except ValueError:
                continue

            all_values.setdefault(code, {})[ref_date] = value

        if not all_values:
            logger.warning("No population data parsed from CSV")
            return None

        # Extract latest population per province
        populations: Dict[str, float] = {}
        for code, date_vals in all_values.items():
            latest_date = max(date_vals.keys())
            populations[code] = date_vals[latest_date]

        results: Dict[str, Any] = {"populations": populations}

        # Compute growth rates from the two most recent periods per province
        growth_rates: Dict[str, float] = {}
        for code, date_vals in all_values.items():
            sorted_dates = sorted(date_vals.keys())
            if len(sorted_dates) >= 2:
                prev_val = date_vals[sorted_dates[-2]]
                curr_val = date_vals[sorted_dates[-1]]
                if prev_val > 0:
                    rate = ((curr_val - prev_val) / prev_val) * 100.0
                    growth_rates[code] = round(rate, 4)

        if growth_rates:
            results["growth_rates"] = growth_rates

        logger.info(
            "Parsed population data for %d provinces/territories, %d growth rates",
            len(populations),
            len(growth_rates),
        )
        return results

    except httpx.HTTPError as exc:
        logger.error("HTTP error downloading demographics CSV: %s", exc)
        return None
    except Exception:
        logger.exception("Failed to parse demographics CSV")
        return None


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached demographic data to Prometheus gauges."""
    # Population by province
    populations = data.get("populations", {})
    for province, pop in populations.items():
        population_gauge.labels(province=province).set(pop)

    # Growth rates by province
    growth_rates = data.get("growth_rates", {})
    for province, rate in growth_rates.items():
        growth_rate_gauge.labels(province=province).set(rate)

    # National demographic indicators
    if "median_age_CA" in data:
        median_age_gauge.labels(province="CA").set(data["median_age_CA"])

    if "births_CA" in data:
        births_gauge.labels(province="CA").set(data["births_CA"])

    if "deaths_CA" in data:
        deaths_gauge.labels(province="CA").set(data["deaths_CA"])

    if "net_migration_CA" in data:
        net_migration_gauge.labels(province="CA").set(data["net_migration_CA"])
