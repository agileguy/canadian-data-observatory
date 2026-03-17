"""Economy collector - fetches Canadian economic indicators from Statistics Canada.

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
from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# StatCan CSV download URLs (table ID -> zip URL)
# ---------------------------------------------------------------------------
STATCAN_TABLES: Dict[str, str] = {
    "gdp": "https://www150.statcan.gc.ca/n1/tbl/csv/36100434-eng.zip",
    "cpi": "https://www150.statcan.gc.ca/n1/tbl/csv/18100004-eng.zip",
    "unemployment": "https://www150.statcan.gc.ca/n1/tbl/csv/14100287-eng.zip",
    "trade": "https://www150.statcan.gc.ca/n1/tbl/csv/12100011-eng.zip",
    "retail": "https://www150.statcan.gc.ca/n1/tbl/csv/20100008-eng.zip",
    "interest_rates": "https://www150.statcan.gc.ca/n1/tbl/csv/10100122-eng.zip",
}

# Prometheus Gauges for economic indicators
# Names and labels match the Grafana dashboard queries (SRD convention)
gdp_gauge = Gauge(
    "cdo_economy_gdp_millions",
    "Canada GDP at basic prices, seasonally adjusted (millions CAD)",
    ["province", "frequency"],
)
cpi_gauge = Gauge(
    "cdo_economy_cpi_index",
    "Consumer Price Index (2002=100)",
    ["province", "basket"],
)
unemployment_gauge = Gauge(
    "cdo_economy_unemployment_rate_percent",
    "Unemployment rate, seasonally adjusted (%)",
    ["province"],
)
employment_gauge = Gauge(
    "cdo_economy_employment_total",
    "Employment count, seasonally adjusted (thousands)",
    ["province", "industry"],
)
exports_gauge = Gauge(
    "cdo_economy_exports_millions",
    "Total exports of goods (millions CAD)",
    ["province"],
)
imports_gauge = Gauge(
    "cdo_economy_imports_millions",
    "Total imports of goods (millions CAD)",
    ["province"],
)
trade_balance_gauge = Gauge(
    "cdo_economy_trade_balance_millions",
    "Trade balance (exports - imports, millions CAD)",
    ["province"],
)
interest_rate_gauge = Gauge(
    "cdo_economy_interest_rate_percent",
    "Bank of Canada policy interest rate (%)",
    ["type"],
)
retail_sales_gauge = Gauge(
    "cdo_economy_retail_sales_millions",
    "Retail trade sales (millions CAD, seasonally adjusted)",
    ["province"],
)
last_update_gauge = Gauge(
    "cdo_economy_last_update_timestamp",
    "Timestamp of last successful economy data update (unix epoch)",
)


async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch economic indicators from StatCan, update Prometheus gauges.

    Uses Redis cache to avoid hammering StatCan API. Cache TTL is 24h
    since most economic indicators update monthly.
    """
    cache_key = "economy:indicators"
    ttl = settings.CACHE_TTLS["economy"]

    # Try cache first
    cached = await cache.get(cache_key)
    if cached:
        logger.info("Economy data served from cache")
        _apply_cached(cached)
        return

    # Fetch fresh data from StatCan
    logger.info("Fetching fresh economy data from Statistics Canada")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_statcan)

        if data:
            _apply_cached(data)
            await cache.set(cache_key, data, ttl=ttl)
            last_update_gauge.set(time.time())
            logger.info("Economy data updated successfully: %d indicators", len(data))
        else:
            logger.warning("StatCan fetch returned empty data")
    except Exception:
        logger.exception("Failed to fetch economy data from StatCan")


def _download_and_extract_latest(
    url: str,
    geo_filter: str = "Canada",
    member_filter: Optional[str] = None,
    member_field: Optional[str] = None,
) -> Optional[float]:
    """Download a StatCan CSV zip and extract the latest VALUE for a GEO.

    Streams the zip to a temp file to avoid memory issues with large CSVs.
    Reads the CSV line-by-line to keep memory usage constant.
    """
    import tempfile
    import os

    tmp_path = None
    try:
        logger.debug("Downloading CSV from %s", url)
        # Stream download to temp file
        tmp_path = tempfile.mktemp(suffix=".zip")
        with httpx.stream("GET", url, timeout=120.0, follow_redirects=True) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_bytes(8192):
                    f.write(chunk)

        with zipfile.ZipFile(tmp_path) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                logger.error("No CSV file found in zip from %s", url)
                return None

            best_date = ""
            best_value: Optional[float] = None

            with zf.open(csv_names[0]) as csv_file:
                text_stream = io.TextIOWrapper(csv_file, encoding="utf-8-sig")
                reader = csv.DictReader(text_stream)

                for row in reader:
                    geo = row.get("GEO", "").strip()
                    if geo != geo_filter:
                        continue

                    if member_filter and member_field:
                        field_val = row.get(member_field, "").strip()
                        if member_filter.lower() not in field_val.lower():
                            continue

                    ref_date = row.get("REF_DATE", "").strip()
                    value_str = row.get("VALUE", "").strip()
                    if not value_str:
                        continue

                    try:
                        value = float(value_str)
                    except ValueError:
                        continue

                    if ref_date > best_date:
                        best_date = ref_date
                        best_value = value

            if best_value is not None:
                logger.debug("Extracted value %s (date %s) from %s", best_value, best_date, url)
            return best_value

    except httpx.HTTPError as exc:
        logger.error("HTTP error downloading %s: %s", url, exc)
        return None
    except Exception:
        logger.exception("Failed to download/extract CSV from %s", url)
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _fetch_statcan() -> Optional[Dict[str, Any]]:
    """Synchronous fetch from Statistics Canada using CSV bulk download.

    Downloads zip files for each table, streams CSVs line-by-line to avoid
    memory issues with large tables. Runs in a thread executor.
    """
    results: Dict[str, Any] = {}

    # --- GDP (Table 36-10-0434-01) ---
    val = _download_and_extract_latest(
        STATCAN_TABLES["gdp"],
        member_filter="Gross domestic product at market prices",
        member_field="North American Industry Classification System (NAICS)",
    )
    if val is not None:
        results["gdp_monthly"] = val

    # --- CPI (Table 18-10-0004-01) ---
    val = _download_and_extract_latest(
        STATCAN_TABLES["cpi"],
        member_filter="All-items",
        member_field="Products and product groups",
    )
    if val is not None:
        results["cpi_all_items"] = val

    # --- Unemployment (Table 14-10-0287-01) ---
    val = _download_and_extract_latest(
        STATCAN_TABLES["unemployment"],
        member_filter="Unemployment rate",
        member_field="Labour force characteristics",
    )
    if val is not None:
        results["unemployment_rate"] = val

    emp_val = _download_and_extract_latest(
        STATCAN_TABLES["unemployment"],
        member_filter="Employment",
        member_field="Labour force characteristics",
    )
    if emp_val is not None:
        results["employment"] = emp_val

    # --- Trade (Table 12-10-0011-01) ---
    exp_val = _download_and_extract_latest(
        STATCAN_TABLES["trade"],
        member_filter="export",
        member_field="Trade",
    )
    if exp_val is not None:
        results["exports_total"] = exp_val

    imp_val = _download_and_extract_latest(
        STATCAN_TABLES["trade"],
        member_filter="import",
        member_field="Trade",
    )
    if imp_val is not None:
        results["imports_total"] = imp_val

    # --- Retail (Table 20-10-0008-01) ---
    val = _download_and_extract_latest(
        STATCAN_TABLES["retail"],
        member_filter="Retail trade",
        member_field="North American Industry Classification System (NAICS)",
    )
    if val is not None:
        results["retail_sales"] = val

    # --- Interest Rates (Table 10-10-0122-01) ---
    val = _download_and_extract_latest(
        STATCAN_TABLES["interest_rates"],
        member_filter="Bank rate",
        member_field="Financial market statistics",
    )
    if val is not None:
        results["interest_rate_target"] = val

    return results if results else None


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached data values to Prometheus gauges."""
    if "gdp_monthly" in data:
        gdp_gauge.labels(province="CA", frequency="monthly").set(data["gdp_monthly"])

    if "cpi_all_items" in data:
        cpi_gauge.labels(province="CA", basket="All-items").set(data["cpi_all_items"])

    if "unemployment_rate" in data:
        unemployment_gauge.labels(province="CA").set(data["unemployment_rate"])

    if "employment" in data:
        employment_gauge.labels(province="CA", industry="Total").set(data["employment"])

    if "exports_total" in data:
        exports_gauge.labels(province="CA").set(data["exports_total"])

    if "imports_total" in data:
        imports_gauge.labels(province="CA").set(data["imports_total"])

    if "exports_total" in data and "imports_total" in data:
        trade_balance_gauge.labels(province="CA").set(
            data["exports_total"] - data["imports_total"]
        )

    if "interest_rate_target" in data:
        interest_rate_gauge.labels(type="overnight").set(data["interest_rate_target"])

    if "retail_sales" in data:
        retail_sales_gauge.labels(province="CA").set(data["retail_sales"])
