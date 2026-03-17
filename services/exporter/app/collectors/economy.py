"""Economy collector - fetches Canadian economic indicators from Statistics Canada."""

import asyncio
import logging
import time
from functools import partial
from typing import Any, Dict, Optional

from prometheus_client import Gauge

from app.cache import RedisCache
from app.config import settings

logger = logging.getLogger(__name__)

# Prometheus Gauges for economic indicators
gdp_gauge = Gauge(
    "cdo_economy_gdp",
    "Canada GDP at basic prices, seasonally adjusted (millions CAD)",
    ["frequency"],
)
cpi_gauge = Gauge(
    "cdo_economy_cpi",
    "Consumer Price Index, all items (2002=100)",
    ["geo"],
)
unemployment_gauge = Gauge(
    "cdo_economy_unemployment_rate",
    "Unemployment rate, seasonally adjusted (%)",
    ["geo"],
)
employment_gauge = Gauge(
    "cdo_economy_employment",
    "Employment count, seasonally adjusted (thousands)",
    ["geo"],
)
exports_gauge = Gauge(
    "cdo_economy_exports",
    "Total exports of goods (millions CAD)",
)
imports_gauge = Gauge(
    "cdo_economy_imports",
    "Total imports of goods (millions CAD)",
)
trade_balance_gauge = Gauge(
    "cdo_economy_trade_balance",
    "Trade balance (exports - imports, millions CAD)",
)
interest_rate_gauge = Gauge(
    "cdo_economy_interest_rate",
    "Bank of Canada policy interest rate (%)",
    ["rate_type"],
)
retail_sales_gauge = Gauge(
    "cdo_economy_retail_sales",
    "Retail trade sales (millions CAD, seasonally adjusted)",
    ["geo"],
)
last_update_gauge = Gauge(
    "cdo_economy_last_update",
    "Timestamp of last successful economy data update (unix epoch)",
)

# StatCan vector IDs for each economic indicator
# Reference: Statistics Canada Table/Vector lookup
VECTORS: Dict[str, str] = {
    "gdp_monthly": "v65201210",           # GDP at basic prices, monthly
    "cpi_all_items": "v41690973",          # CPI, all items, Canada
    "unemployment_rate": "v2062815",       # Unemployment rate, Canada, SA
    "employment": "v2062811",             # Employment, Canada, SA
    "exports_total": "v1001829628",       # Total exports
    "imports_total": "v1001829629",       # Total imports
    "interest_rate_target": "v39079",     # Bank rate / target overnight rate
    "retail_sales": "v52367797",          # Retail trade, Canada, SA
}


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


def _fetch_statcan() -> Optional[Dict[str, Any]]:
    """Synchronous fetch from Statistics Canada using stats_can library.

    Runs in a thread executor to avoid blocking the event loop.
    Returns a dict of indicator_name -> latest_value.
    """
    try:
        import stats_can

        vector_ids = list(VECTORS.values())
        vector_names = list(VECTORS.keys())

        # Fetch latest values for all vectors at once
        df = stats_can.vectors_to_df(vector_ids, periods=1)

        results = {}
        for name, vec_id in VECTORS.items():
            col_match = [c for c in df.columns if vec_id in str(c)]
            if col_match:
                val = df[col_match[0]].dropna().iloc[-1] if not df[col_match[0]].dropna().empty else None
                if val is not None:
                    results[name] = float(val)
                    logger.debug("StatCan %s (%s) = %s", name, vec_id, val)

        return results if results else None

    except ImportError:
        logger.error("stats_can library not available")
        return None
    except Exception:
        logger.exception("StatCan API call failed")
        return None


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached data values to Prometheus gauges."""
    if "gdp_monthly" in data:
        gdp_gauge.labels(frequency="monthly").set(data["gdp_monthly"])

    if "cpi_all_items" in data:
        cpi_gauge.labels(geo="Canada").set(data["cpi_all_items"])

    if "unemployment_rate" in data:
        unemployment_gauge.labels(geo="Canada").set(data["unemployment_rate"])

    if "employment" in data:
        employment_gauge.labels(geo="Canada").set(data["employment"])

    if "exports_total" in data:
        exports_gauge.set(data["exports_total"])

    if "imports_total" in data:
        imports_gauge.set(data["imports_total"])

    if "exports_total" in data and "imports_total" in data:
        trade_balance_gauge.set(data["exports_total"] - data["imports_total"])

    if "interest_rate_target" in data:
        interest_rate_gauge.labels(rate_type="target").set(data["interest_rate_target"])

    if "retail_sales" in data:
        retail_sales_gauge.labels(geo="Canada").set(data["retail_sales"])
