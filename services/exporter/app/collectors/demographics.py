"""Demographics collector - fetches Canadian population data from Statistics Canada."""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from prometheus_client import Gauge

from app.cache import RedisCache
from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# StatCan Table / Vector references
# ---------------------------------------------------------------------------
# Table 17-10-0005-01: Population estimates, quarterly
# Vectors map province -> population estimate vector ID
PROVINCE_VECTORS: Dict[str, str] = {
    "CA": "v1",           # Canada total
    "ON": "v2",           # Ontario
    "QC": "v3",           # Quebec
    "BC": "v4",           # British Columbia
    "AB": "v5",           # Alberta
    "MB": "v6",           # Manitoba
    "SK": "v7",           # Saskatchewan
    "NS": "v8",           # Nova Scotia
    "NB": "v9",           # New Brunswick
    "NL": "v10",          # Newfoundland and Labrador
    "PE": "v11",          # Prince Edward Island
}

# Growth, median age, births, deaths, migration vectors
# Table 17-10-0005-01 and related tables
DEMOGRAPHIC_VECTORS: Dict[str, str] = {
    "growth_rate_CA": "v36397",        # Population growth rate, Canada
    "median_age_CA": "v42804054",      # Median age, Canada
    "births_CA": "v22380553",          # Births, Canada
    "deaths_CA": "v22380559",          # Deaths, Canada
    "net_migration_CA": "v22380565",   # Net migration, Canada
}

# Province-level demographic indicator vectors
PROVINCE_GROWTH_VECTORS: Dict[str, str] = {
    "CA": "v36397",
    "ON": "v36398",
    "QC": "v36399",
    "BC": "v36400",
    "AB": "v36401",
    "MB": "v36402",
    "SK": "v36403",
    "NS": "v36404",
    "NB": "v36405",
    "NL": "v36406",
    "PE": "v36407",
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
        data = await loop.run_in_executor(None, _fetch_statcan)

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


def _fetch_statcan() -> Optional[Dict[str, Any]]:
    """Synchronous fetch from Statistics Canada using stats_can library.

    Runs in a thread executor to avoid blocking the event loop.
    Returns structured population and demographic data.
    """
    try:
        import stats_can

        results: Dict[str, Any] = {}

        # Fetch population by province
        pop_vectors = list(PROVINCE_VECTORS.values())
        pop_names = list(PROVINCE_VECTORS.keys())

        df = stats_can.vectors_to_df(pop_vectors, periods=1)
        populations = {}
        for province, vec_id in PROVINCE_VECTORS.items():
            col_match = [c for c in df.columns if vec_id in str(c)]
            if col_match:
                series = df[col_match[0]].dropna()
                if not series.empty:
                    populations[province] = float(series.iloc[-1])
                    logger.debug("Population %s = %s", province, populations[province])

        if populations:
            results["populations"] = populations

        # Fetch growth rates by province
        growth_vectors = list(PROVINCE_GROWTH_VECTORS.values())
        try:
            gdf = stats_can.vectors_to_df(growth_vectors, periods=1)
            growth_rates = {}
            for province, vec_id in PROVINCE_GROWTH_VECTORS.items():
                col_match = [c for c in gdf.columns if vec_id in str(c)]
                if col_match:
                    series = gdf[col_match[0]].dropna()
                    if not series.empty:
                        growth_rates[province] = float(series.iloc[-1])
            if growth_rates:
                results["growth_rates"] = growth_rates
        except Exception:
            logger.warning("Failed to fetch growth rate vectors")

        # Fetch demographic indicators (national level)
        demo_vectors = list(DEMOGRAPHIC_VECTORS.values())
        demo_names = list(DEMOGRAPHIC_VECTORS.keys())

        try:
            ddf = stats_can.vectors_to_df(demo_vectors, periods=1)
            for name, vec_id in DEMOGRAPHIC_VECTORS.items():
                col_match = [c for c in ddf.columns if vec_id in str(c)]
                if col_match:
                    series = ddf[col_match[0]].dropna()
                    if not series.empty:
                        results[name] = float(series.iloc[-1])
                        logger.debug("Demographic %s = %s", name, results[name])
        except Exception:
            logger.warning("Failed to fetch demographic indicator vectors")

        return results if results else None

    except ImportError:
        logger.error("stats_can library not available")
        return None
    except Exception:
        logger.exception("StatCan API call failed for demographics")
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
