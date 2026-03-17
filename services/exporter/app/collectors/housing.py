"""Housing collector - fetches Canadian housing market data from Statistics Canada."""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from prometheus_client import Gauge

from app.cache import RedisCache
from app.config import settings

logger = logging.getLogger(__name__)

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

# ---------------------------------------------------------------------------
# StatCan table and vector mappings
# ---------------------------------------------------------------------------
# Table 18-10-0205-01: New Housing Price Index
# Vectors for NHPI total (house and land) by CMA
NHPI_VECTORS: Dict[str, str] = {
    "Toronto": "v111093680",
    "Vancouver": "v111093686",
    "Montreal": "v111093662",
    "Calgary": "v111093656",
    "Edmonton": "v111093658",
    "Ottawa-Gatineau": "v111093670",
    "Winnipeg": "v111093690",
    "Halifax": "v111093660",
    "Victoria": "v111093688",
    "Hamilton": "v111093692",
}

# Table 34-10-0135-01: Housing starts by type
STARTS_VECTORS: Dict[str, Dict[str, str]] = {
    "Toronto": {"all": "v1057028026", "single": "v1057028027", "multi": "v1057028028"},
    "Vancouver": {"all": "v1057028059", "single": "v1057028060", "multi": "v1057028061"},
    "Montreal": {"all": "v1057028014", "single": "v1057028015", "multi": "v1057028016"},
    "Calgary": {"all": "v1057027990", "single": "v1057027991", "multi": "v1057027992"},
    "Edmonton": {"all": "v1057027993", "single": "v1057027994", "multi": "v1057027995"},
    "Ottawa-Gatineau": {"all": "v1057028038", "single": "v1057028039", "multi": "v1057028040"},
    "Winnipeg": {"all": "v1057028062", "single": "v1057028063", "multi": "v1057028064"},
    "Halifax": {"all": "v1057028005", "single": "v1057028006", "multi": "v1057028007"},
    "Victoria": {"all": "v1057028056", "single": "v1057028057", "multi": "v1057028058"},
    "Hamilton": {"all": "v1057028008", "single": "v1057028009", "multi": "v1057028010"},
}

# Table 34-10-0127-01: Vacancy rates
VACANCY_VECTORS: Dict[str, str] = {
    "Toronto": "v1057026776",
    "Vancouver": "v1057026812",
    "Montreal": "v1057026764",
    "Calgary": "v1057026740",
    "Edmonton": "v1057026744",
    "Ottawa-Gatineau": "v1057026788",
    "Winnipeg": "v1057026816",
    "Halifax": "v1057026756",
    "Victoria": "v1057026808",
    "Hamilton": "v1057026760",
}

# Table 34-10-0133-01: Average rents by bedroom type
RENT_VECTORS: Dict[str, Dict[str, str]] = {
    "Toronto": {"total": "v1057027602", "1br": "v1057027603", "2br": "v1057027604"},
    "Vancouver": {"total": "v1057027638", "1br": "v1057027639", "2br": "v1057027640"},
    "Montreal": {"total": "v1057027590", "1br": "v1057027591", "2br": "v1057027592"},
    "Calgary": {"total": "v1057027566", "1br": "v1057027567", "2br": "v1057027568"},
    "Edmonton": {"total": "v1057027570", "1br": "v1057027571", "2br": "v1057027572"},
    "Ottawa-Gatineau": {"total": "v1057027614", "1br": "v1057027615", "2br": "v1057027616"},
    "Winnipeg": {"total": "v1057027642", "1br": "v1057027643", "2br": "v1057027644"},
    "Halifax": {"total": "v1057027582", "1br": "v1057027583", "2br": "v1057027584"},
    "Victoria": {"total": "v1057027634", "1br": "v1057027635", "2br": "v1057027636"},
    "Hamilton": {"total": "v1057027586", "1br": "v1057027587", "2br": "v1057027588"},
}

# Table 11-10-0222-01: Median after-tax income by CMA (for price-to-income)
INCOME_VECTORS: Dict[str, str] = {
    "Toronto": "v1057029040",
    "Vancouver": "v1057029046",
    "Montreal": "v1057029022",
    "Calgary": "v1057029016",
    "Edmonton": "v1057029018",
    "Ottawa-Gatineau": "v1057029030",
    "Winnipeg": "v1057029050",
    "Halifax": "v1057029020",
    "Victoria": "v1057029048",
    "Hamilton": "v1057029024",
}

# Table 18-10-0205-01: Average house prices (composite)
AVG_PRICE_VECTORS: Dict[str, str] = {
    "Toronto": "v111093681",
    "Vancouver": "v111093687",
    "Montreal": "v111093663",
    "Calgary": "v111093657",
    "Edmonton": "v111093659",
    "Ottawa-Gatineau": "v111093671",
    "Winnipeg": "v111093691",
    "Halifax": "v111093661",
    "Victoria": "v111093689",
    "Hamilton": "v111093693",
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


def _fetch_statcan() -> Optional[Dict[str, Any]]:
    """Synchronous fetch from Statistics Canada using stats_can library.

    Runs in a thread executor to avoid blocking the event loop.
    Returns a dict of structured housing data.
    """
    try:
        import stats_can

        results: Dict[str, Any] = {}

        # Collect all vector IDs for a single batch request
        all_vectors = []
        vector_map = {}  # vector_id -> (category, cma, subkey)

        # NHPI vectors
        for cma, vec in NHPI_VECTORS.items():
            all_vectors.append(vec)
            vector_map[vec] = ("nhpi", cma, None)

        # Housing starts vectors
        for cma, types in STARTS_VECTORS.items():
            for stype, vec in types.items():
                all_vectors.append(vec)
                vector_map[vec] = ("starts", cma, stype)

        # Vacancy rate vectors
        for cma, vec in VACANCY_VECTORS.items():
            all_vectors.append(vec)
            vector_map[vec] = ("vacancy", cma, None)

        # Rent vectors
        for cma, bedrooms in RENT_VECTORS.items():
            for btype, vec in bedrooms.items():
                all_vectors.append(vec)
                vector_map[vec] = ("rent", cma, btype)

        # Average price vectors
        for cma, vec in AVG_PRICE_VECTORS.items():
            all_vectors.append(vec)
            vector_map[vec] = ("avg_price", cma, None)

        # Fetch all vectors in one batch
        df = stats_can.vectors_to_df(all_vectors, periods=1)

        for vec_id, (category, cma, subkey) in vector_map.items():
            col_match = [c for c in df.columns if vec_id in str(c)]
            if col_match:
                series = df[col_match[0]].dropna()
                if not series.empty:
                    val = float(series.iloc[-1])
                    if subkey:
                        key = f"{category}:{cma}:{subkey}"
                    else:
                        key = f"{category}:{cma}"
                    results[key] = val
                    logger.debug("StatCan housing %s = %s", key, val)

        return results if results else None

    except ImportError:
        logger.error("stats_can library not available")
        return None
    except Exception:
        logger.exception("StatCan housing API call failed")
        return None


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
