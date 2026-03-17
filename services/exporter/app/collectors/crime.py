"""Crime collector - fetches Canadian crime statistics from Statistics Canada."""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from prometheus_client import Gauge

from app.cache import RedisCache

logger = logging.getLogger(__name__)

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

# ---------------------------------------------------------------------------
# StatCan Table 35-10-0026-01: Crime Severity Index and crime rate
# Provincial CSI vectors (total Crime Severity Index)
PROVINCIAL_CSI_VECTORS: Dict[str, Dict[str, str]] = {
    "CA": {"total": "v107388556", "violent": "v107388601", "nonviolent": "v107388646", "crime_rate": "v107388691"},
    "NL": {"total": "v107388557", "violent": "v107388602", "nonviolent": "v107388647", "crime_rate": "v107388692"},
    "PE": {"total": "v107388558", "violent": "v107388603", "nonviolent": "v107388648", "crime_rate": "v107388693"},
    "NS": {"total": "v107388559", "violent": "v107388604", "nonviolent": "v107388649", "crime_rate": "v107388694"},
    "NB": {"total": "v107388560", "violent": "v107388605", "nonviolent": "v107388650", "crime_rate": "v107388695"},
    "QC": {"total": "v107388561", "violent": "v107388606", "nonviolent": "v107388651", "crime_rate": "v107388696"},
    "ON": {"total": "v107388562", "violent": "v107388607", "nonviolent": "v107388652", "crime_rate": "v107388697"},
    "MB": {"total": "v107388563", "violent": "v107388608", "nonviolent": "v107388653", "crime_rate": "v107388698"},
    "SK": {"total": "v107388564", "violent": "v107388609", "nonviolent": "v107388654", "crime_rate": "v107388699"},
    "AB": {"total": "v107388565", "violent": "v107388610", "nonviolent": "v107388655", "crime_rate": "v107388700"},
    "BC": {"total": "v107388566", "violent": "v107388611", "nonviolent": "v107388656", "crime_rate": "v107388701"},
    "YT": {"total": "v107388567", "violent": "v107388612", "nonviolent": "v107388657", "crime_rate": "v107388702"},
    "NT": {"total": "v107388568", "violent": "v107388613", "nonviolent": "v107388658", "crime_rate": "v107388703"},
    "NU": {"total": "v107388569", "violent": "v107388614", "nonviolent": "v107388659", "crime_rate": "v107388704"},
}

# Table 35-10-0177-01: Incident-based crime stats by offence type
OFFENCE_VECTORS: Dict[str, str] = {
    "total_violations": "v107389050",
    "homicide": "v107389051",
    "assault": "v107389055",
    "robbery": "v107389060",
    "breaking_entering": "v107389070",
    "theft_under_5000": "v107389075",
    "theft_over_5000": "v107389074",
    "motor_vehicle_theft": "v107389076",
    "fraud": "v107389078",
    "mischief": "v107389080",
    "drug_offences": "v107389090",
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
    """Synchronous fetch from Statistics Canada using stats_can library.

    Runs in a thread executor to avoid blocking the event loop.
    Returns a dict of structured crime data.
    """
    try:
        import stats_can

        results: Dict[str, Any] = {}

        # Collect all vector IDs for batch request
        all_vectors = []
        vector_map = {}  # vector_id -> (category, province, subkey)

        # Provincial CSI vectors
        for prov, types in PROVINCIAL_CSI_VECTORS.items():
            for csi_type, vec in types.items():
                all_vectors.append(vec)
                if csi_type == "crime_rate":
                    vector_map[vec] = ("crime_rate", prov, None)
                else:
                    vector_map[vec] = ("csi", prov, csi_type)

        # Offence type vectors (Canada-level)
        for offence, vec in OFFENCE_VECTORS.items():
            all_vectors.append(vec)
            vector_map[vec] = ("offence", "CA", offence)

        # Deduplicate vectors (some may overlap)
        all_vectors = list(set(all_vectors))

        # Fetch all vectors in one batch
        df = stats_can.vectors_to_df(all_vectors, periods=1)

        for vec_id, (category, prov, subkey) in vector_map.items():
            col_match = [c for c in df.columns if vec_id in str(c)]
            if col_match:
                series = df[col_match[0]].dropna()
                if not series.empty:
                    val = float(series.iloc[-1])
                    if subkey:
                        key = f"{category}:{prov}:{subkey}"
                    else:
                        key = f"{category}:{prov}"
                    results[key] = val
                    logger.debug("StatCan crime %s = %s", key, val)

        return results if results else None

    except ImportError:
        logger.error("stats_can library not available")
        return None
    except Exception:
        logger.exception("StatCan crime API call failed")
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

    # Offence-type incidents (Canada level)
    for offence in OFFENCE_VECTORS:
        key = f"offence:CA:{offence}"
        if key in data:
            crime_incidents_gauge.labels(
                province="CA", offence_type=offence
            ).set(data[key])
