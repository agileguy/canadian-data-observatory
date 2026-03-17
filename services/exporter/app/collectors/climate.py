"""
Climate collector for Environment and Climate Change Canada (ECCC) data.

Fetches current weather conditions from the ECCC GeoMet WFS API and
exposes them as Prometheus gauges.
"""

import logging
import time
from typing import Any, Optional

import httpx
from prometheus_client import Gauge

from app.cache import RedisCache
from app.parsers.eccc import parse_wfs_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ECCC GeoMet WFS endpoint
# ---------------------------------------------------------------------------
WFS_URL = "https://geo.weather.gc.ca/geomet"
WFS_PARAMS = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typename": "CURRENT_CONDITIONS",
    "outputformat": "application/json",
    "BBOX": "-141,41,-52,84",
    "count": "500",
}

# Redis cache key (no cdo: prefix -- RedisCache adds it)
CACHE_KEY = "climate:wfs_response"
CACHE_TTL = 900  # 15 minutes

# ---------------------------------------------------------------------------
# Major stations to track
# ---------------------------------------------------------------------------
MAJOR_STATIONS: dict[str, dict[str, str]] = {
    "51459": {"city": "Toronto", "province": "ON"},
    "51442": {"city": "Vancouver", "province": "BC"},
    "51157": {"city": "Montreal", "province": "QC"},
    "50430": {"city": "Calgary", "province": "AB"},
    "50149": {"city": "Edmonton", "province": "AB"},
    "49568": {"city": "Ottawa", "province": "ON"},
    "51097": {"city": "Winnipeg", "province": "MB"},
    "50620": {"city": "Halifax", "province": "NS"},
}

# Reverse lookup: city name -> station id (for name-based matching)
CITY_TO_STATION: dict[str, str] = {
    v["city"]: k for k, v in MAJOR_STATIONS.items()
}

# Known station name substrings for fuzzy matching
STATION_NAME_HINTS: dict[str, str] = {
    "TORONTO PEARSON": "51459",
    "TORONTO LESTER": "51459",
    "VANCOUVER INT": "51442",
    "MONTRÉAL TRUDEAU": "51157",
    "MONTREAL TRUDEAU": "51157",
    "CALGARY INT": "50430",
    "EDMONTON INT": "50149",
    "OTTAWA CDA": "49568",
    "OTTAWA MACDONALD": "49568",
    "WINNIPEG INT": "51097",
    "WINNIPEG RICHARD": "51097",
    "HALIFAX STANFIELD": "50620",
}

# ---------------------------------------------------------------------------
# Prometheus gauges
# ---------------------------------------------------------------------------
LABEL_KEYS = ["station", "city", "province"]

temperature_gauge = Gauge(
    "cdo_climate_temperature_celsius",
    "Current temperature in Celsius",
    LABEL_KEYS,
)
humidity_gauge = Gauge(
    "cdo_climate_humidity_percent",
    "Current relative humidity percentage",
    LABEL_KEYS,
)
wind_speed_gauge = Gauge(
    "cdo_climate_wind_speed_kmh",
    "Current wind speed in km/h",
    LABEL_KEYS,
)
pressure_gauge = Gauge(
    "cdo_climate_pressure_kpa",
    "Current atmospheric pressure in kPa",
    LABEL_KEYS,
)
precipitation_gauge = Gauge(
    "cdo_climate_precipitation_mm",
    "Precipitation in mm",
    LABEL_KEYS,
)
daily_high_gauge = Gauge(
    "cdo_climate_temperature_daily_high_celsius",
    "Daily high temperature in Celsius",
    ["city", "province"],
)
daily_low_gauge = Gauge(
    "cdo_climate_temperature_daily_low_celsius",
    "Daily low temperature in Celsius",
    ["city", "province"],
)
last_update_gauge = Gauge(
    "cdo_climate_last_update_timestamp",
    "Unix timestamp of last successful climate data update",
)


# ---------------------------------------------------------------------------
# Station matching helpers
# ---------------------------------------------------------------------------

def _match_station_id(station_id: Optional[str], station_name: Optional[str]) -> Optional[str]:
    """Try to match a station to one of the major tracked stations.

    Returns the canonical station ID if matched, None otherwise.
    """
    # Direct ID match
    if station_id and station_id in MAJOR_STATIONS:
        return station_id

    # Name-based fuzzy match
    if station_name:
        upper_name = station_name.upper()
        for hint, sid in STATION_NAME_HINTS.items():
            if hint in upper_name:
                return sid

    return None


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Core collector
# ---------------------------------------------------------------------------

async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch current conditions from ECCC and update Prometheus gauges.

    Uses RedisCache for caching the raw WFS response to avoid hammering
    the API. Falls back to a direct fetch if cache is empty.
    """
    geojson_data = None

    # Try cache first
    try:
        cached = await cache.get(CACHE_KEY)
        if cached:
            geojson_data = cached
            logger.debug("Climate data loaded from Redis cache")
    except Exception as exc:
        logger.warning("Redis cache read failed: %s", exc)

    # Fetch from API if no cache hit
    if geojson_data is None:
        geojson_data = await _fetch_wfs()
        if geojson_data is None:
            logger.error("Failed to fetch climate data from ECCC")
            return

        # Store in cache
        try:
            await cache.set(CACHE_KEY, geojson_data, ttl=CACHE_TTL)
            logger.debug("Climate data cached in Redis (TTL=%ds)", CACHE_TTL)
        except Exception as exc:
            logger.warning("Redis cache write failed: %s", exc)

    # Parse and update gauges
    stations = parse_wfs_response(geojson_data)
    matched_count = 0

    for stn in stations:
        sid = _match_station_id(stn.get("station_id"), stn.get("station_name"))
        if sid is None:
            continue

        meta = MAJOR_STATIONS[sid]
        city = meta["city"]
        province = meta["province"]
        station_name = stn.get("station_name", sid)
        labels = {"station": station_name, "city": city, "province": province}

        temp = _safe_float(stn.get("temperature"))
        if temp is not None:
            temperature_gauge.labels(**labels).set(temp)

        hum = _safe_float(stn.get("humidity"))
        if hum is not None:
            humidity_gauge.labels(**labels).set(hum)

        wind = _safe_float(stn.get("wind_speed"))
        if wind is not None:
            wind_speed_gauge.labels(**labels).set(wind)

        pres = _safe_float(stn.get("pressure"))
        if pres is not None:
            pressure_gauge.labels(**labels).set(pres)

        precip = _safe_float(stn.get("precipitation"))
        if precip is not None:
            precipitation_gauge.labels(**labels).set(precip)

        daily_high = _safe_float(stn.get("daily_high"))
        if daily_high is not None:
            daily_high_gauge.labels(city=city, province=province).set(daily_high)

        daily_low = _safe_float(stn.get("daily_low"))
        if daily_low is not None:
            daily_low_gauge.labels(city=city, province=province).set(daily_low)

        matched_count += 1

    last_update_gauge.set(time.time())
    logger.info(
        "Climate update complete: %d stations parsed, %d major stations matched",
        len(stations),
        matched_count,
    )


async def _fetch_wfs() -> Optional[dict]:
    """Fetch current conditions GeoJSON from ECCC GeoMet WFS."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(WFS_URL, params=WFS_PARAMS)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        logger.error("ECCC WFS HTTP error %d: %s", exc.response.status_code, exc)
    except httpx.RequestError as exc:
        logger.error("ECCC WFS request failed: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error fetching ECCC data: %s", exc)
    return None
