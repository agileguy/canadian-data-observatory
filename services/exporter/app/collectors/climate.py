"""
Climate collector for Environment and Climate Change Canada (ECCC) data.

Fetches current weather conditions from the ECCC OGC API (api.weather.gc.ca)
and exposes them as Prometheus gauges. The old WFS endpoint at geo.weather.gc.ca
does not work; the OGC API is the working replacement.
"""

import logging
import time
from typing import Any, Optional

import httpx
from prometheus_client import Gauge

from app.cache import RedisCache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ECCC OGC API endpoint
# ---------------------------------------------------------------------------
OGC_API_BASE = "https://api.weather.gc.ca/collections/climate-hourly/items"

# Redis cache key (no cdo: prefix -- RedisCache adds it)
CACHE_KEY = "climate:ogc_response"
CACHE_TTL = 900  # 15 minutes

# ---------------------------------------------------------------------------
# Major stations to track — using station names that work with the OGC API
# ---------------------------------------------------------------------------
STATIONS: list[dict[str, str]] = [
    {"name": "TORONTO INTL A", "stn_id": "51459", "city": "Toronto", "province": "ON"},
    {"name": "VANCOUVER INTL A", "stn_id": "51442", "city": "Vancouver", "province": "BC"},
    {"name": "MONTREAL/TRUDEAU INTL A", "stn_id": "51157", "city": "Montreal", "province": "QC"},
    {"name": "CALGARY INTL A", "stn_id": "50430", "city": "Calgary", "province": "AB"},
    {"name": "EDMONTON INTL A", "stn_id": "50149", "city": "Edmonton", "province": "AB"},
    {"name": "OTTAWA CDA", "stn_id": "49568", "city": "Ottawa", "province": "ON"},
    {"name": "WINNIPEG INTL A", "stn_id": "51097", "city": "Winnipeg", "province": "MB"},
    {"name": "HALIFAX STANFIELD INTL A", "stn_id": "50620", "city": "Halifax", "province": "NS"},
]

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
dew_point_gauge = Gauge(
    "cdo_climate_dew_point_celsius",
    "Current dew point temperature in Celsius",
    LABEL_KEYS,
)
last_update_gauge = Gauge(
    "cdo_climate_last_update_timestamp",
    "Unix timestamp of last successful climate data update",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
    """Fetch current conditions from ECCC OGC API and update Prometheus gauges.

    Uses RedisCache for caching the parsed station data to avoid hammering
    the API. Falls back to a direct fetch if cache is empty.
    """
    # Try cache first
    try:
        cached = await cache.get(CACHE_KEY)
        if cached:
            logger.debug("Climate data loaded from Redis cache")
            _apply_station_data(cached)
            last_update_gauge.set(time.time())
            return
    except Exception as exc:
        logger.warning("Redis cache read failed: %s", exc)

    # Fetch from OGC API — one request per station (small responses)
    logger.info("Fetching climate data from ECCC OGC API for %d stations", len(STATIONS))
    station_data = await _fetch_all_stations()

    if not station_data:
        logger.error("Failed to fetch any climate data from ECCC OGC API")
        return

    # Apply to gauges
    _apply_station_data(station_data)

    # Store in cache
    try:
        await cache.set(CACHE_KEY, station_data, ttl=CACHE_TTL)
        logger.debug("Climate data cached in Redis (TTL=%ds)", CACHE_TTL)
    except Exception as exc:
        logger.warning("Redis cache write failed: %s", exc)

    last_update_gauge.set(time.time())
    logger.info(
        "Climate update complete: %d stations fetched",
        len(station_data),
    )


async def _fetch_all_stations() -> list[dict[str, Any]]:
    """Fetch the latest observation for each station from the OGC API."""
    results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for station in STATIONS:
            try:
                data = await _fetch_station(client, station)
                if data:
                    results.append(data)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch climate data for %s: %s",
                    station["name"],
                    exc,
                )

    return results


async def _fetch_station(
    client: httpx.AsyncClient,
    station: dict[str, str],
) -> Optional[dict[str, Any]]:
    """Fetch the latest hourly observation for a single station.

    Uses the OGC API endpoint:
    https://api.weather.gc.ca/collections/climate-hourly/items?f=json&limit=1&STATION_NAME={name}&sortby=-LOCAL_DATE
    """
    params = {
        "f": "json",
        "limit": 1,
        "STATION_NAME": station["name"],
        "sortby": "-LOCAL_DATE",
    }

    resp = await client.get(OGC_API_BASE, params=params)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    if not features:
        logger.debug("No features returned for station %s", station["name"])
        return None

    props = features[0].get("properties", {})
    geometry = features[0].get("geometry", {})
    coords = geometry.get("coordinates", [])

    return {
        "station_name": station["name"],
        "stn_id": station["stn_id"],
        "city": station["city"],
        "province": station["province"],
        "temperature": props.get("TEMP"),
        "humidity": props.get("RELATIVE_HUMIDITY"),
        "wind_speed": props.get("WIND_SPEED"),
        "pressure": props.get("STATION_PRESSURE"),
        "dew_point": props.get("DEW_POINT_TEMP"),
        "longitude": coords[0] if len(coords) > 0 else None,
        "latitude": coords[1] if len(coords) > 1 else None,
    }


def _apply_station_data(station_data: list[dict[str, Any]]) -> None:
    """Apply station observation data to Prometheus gauges."""
    for stn in station_data:
        labels = {
            "station": stn.get("station_name", ""),
            "city": stn.get("city", ""),
            "province": stn.get("province", ""),
        }

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

        dew = _safe_float(stn.get("dew_point"))
        if dew is not None:
            dew_point_gauge.labels(**labels).set(dew)
