"""
Parser for Environment and Climate Change Canada (ECCC) GeoMet WFS responses.

Handles GeoJSON feature collections from the CURRENT_CONDITIONS layer.
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def parse_wfs_response(geojson_data: dict) -> list[dict]:
    """Parse a WFS GeoJSON response into a list of station data dicts.

    Args:
        geojson_data: Raw GeoJSON FeatureCollection from ECCC GeoMet WFS.

    Returns:
        List of dicts, each containing parsed station data with keys:
            station_id, station_name, province, latitude, longitude,
            temperature, dewpoint, wind_speed, wind_direction,
            pressure, humidity, condition, precipitation,
            daily_high, daily_low, timestamp
    """
    if not isinstance(geojson_data, dict):
        logger.warning("Expected dict for GeoJSON, got %s", type(geojson_data).__name__)
        return []

    features = geojson_data.get("features", [])
    if not features:
        logger.warning("No features found in WFS response")
        return []

    stations = []
    for feature in features:
        try:
            parsed = extract_station_data(feature)
            if parsed is not None:
                stations.append(parsed)
        except Exception as exc:
            logger.debug("Skipping malformed feature: %s", exc)

    logger.debug("Parsed %d stations from %d features", len(stations), len(features))
    return stations


def extract_station_data(feature: dict) -> Optional[dict]:
    """Extract structured station data from a single GeoJSON feature.

    Args:
        feature: A GeoJSON Feature dict with 'properties' and optionally 'geometry'.

    Returns:
        Dict with numeric fields converted to float where possible,
        or None if the feature has no usable properties.
    """
    props = feature.get("properties")
    if not props:
        return None

    # Extract coordinates from geometry if present
    geometry = feature.get("geometry")
    lat, lon = None, None
    if geometry and geometry.get("coordinates"):
        coords = geometry["coordinates"]
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon = _to_float(coords[0])
            lat = _to_float(coords[1])

    # The ECCC WFS uses varying property names. We try common variants.
    station_id = (
        props.get("station_id")
        or props.get("STATION_ID")
        or props.get("climate_id")
        or props.get("CLIMATE_ID")
    )

    station_name = (
        props.get("station_name")
        or props.get("STATION_NAME")
        or props.get("name")
        or props.get("NAME")
    )

    province = (
        props.get("province")
        or props.get("PROVINCE")
        or props.get("prov_terr")
        or props.get("PROV_TERR")
    )

    return {
        "station_id": str(station_id) if station_id is not None else None,
        "station_name": station_name,
        "province": province,
        "latitude": lat,
        "longitude": lon,
        "temperature": _to_float(
            props.get("temperature") or props.get("TEMPERATURE") or props.get("temp")
        ),
        "dewpoint": _to_float(
            props.get("dewpoint") or props.get("DEWPOINT") or props.get("dew_point")
        ),
        "wind_speed": _to_float(
            props.get("wind_speed") or props.get("WIND_SPEED") or props.get("wspd")
        ),
        "wind_direction": _to_float(
            props.get("wind_direction") or props.get("WIND_DIRECTION") or props.get("wdir")
        ),
        "pressure": _to_float(
            props.get("pressure") or props.get("PRESSURE") or props.get("mslp")
        ),
        "humidity": _to_float(
            props.get("humidity")
            or props.get("HUMIDITY")
            or props.get("relative_humidity")
            or props.get("RELATIVE_HUMIDITY")
        ),
        "condition": (
            props.get("condition") or props.get("CONDITION") or props.get("wxcond")
        ),
        "precipitation": _to_float(
            props.get("precipitation") or props.get("PRECIPITATION") or props.get("precip")
        ),
        "daily_high": _to_float(
            props.get("daily_high") or props.get("DAILY_HIGH") or props.get("temp_max")
        ),
        "daily_low": _to_float(
            props.get("daily_low") or props.get("DAILY_LOW") or props.get("temp_min")
        ),
        "timestamp": (
            props.get("timestamp")
            or props.get("TIMESTAMP")
            or props.get("observation_datetime")
            or props.get("OBSERVATION_DATETIME")
        ),
    }


def _to_float(value: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure or None input."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
