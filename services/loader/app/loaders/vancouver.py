"""Vancouver Open Data loader - crime incidents via Explore API v2.1."""

import logging
from datetime import date

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/"
CRIME_DATASET = "crimedata"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 60.0
MAX_PAGES = 500  # Safety limit: 50,000 records max


def _fetch_crime_page(offset: int = 0, limit: int = PAGE_SIZE) -> dict | None:
    """Fetch a page of crime records from Vancouver Open Data API."""
    url = f"{BASE_URL}{CRIME_DATASET}/records"
    params = {
        "limit": limit,
        "offset": offset,
        "order_by": "year DESC, month DESC",
    }

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()

    except httpx.HTTPStatusError as exc:
        logger.error(
            "Vancouver API HTTP %d at offset %d: %s",
            exc.response.status_code,
            offset,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("Vancouver API request failed at offset %d: %s", offset, exc)

    return None


def _fetch_all_crime_records() -> list[dict]:
    """Paginate through all Vancouver crime records."""
    all_records: list[dict] = []
    offset = 0

    for page_num in range(MAX_PAGES):
        logger.info(
            "Fetching Vancouver crime records page %d (offset %d)...",
            page_num + 1,
            offset,
        )

        data = _fetch_crime_page(offset=offset, limit=PAGE_SIZE)
        if not data:
            logger.warning("Failed to fetch page at offset %d, stopping", offset)
            break

        records = data.get("results", [])
        if not records:
            logger.info("No more records at offset %d, pagination complete", offset)
            break

        all_records.extend(records)
        total_count = data.get("total_count", 0)

        logger.info(
            "Fetched %d records (total so far: %d / %d)",
            len(records),
            len(all_records),
            total_count,
        )

        offset += len(records)

        if len(all_records) >= total_count:
            break

    logger.info("Total Vancouver crime records fetched: %d", len(all_records))
    return all_records


def _parse_int(val) -> int | None:
    """Safely parse an integer value."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_float(val) -> float | None:
    """Safely parse a float value."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _build_occurred_date(year: int | None, month: int | None) -> date | None:
    """Build an approximate occurred_date from year and month."""
    if not year:
        return None
    month = month or 1
    try:
        return date(year, month, 1)
    except (ValueError, TypeError):
        return None


def load_crime_incidents() -> int:
    """Fetch Vancouver crime data and insert into muni.crime_incidents.

    Returns:
        Number of rows inserted.
    """
    logger.info("Loading Vancouver crime incidents...")
    records = _fetch_all_crime_records()

    if not records:
        logger.warning("No Vancouver crime records to load")
        return 0

    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row_index, record in enumerate(records):
                crime_type = (record.get("type") or "").strip()
                if not crime_type:
                    continue

                year = _parse_int(record.get("year"))
                month = _parse_int(record.get("month"))
                neighbourhood = (
                    record.get("neighbourhood") or ""
                ).strip() or None

                occurred_date = _build_occurred_date(year, month)

                # Geometry from geo_point_2d field
                geo_point = record.get("geo_point_2d") or {}
                lat = _parse_float(geo_point.get("lat"))
                lon = _parse_float(geo_point.get("lon"))

                # Fallback to top-level lat/lon
                if lat is None:
                    lat = _parse_float(record.get("latitude"))
                if lon is None:
                    lon = _parse_float(record.get("longitude"))

                geom_expr = "NULL"
                geom_params: list = []
                if lat is not None and lon is not None:
                    geom_expr = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"
                    geom_params = [lon, lat]

                # Build a unique incident_id from available fields
                hundred_block = (record.get("hundred_block") or "").strip()
                incident_id = f"VAN-{year or 0}-{month or 0}-{crime_type}-{hundred_block}-{row_index}"

                cur.execute("SAVEPOINT row_sp")
                try:
                    cur.execute(
                        f"""
                        INSERT INTO muni.crime_incidents
                            (incident_id, city, occurred_date, crime_type,
                             neighbourhood, latitude, longitude, geom,
                             premises_type, data_source)
                        VALUES
                            (%s, 'Vancouver', %s, %s, %s, %s, %s,
                             {geom_expr}, %s, 'vancouver_open_data')
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            incident_id,
                            occurred_date,
                            crime_type,
                            neighbourhood,
                            lat,
                            lon,
                            *geom_params,
                            None,  # premises_type not in Vancouver data
                        ],
                    )
                    cur.execute("RELEASE SAVEPOINT row_sp")
                    inserted += 1
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    logger.warning(
                        "Skipping Vancouver crime record: %s", exc
                    )
                    continue

    logger.info("Vancouver crime incidents: inserted %d rows", inserted)
    return inserted
