"""Calgary Open Data loader - crime and building permits via Socrata SODA API."""

import logging
from datetime import date, datetime

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

# Socrata SODA API endpoints
CRIME_URL = "https://data.calgary.ca/resource/78gh-n26t.json"
PERMITS_URL = "https://data.calgary.ca/resource/6933-unr-res.json"

REQUEST_TIMEOUT = 60.0
PAGE_SIZE = 10000
MAX_PAGES = 50  # Safety limit


def _fetch_soda_records(
    base_url: str,
    where_clause: str | None = None,
    limit: int = PAGE_SIZE,
    offset: int = 0,
) -> list[dict] | None:
    """Fetch records from a Socrata SODA API endpoint."""
    params: dict = {"$limit": limit, "$offset": offset, "$order": ":id"}
    if where_clause:
        params["$where"] = where_clause

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(base_url, params=params)
            resp.raise_for_status()
            return resp.json()

    except httpx.HTTPStatusError as exc:
        logger.error(
            "Calgary SODA HTTP %d for %s: %s",
            exc.response.status_code,
            base_url,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("Calgary SODA request failed for %s: %s", base_url, exc)

    return None


def _paginate_soda(
    base_url: str, where_clause: str | None = None
) -> list[dict]:
    """Paginate through all records from a SODA endpoint."""
    all_records: list[dict] = []
    offset = 0

    for page_num in range(MAX_PAGES):
        logger.info(
            "Fetching Calgary SODA page %d (offset %d) from %s",
            page_num + 1,
            offset,
            base_url.split("/")[-1],
        )

        records = _fetch_soda_records(
            base_url, where_clause=where_clause, offset=offset
        )
        if records is None:
            logger.warning("Failed to fetch page at offset %d, stopping", offset)
            break

        if not records:
            logger.info("No more records at offset %d, pagination complete", offset)
            break

        all_records.extend(records)
        offset += len(records)

        # If we got fewer than PAGE_SIZE, we've reached the end
        if len(records) < PAGE_SIZE:
            break

    logger.info("Total records fetched from %s: %d", base_url, len(all_records))
    return all_records


def _parse_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_date(date_str: str | None) -> date | None:
    """Parse a Socrata date string."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


def _build_date_from_parts(year: int | None, month: int | None) -> date | None:
    """Build date from year and month integers."""
    if not year:
        return None
    month = month or 1
    try:
        return date(year, month, 1)
    except (ValueError, TypeError):
        return None


def load_crime_incidents() -> int:
    """Fetch Calgary community crime stats and insert into muni.crime_incidents.

    Returns:
        Number of rows inserted.
    """
    logger.info("Loading Calgary crime incidents...")
    records = _paginate_soda(CRIME_URL, where_clause="year>2020")

    if not records:
        logger.warning("No Calgary crime records to load")
        return 0

    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row_index, record in enumerate(records):
                crime_type = (
                    record.get("category")
                    or record.get("crime_type")
                    or ""
                ).strip()
                if not crime_type:
                    continue

                year = _parse_int(record.get("year"))
                month = _parse_int(record.get("month"))
                community = (
                    record.get("community_name")
                    or record.get("neighbourhood")
                    or ""
                ).strip() or None

                occurred_date = _build_date_from_parts(year, month)

                lat = _parse_float(record.get("latitude"))
                lon = _parse_float(record.get("longitude"))

                # Some Calgary datasets embed geometry differently
                if lat is None and "geocoded_column" in record:
                    geo_col = record.get("geocoded_column") or {}
                    coords = geo_col.get("coordinates", [])
                    if len(coords) >= 2:
                        lon = _parse_float(coords[0])
                        lat = _parse_float(coords[1])

                geom_expr = "NULL"
                geom_params: list = []
                if lat is not None and lon is not None:
                    geom_expr = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"
                    geom_params = [lon, lat]

                incident_id = (
                    f"CGY-{year or 0}-{month or 0}"
                    f"-{crime_type}-{community or 'UNK'}-{row_index}"
                )

                cur.execute("SAVEPOINT row_sp")
                try:
                    cur.execute(
                        f"""
                        INSERT INTO muni.crime_incidents
                            (incident_id, city, occurred_date, crime_type,
                             neighbourhood, latitude, longitude, geom,
                             data_source)
                        VALUES
                            (%s, 'Calgary', %s, %s, %s, %s, %s,
                             {geom_expr}, 'calgary_open_data')
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            incident_id,
                            occurred_date,
                            crime_type,
                            community,
                            lat,
                            lon,
                            *geom_params,
                        ],
                    )
                    cur.execute("RELEASE SAVEPOINT row_sp")
                    inserted += 1
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    logger.warning(
                        "Skipping Calgary crime record: %s", exc
                    )
                    continue

    logger.info("Calgary crime incidents: inserted %d rows", inserted)
    return inserted


def load_building_permits() -> int:
    """Fetch Calgary building permits from SODA and insert into DB.

    Returns:
        Number of rows inserted.
    """
    logger.info("Loading Calgary building permits...")
    records = _paginate_soda(PERMITS_URL)

    if not records:
        logger.warning("No Calgary building permit records to load")
        return 0

    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for record in records:
                permit_number = (
                    record.get("permitnum")
                    or record.get("permit_number")
                    or record.get("permit_num")
                    or ""
                ).strip()
                if not permit_number:
                    continue

                issue_date = _parse_date(
                    record.get("issueddate")
                    or record.get("issued_date")
                    or record.get("issue_date")
                )
                permit_type = (
                    record.get("workclassgroup")
                    or record.get("permit_type")
                    or ""
                ).strip() or None
                work_type = (
                    record.get("workclasstype")
                    or record.get("work_type")
                    or ""
                ).strip() or None
                description = (
                    record.get("description")
                    or record.get("permit_description")
                    or ""
                ).strip() or None
                address = (
                    record.get("originaladdress")
                    or record.get("address")
                    or ""
                ).strip() or None
                project_value = _parse_float(
                    record.get("estprojectcost")
                    or record.get("project_value")
                )
                status = (
                    record.get("statuscurrent")
                    or record.get("status")
                    or ""
                ).strip() or None

                lat = _parse_float(record.get("latitude"))
                lon = _parse_float(record.get("longitude"))

                geom_expr = "NULL"
                geom_params: list = []
                if lat is not None and lon is not None:
                    geom_expr = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"
                    geom_params = [lon, lat]

                cur.execute("SAVEPOINT row_sp")
                try:
                    cur.execute(
                        f"""
                        INSERT INTO muni.building_permits
                            (permit_number, city, issue_date, permit_type,
                             work_type, description, address, project_value,
                             latitude, longitude, geom, status, data_source)
                        VALUES
                            (%s, 'Calgary', %s, %s, %s, %s, %s, %s, %s, %s,
                             {geom_expr}, %s, 'calgary_open_data')
                        ON CONFLICT (permit_number, city) DO UPDATE SET
                            issue_date = EXCLUDED.issue_date,
                            permit_type = EXCLUDED.permit_type,
                            work_type = EXCLUDED.work_type,
                            description = EXCLUDED.description,
                            address = EXCLUDED.address,
                            project_value = EXCLUDED.project_value,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            geom = EXCLUDED.geom,
                            status = EXCLUDED.status
                        """,
                        [
                            permit_number,
                            issue_date,
                            permit_type,
                            work_type,
                            description,
                            address,
                            project_value,
                            lat,
                            lon,
                            *geom_params,
                            status,
                        ],
                    )
                    cur.execute("RELEASE SAVEPOINT row_sp")
                    inserted += 1
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    logger.warning(
                        "Skipping Calgary permit %s: %s",
                        permit_number,
                        exc,
                    )
                    continue

    logger.info("Calgary building permits: inserted/updated %d rows", inserted)
    return inserted
