"""Toronto Open Data loader - building permits via CKAN API."""

import csv
import io
import logging
from datetime import datetime

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
REQUEST_TIMEOUT = 60.0


def _fetch_package(package_name: str) -> dict | None:
    """Fetch a dataset package from the Toronto CKAN portal."""
    url = f"{BASE_URL}package_show"
    params = {"id": package_name}

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                logger.error(
                    "Toronto CKAN package_show failed for %s: %s",
                    package_name,
                    data.get("error"),
                )
                return None

            return data.get("result")

    except httpx.HTTPStatusError as exc:
        logger.error(
            "Toronto CKAN HTTP %d for %s: %s",
            exc.response.status_code,
            package_name,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("Toronto CKAN request failed for %s: %s", package_name, exc)

    return None


def _find_csv_resource(package: dict) -> dict | None:
    """Find the first CSV resource in a CKAN package."""
    for resource in package.get("resources", []):
        fmt = (resource.get("format") or "").upper()
        if fmt == "CSV":
            return resource
    return None


def _download_csv(url: str) -> list[dict] | None:
    """Download and parse a CSV file from a URL."""
    try:
        with httpx.Client(timeout=120.0, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()

            text = resp.text
            reader = csv.DictReader(io.StringIO(text))
            return list(reader)

    except httpx.HTTPStatusError as exc:
        logger.error("CSV download HTTP %d: %s", exc.response.status_code, exc)
    except httpx.RequestError as exc:
        logger.error("CSV download failed: %s", exc)
    except Exception as exc:
        logger.error("CSV parse error: %s", exc)

    return None


def _parse_date(date_str: str | None) -> datetime | None:
    """Try to parse a date string in common Toronto Open Data formats."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def _parse_numeric(val: str | None) -> float | None:
    """Parse a numeric string, stripping currency symbols."""
    if not val:
        return None
    cleaned = val.strip().replace("$", "").replace(",", "")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def load_building_permits() -> int:
    """Fetch Toronto building permits from CKAN and insert into DB.

    Returns:
        Number of rows inserted.
    """
    logger.info("Fetching Toronto building permits package...")
    package = _fetch_package("building-permits")

    if not package:
        logger.error("Could not fetch Toronto building-permits package")
        return 0

    resource = _find_csv_resource(package)
    if not resource:
        logger.error("No CSV resource found in building-permits package")
        return 0

    download_url = resource.get("url")
    logger.info("Downloading CSV from %s", download_url)
    rows = _download_csv(download_url)

    if not rows:
        logger.warning("No rows parsed from Toronto building permits CSV")
        return 0

    logger.info("Parsed %d rows from Toronto building permits CSV", len(rows))

    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                permit_number = (
                    row.get("PERMIT_NUM")
                    or row.get("PERMIT_NUMBER")
                    or row.get("permit_num")
                    or ""
                ).strip()

                if not permit_number:
                    continue

                issue_date = _parse_date(
                    row.get("ISSUED_DATE")
                    or row.get("ISSUE_DATE")
                    or row.get("issued_date")
                )
                permit_type = (
                    row.get("PERMIT_TYPE")
                    or row.get("permit_type")
                    or ""
                ).strip() or None
                work_type = (
                    row.get("WORK")
                    or row.get("WORK_TYPE")
                    or row.get("work_type")
                    or ""
                ).strip() or None
                description = (
                    row.get("DESCRIPTION")
                    or row.get("description")
                    or ""
                ).strip() or None
                address = (
                    row.get("STREET_NAME")
                    or row.get("ADDRESS")
                    or row.get("address")
                    or ""
                ).strip() or None
                project_value = _parse_numeric(
                    row.get("EST_CONST_COST")
                    or row.get("PROJECT_VALUE")
                    or row.get("project_value")
                )
                status = (
                    row.get("STATUS")
                    or row.get("status")
                    or ""
                ).strip() or None

                lat = _parse_numeric(
                    row.get("LATITUDE") or row.get("latitude")
                )
                lon = _parse_numeric(
                    row.get("LONGITUDE") or row.get("longitude")
                )

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
                            (%s, 'Toronto', %s, %s, %s, %s, %s, %s, %s, %s,
                             {geom_expr}, %s, 'toronto_open_data')
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
                        "Skipping Toronto permit %s: %s",
                        permit_number,
                        exc,
                    )
                    continue

    logger.info("Toronto building permits: inserted/updated %d rows", inserted)
    return inserted
