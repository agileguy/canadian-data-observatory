"""Government of Canada contracts loader - proactive disclosure via CKAN."""

import csv
import io
import logging
from datetime import datetime

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

# Open Canada CKAN API
BASE_URL = "https://open.canada.ca/data/api/3/action/"
CONTRACTS_PACKAGE_ID = "d8f85d91-7dec-4fd1-8055-483b77225d8b"
REQUEST_TIMEOUT = 60.0


def _fetch_package(package_id: str) -> dict | None:
    """Fetch dataset metadata from the Open Canada CKAN API."""
    url = f"{BASE_URL}package_show"
    params = {"id": package_id}

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                logger.error(
                    "CKAN package_show failed for %s: %s",
                    package_id,
                    data.get("error"),
                )
                return None

            return data.get("result")

    except httpx.HTTPStatusError as exc:
        logger.error(
            "CKAN HTTP %d for %s: %s",
            exc.response.status_code,
            package_id,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("CKAN request failed for %s: %s", package_id, exc)

    return None


def _find_csv_resource(package: dict) -> dict | None:
    """Find the primary CSV resource in a CKAN package."""
    for resource in package.get("resources", []):
        fmt = (resource.get("format") or "").upper()
        if fmt == "CSV":
            return resource
    return None


def _download_csv(url: str) -> list[dict] | None:
    """Download and parse a CSV file."""
    try:
        with httpx.Client(timeout=180.0, follow_redirects=True) as client:
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
    """Parse date strings from Government of Canada disclosure data."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def _parse_numeric(val: str | None) -> float | None:
    """Parse a numeric/currency string."""
    if not val:
        return None
    cleaned = val.strip().replace("$", "").replace(",", "").replace(" ", "")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def load_government_contracts() -> int:
    """Fetch federal government contracts over $10K and insert into DB.

    Returns:
        Number of rows inserted.
    """
    logger.info("Fetching government contracts package %s...", CONTRACTS_PACKAGE_ID)
    package = _fetch_package(CONTRACTS_PACKAGE_ID)

    if not package:
        logger.error("Could not fetch government contracts package")
        return 0

    resource = _find_csv_resource(package)
    if not resource:
        logger.error("No CSV resource found in contracts package")
        return 0

    download_url = resource.get("url")
    logger.info("Downloading contracts CSV from %s", download_url)
    rows = _download_csv(download_url)

    if not rows:
        logger.warning("No rows parsed from government contracts CSV")
        return 0

    logger.info("Parsed %d rows from government contracts CSV", len(rows))

    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                # The Government of Canada proactive disclosure CSVs have
                # bilingual column names; try common English variants
                contract_id = (
                    row.get("reference_number")
                    or row.get("contract_number")
                    or row.get("Reference Number")
                    or row.get("ref_number")
                    or ""
                ).strip()

                vendor = (
                    row.get("vendor_name")
                    or row.get("Vendor Name")
                    or row.get("economic_object_code")
                    or ""
                ).strip() or None

                department = (
                    row.get("owner_org")
                    or row.get("department")
                    or row.get("owner_org_title")
                    or row.get("Department")
                    or ""
                ).strip() or None

                contract_value = _parse_numeric(
                    row.get("contract_value")
                    or row.get("original_value")
                    or row.get("Contract Value")
                    or row.get("contract_period_start")
                )

                contract_title = (
                    row.get("description_en")
                    or row.get("description")
                    or row.get("Description")
                    or ""
                ).strip() or None

                award_date = _parse_date(
                    row.get("contract_date")
                    or row.get("award_date")
                    or row.get("contract_period_start")
                    or row.get("Contract Date")
                )

                contract_type = (
                    row.get("procurement_id")
                    or row.get("contract_type")
                    or row.get("instrument_type")
                    or ""
                ).strip() or None

                # Skip rows that are completely empty
                if not contract_id and not vendor and not department:
                    continue

                # Generate a fallback ID if none available
                if not contract_id:
                    contract_id = (
                        f"GOV-{department or 'UNK'}"
                        f"-{vendor or 'UNK'}"
                        f"-{contract_value or 0}"
                    )[:50]

                try:
                    cur.execute(
                        """
                        INSERT INTO muni.government_contracts
                            (contract_id, city, vendor_name, contract_title,
                             award_date, contract_value, department,
                             contract_type, data_source)
                        VALUES
                            (%s, 'Federal', %s, %s, %s, %s, %s, %s,
                             'open_canada_proactive_disclosure')
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            contract_id,
                            vendor,
                            contract_title,
                            award_date,
                            contract_value,
                            department,
                            contract_type,
                        ],
                    )
                    inserted += 1
                except Exception as exc:
                    logger.warning(
                        "Failed to insert government contract %s: %s",
                        contract_id,
                        exc,
                    )
                    conn.rollback()
                    continue

    logger.info("Government contracts: inserted %d rows", inserted)
    return inserted
