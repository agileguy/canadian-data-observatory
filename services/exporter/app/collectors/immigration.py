"""Immigration collector - fetches IRCC data from Open Canada CKAN portal."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from prometheus_client import Gauge

from app.cache import RedisCache
from app.parsers.ckan import fetch_ckan_dataset, fetch_ckan_resource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CKAN Package IDs (Open Canada / IRCC)
# ---------------------------------------------------------------------------
PACKAGES: Dict[str, str] = {
    "permanent_residents": "f7e5498e-0ad8-4f28-85e3-28b3f4006816",
    "temporary_residents": "360024f2-17e9-4558-bfc1-3616485d65b9",
    "refugee_claimants": "b6cbcf4d-f763-4864-a67e-20e28d44ac28",
    "citizenship_grants": "3a72a3d3-fee4-49c5-869f-a530dba2bba4",
}

# Province code mapping for normalisation
PROVINCE_CODES: Dict[str, str] = {
    "Canada": "CA",
    "Ontario": "ON",
    "Quebec": "QC",
    "British Columbia": "BC",
    "Alberta": "AB",
    "Manitoba": "MB",
    "Saskatchewan": "SK",
    "Nova Scotia": "NS",
    "New Brunswick": "NB",
    "Newfoundland and Labrador": "NL",
    "Prince Edward Island": "PE",
    "Yukon": "YT",
    "Northwest Territories": "NT",
    "Nunavut": "NU",
    "Province not stated": "XX",
    # French variants
    "Colombie-Britannique": "BC",
    "Nouveau-Brunswick": "NB",
    "Nouvelle-\u00c9cosse": "NS",
    "Terre-Neuve-et-Labrador": "NL",
    "\u00cele-du-Prince-\u00c9douard": "PE",
    "Territoires du Nord-Ouest": "NT",
}

# ---------------------------------------------------------------------------
# Prometheus gauges
# ---------------------------------------------------------------------------
permanent_residents_gauge = Gauge(
    "cdo_immigration_permanent_residents_total",
    "Total permanent residents admitted",
    ["province", "year"],
)
temporary_residents_gauge = Gauge(
    "cdo_immigration_temporary_residents_total",
    "Total temporary residents",
    ["province", "year"],
)
refugees_gauge = Gauge(
    "cdo_immigration_refugees_total",
    "Total refugee claimants",
    ["province", "year"],
)
citizenship_grants_gauge = Gauge(
    "cdo_immigration_citizenship_grants_total",
    "Total citizenship grants",
    ["year"],
)
source_country_gauge = Gauge(
    "cdo_immigration_by_source_country_total",
    "Permanent residents by source country",
    ["country", "year"],
)
last_update_gauge = Gauge(
    "cdo_immigration_last_update_timestamp",
    "Timestamp of last successful immigration data update (unix epoch)",
)

# Redis cache settings
CACHE_KEY = "immigration:ircc"
CACHE_TTL = 604800  # 7 days


async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch immigration data from IRCC (Open Canada CKAN) and update gauges.

    Uses Redis cache with a 7-day TTL since IRCC publishes data quarterly.
    """
    cached = await cache.get(CACHE_KEY)
    if cached:
        logger.info("Immigration data served from cache")
        _apply_cached(cached)
        return

    logger.info("Fetching fresh immigration data from Open Canada (IRCC)")
    try:
        data = await _fetch_all_ircc()

        if data:
            _apply_cached(data)
            await cache.set(CACHE_KEY, data, ttl=CACHE_TTL)
            last_update_gauge.set(time.time())
            logger.info(
                "Immigration data updated: %d categories",
                len(data),
            )
        else:
            logger.warning("IRCC fetch returned empty data")
    except Exception:
        logger.exception("Failed to fetch immigration data from IRCC")


async def _fetch_all_ircc() -> Optional[Dict[str, Any]]:
    """Fetch data from all four IRCC CKAN packages in parallel."""
    results: Dict[str, Any] = {}

    tasks = {
        "permanent_residents": _fetch_package_records(PACKAGES["permanent_residents"]),
        "temporary_residents": _fetch_package_records(PACKAGES["temporary_residents"]),
        "refugee_claimants": _fetch_package_records(PACKAGES["refugee_claimants"]),
        "citizenship_grants": _fetch_package_records(PACKAGES["citizenship_grants"]),
    }

    gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)

    for key, result in zip(tasks.keys(), gathered):
        if isinstance(result, Exception):
            logger.error("Failed to fetch %s: %s", key, result)
            continue
        if result:
            results[key] = result

    if not results:
        return None

    # Aggregate records into structured data for caching
    aggregated: Dict[str, Any] = {}

    if "permanent_residents" in results:
        aggregated["permanent_residents"] = _aggregate_by_province_year(
            results["permanent_residents"],
            province_field="Province/Territory of intended destination",
            year_field="Year",
            value_field="Value",
        )
        aggregated["by_source_country"] = _aggregate_by_field_year(
            results["permanent_residents"],
            field="Country of citizenship",
            year_field="Year",
            value_field="Value",
            top_n=15,
        )

    if "temporary_residents" in results:
        aggregated["temporary_residents"] = _aggregate_by_province_year(
            results["temporary_residents"],
            province_field="Province/Territory",
            year_field="Year",
            value_field="Value",
        )

    if "refugee_claimants" in results:
        aggregated["refugees"] = _aggregate_by_province_year(
            results["refugee_claimants"],
            province_field="Province/Territory of claim",
            year_field="Year",
            value_field="Value",
        )

    if "citizenship_grants" in results:
        aggregated["citizenship_grants"] = _aggregate_by_year(
            results["citizenship_grants"],
            year_field="Year",
            value_field="Value",
        )

    return aggregated if aggregated else None


async def _fetch_package_records(package_id: str) -> Optional[List[Dict]]:
    """Fetch dataset metadata then pull records from the first CSV resource."""
    dataset = await fetch_ckan_dataset(package_id)
    if not dataset:
        return None

    # Find the first datastore-active CSV resource
    resources = dataset.get("resources", [])
    csv_resource = None
    for res in resources:
        fmt = (res.get("format") or "").upper()
        if fmt in ("CSV", "XLSX") and res.get("datastore_active", False):
            csv_resource = res
            break

    # Fallback: first resource with datastore_active
    if csv_resource is None:
        for res in resources:
            if res.get("datastore_active", False):
                csv_resource = res
                break

    if csv_resource is None:
        logger.warning(
            "No datastore-active resource found in package %s", package_id
        )
        return None

    resource_id = csv_resource["id"]
    logger.debug("Using resource %s from package %s", resource_id, package_id)

    # Paginate through all records (CKAN default limit is 100)
    all_records: List[Dict] = []
    offset = 0
    page_size = 1000

    while True:
        records = await fetch_ckan_resource(
            resource_id, limit=page_size, offset=offset
        )
        if records is None:
            break
        all_records.extend(records)
        if len(records) < page_size:
            break
        offset += page_size

    logger.info(
        "Fetched %d records from package %s (resource %s)",
        len(all_records),
        package_id,
        resource_id,
    )
    return all_records if all_records else None


def _normalise_province(raw: Optional[str]) -> Optional[str]:
    """Normalise a province name to a two-letter code."""
    if not raw:
        return None
    cleaned = raw.strip()
    if cleaned in PROVINCE_CODES:
        return PROVINCE_CODES[cleaned]
    # Try case-insensitive match
    for name, code in PROVINCE_CODES.items():
        if name.lower() == cleaned.lower():
            return code
    return None


def _safe_int(value: Any) -> int:
    """Safely coerce a value to int, returning 0 on failure."""
    if value is None:
        return 0
    try:
        return int(float(str(value).replace(",", "")))
    except (ValueError, TypeError):
        return 0


def _aggregate_by_province_year(
    records: List[Dict],
    province_field: str,
    year_field: str,
    value_field: str,
) -> Dict[str, Dict[str, int]]:
    """Aggregate records into {province_code: {year: total}}."""
    agg: Dict[str, Dict[str, int]] = {}
    for rec in records:
        province = _normalise_province(rec.get(province_field))
        year = str(rec.get(year_field, "")).strip()
        val = _safe_int(rec.get(value_field))
        if not province or not year or not year.isdigit():
            continue
        agg.setdefault(province, {})
        agg[province][year] = agg[province].get(year, 0) + val
    return agg


def _aggregate_by_field_year(
    records: List[Dict],
    field: str,
    year_field: str,
    value_field: str,
    top_n: int = 15,
) -> Dict[str, Dict[str, int]]:
    """Aggregate records by arbitrary field and year, keeping top N."""
    agg: Dict[str, Dict[str, int]] = {}
    for rec in records:
        key = str(rec.get(field, "")).strip()
        year = str(rec.get(year_field, "")).strip()
        val = _safe_int(rec.get(value_field))
        if not key or not year or not year.isdigit():
            continue
        agg.setdefault(key, {})
        agg[key][year] = agg[key].get(year, 0) + val

    # Keep only top N by total across all years
    totals = {k: sum(v.values()) for k, v in agg.items()}
    top_keys = sorted(totals, key=totals.get, reverse=True)[:top_n]
    return {k: agg[k] for k in top_keys}


def _aggregate_by_year(
    records: List[Dict],
    year_field: str,
    value_field: str,
) -> Dict[str, int]:
    """Aggregate records into {year: total}."""
    agg: Dict[str, int] = {}
    for rec in records:
        year = str(rec.get(year_field, "")).strip()
        val = _safe_int(rec.get(value_field))
        if not year or not year.isdigit():
            continue
        agg[year] = agg.get(year, 0) + val
    return agg


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached immigration data to Prometheus gauges."""
    # Permanent residents by province and year
    pr = data.get("permanent_residents", {})
    for province, years in pr.items():
        for year, total in years.items():
            permanent_residents_gauge.labels(province=province, year=year).set(total)

    # Temporary residents by province and year
    tr = data.get("temporary_residents", {})
    for province, years in tr.items():
        for year, total in years.items():
            temporary_residents_gauge.labels(province=province, year=year).set(total)

    # Refugees by province and year
    ref = data.get("refugees", {})
    for province, years in ref.items():
        for year, total in years.items():
            refugees_gauge.labels(province=province, year=year).set(total)

    # Citizenship grants by year
    cg = data.get("citizenship_grants", {})
    for year, total in cg.items():
        citizenship_grants_gauge.labels(year=year).set(total)

    # Source countries
    sc = data.get("by_source_country", {})
    for country, years in sc.items():
        for year, total in years.items():
            source_country_gauge.labels(country=country, year=year).set(total)
