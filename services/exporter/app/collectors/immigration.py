"""Immigration collector - fetches IRCC data from Open Canada CKAN portal."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from prometheus_client import Gauge

import csv
import io

import httpx

from app.cache import RedisCache
from app.parsers.ckan import fetch_ckan_dataset, fetch_ckan_resource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CKAN Package IDs (Open Canada / IRCC)
# ---------------------------------------------------------------------------
PACKAGES: Dict[str, str] = {
    "permanent_residents": "f7e5498e-0ad8-4417-85c9-9b8aff9b9eda",
    "temporary_residents": "360024f2-17e9-4558-bfc1-3616485d65b9",
    "refugee_claimants": "b6cbcf4d-f763-4924-a2fb-8cc4a06e3de4",
    "citizenship_grants": "9b34e712-513f-44e9-babf-9df4f7256550",
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


def _detect_field(records: List[Dict], candidates: List[str]) -> str:
    """Return the first field name from *candidates* that exists in the records.

    Falls back to the first candidate if none are found (the aggregation
    will simply produce an empty result rather than crashing).
    """
    if not records:
        return candidates[0]
    sample = records[0]
    for c in candidates:
        if c in sample:
            return c
    # Try case-insensitive match
    lower_keys = {k.lower(): k for k in sample.keys()}
    for c in candidates:
        if c.lower() in lower_keys:
            return lower_keys[c.lower()]
    logger.warning(
        "None of %s found in record keys %s; using '%s'",
        candidates, list(sample.keys())[:10], candidates[0],
    )
    return candidates[0]


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
        province_field = _detect_field(
            results["permanent_residents"],
            ["Province/Territory of intended destination", "Province/Territory",
             "Province / Territory of Intended Destination", "province_territory"],
        )
        year_field = _detect_field(
            results["permanent_residents"], ["Year", "year", "YEAR"]
        )
        value_field = _detect_field(
            results["permanent_residents"], ["Value", "value", "VALUE", "Persons"]
        )
        aggregated["permanent_residents"] = _aggregate_by_province_year(
            results["permanent_residents"],
            province_field=province_field,
            year_field=year_field,
            value_field=value_field,
        )
        country_field = _detect_field(
            results["permanent_residents"],
            ["Country of citizenship", "Country of Citizenship",
             "country_of_citizenship", "Source country"],
        )
        aggregated["by_source_country"] = _aggregate_by_field_year(
            results["permanent_residents"],
            field=country_field,
            year_field=year_field,
            value_field=value_field,
            top_n=15,
        )

    if "temporary_residents" in results:
        province_field = _detect_field(
            results["temporary_residents"],
            ["Province/Territory", "Province/Territory of temporary residence",
             "Province / Territory", "province_territory"],
        )
        year_field = _detect_field(
            results["temporary_residents"], ["Year", "year", "YEAR"]
        )
        value_field = _detect_field(
            results["temporary_residents"], ["Value", "value", "VALUE", "Persons"]
        )
        aggregated["temporary_residents"] = _aggregate_by_province_year(
            results["temporary_residents"],
            province_field=province_field,
            year_field=year_field,
            value_field=value_field,
        )

    if "refugee_claimants" in results:
        province_field = _detect_field(
            results["refugee_claimants"],
            ["Province/Territory of claim", "Province/Territory",
             "Province / Territory of Claim", "province_territory"],
        )
        year_field = _detect_field(
            results["refugee_claimants"], ["Year", "year", "YEAR"]
        )
        value_field = _detect_field(
            results["refugee_claimants"], ["Value", "value", "VALUE", "Persons"]
        )
        aggregated["refugees"] = _aggregate_by_province_year(
            results["refugee_claimants"],
            province_field=province_field,
            year_field=year_field,
            value_field=value_field,
        )

    if "citizenship_grants" in results:
        year_field = _detect_field(
            results["citizenship_grants"], ["Year", "year", "YEAR"]
        )
        value_field = _detect_field(
            results["citizenship_grants"], ["Value", "value", "VALUE", "Persons"]
        )
        aggregated["citizenship_grants"] = _aggregate_by_year(
            results["citizenship_grants"],
            year_field=year_field,
            value_field=value_field,
        )

    return aggregated if aggregated else None


async def _fetch_package_records(package_id: str) -> Optional[List[Dict]]:
    """Fetch dataset metadata then pull records from the first CSV resource."""
    dataset = await fetch_ckan_dataset(package_id)
    if not dataset:
        return None

    # Find the first datastore-active CSV resource (prefer CSV over XLSX)
    resources = dataset.get("resources", [])
    csv_resource = None

    # Priority 1: datastore-active CSV
    for res in resources:
        fmt = (res.get("format") or "").upper()
        if fmt == "CSV" and res.get("datastore_active", False):
            csv_resource = res
            break

    # Priority 2: any datastore-active resource
    if csv_resource is None:
        for res in resources:
            if res.get("datastore_active", False):
                csv_resource = res
                break

    # Priority 3: downloadable CSV (not XLSX — we can't parse XLSX without openpyxl)
    if csv_resource is None:
        for res in resources:
            fmt = (res.get("format") or "").upper()
            if fmt == "CSV":
                csv_resource = res
                break

    # Priority 4: XLSX — skip with warning (requires openpyxl which is not installed)
    if csv_resource is None:
        has_xlsx = any((res.get("format") or "").upper() == "XLSX" for res in resources)
        if has_xlsx:
            logger.warning(
                "Package %s only has XLSX resources; skipping (openpyxl not installed)",
                package_id,
            )
        else:
            logger.warning("No suitable resource found in package %s", package_id)
        return None

    # If not datastore-active, try downloading the CSV directly
    if not csv_resource.get("datastore_active", False):
        return await _download_resource_csv(csv_resource, package_id)

    resource_id = csv_resource["id"]
    logger.debug("Using resource %s from package %s", resource_id, package_id)

    # Paginate through all records (CKAN default limit is 100)
    all_records: List[Dict] = []
    offset = 0
    page_size = 1000
    max_records = 500000

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
        if len(all_records) >= max_records:
            logger.warning(
                "Hit max_records safety cap (%d) for package %s",
                max_records,
                package_id,
            )
            break

    logger.info(
        "Fetched %d records from package %s (resource %s)",
        len(all_records),
        package_id,
        resource_id,
    )
    return all_records if all_records else None


async def _download_resource_csv(
    resource: dict, package_id: str
) -> Optional[List[Dict]]:
    """Download a CSV resource directly when CKAN datastore is not active.

    Falls back to downloading the raw CSV file from the resource URL and
    parsing it with the csv module.
    """
    url = resource.get("url")
    if not url:
        logger.warning("Resource in package %s has no URL", package_id)
        return None

    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text

        reader = csv.DictReader(io.StringIO(text))
        records = list(reader)
        logger.info(
            "Downloaded %d records from CSV resource in package %s",
            len(records),
            package_id,
        )
        return records if records else None

    except Exception as exc:
        logger.error(
            "Failed to download CSV resource from %s (package %s): %s",
            url,
            package_id,
            exc,
        )
        return None


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


def _recent_years(years: Dict[str, Any], n: int = 5) -> Dict[str, Any]:
    """Return only the most recent N years from a year-keyed dict."""
    sorted_keys = sorted(years.keys(), reverse=True)[:n]
    return {k: years[k] for k in sorted_keys}


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached immigration data to Prometheus gauges.

    Only emits the most recent 5 years of data per metric to keep
    cardinality reasonable (~14 provinces x 5 years = 70 series).
    """
    # Permanent residents by province and year
    pr = data.get("permanent_residents", {})
    for province, years in pr.items():
        for year, total in _recent_years(years).items():
            permanent_residents_gauge.labels(province=province, year=year).set(total)

    # Temporary residents by province and year
    tr = data.get("temporary_residents", {})
    for province, years in tr.items():
        for year, total in _recent_years(years).items():
            temporary_residents_gauge.labels(province=province, year=year).set(total)

    # Refugees by province and year
    ref = data.get("refugees", {})
    for province, years in ref.items():
        for year, total in _recent_years(years).items():
            refugees_gauge.labels(province=province, year=year).set(total)

    # Citizenship grants by year
    cg = data.get("citizenship_grants", {})
    for year, total in _recent_years(cg).items():
        citizenship_grants_gauge.labels(year=year).set(total)

    # Source countries
    sc = data.get("by_source_country", {})
    for country, years in sc.items():
        for year, total in _recent_years(years).items():
            source_country_gauge.labels(country=country, year=year).set(total)
