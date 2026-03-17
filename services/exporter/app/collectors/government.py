"""Government spending collector - fetches federal contract and grant data from Open Canada CKAN."""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from prometheus_client import Gauge

from app.cache import RedisCache
from app.config import settings
from app.parsers.ckan import fetch_ckan_dataset, fetch_ckan_resource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus Gauges (SRD naming convention)
# ---------------------------------------------------------------------------
contracts_value_gauge = Gauge(
    "cdo_government_contracts_total_value_dollars",
    "Total value of federal contracts over $10K (CAD)",
    ["department", "fiscal_year"],
)
contracts_count_gauge = Gauge(
    "cdo_government_contracts_count",
    "Number of federal contracts over $10K",
    ["department", "fiscal_year"],
)
grants_value_gauge = Gauge(
    "cdo_government_grants_total_value_dollars",
    "Total value of federal grants and contributions (CAD)",
    ["fiscal_year"],
)
travel_total_gauge = Gauge(
    "cdo_government_travel_total_dollars",
    "Total federal travel expenses (CAD)",
    ["fiscal_year"],
)
last_update_gauge = Gauge(
    "cdo_government_last_update_timestamp",
    "Timestamp of last successful government data update (unix epoch)",
)

# ---------------------------------------------------------------------------
# CKAN package IDs from Open Canada
# ---------------------------------------------------------------------------
CONTRACTS_PACKAGE_ID = "d8f85d91-7dec-4fd1-8055-483b77225d8b"
GRANTS_PACKAGE_ID = "432527ab-7aac-45b5-81d6-7597107a7013"

# Only emit data for the last 5 fiscal years to control cardinality
MAX_FISCAL_YEARS = 5


def _current_fiscal_year() -> str:
    """Return the current federal fiscal year label (e.g. '2025-2026').

    The Canadian federal fiscal year runs April 1 to March 31.
    """
    now = time.localtime()
    year = now.tm_year
    month = now.tm_mon
    if month >= 4:
        return f"{year}-{year + 1}"
    return f"{year - 1}-{year}"


def _recent_fiscal_years() -> List[str]:
    """Return the last MAX_FISCAL_YEARS fiscal year labels."""
    now = time.localtime()
    year = now.tm_year
    month = now.tm_mon
    end_year = year if month >= 4 else year - 1
    return [f"{end_year - i}-{end_year - i + 1}" for i in range(MAX_FISCAL_YEARS)]


async def fetch_and_update(cache: RedisCache) -> None:
    """Fetch government spending data from Open Canada CKAN, update Prometheus gauges.

    Uses Redis cache with 24h TTL since this data updates infrequently.
    """
    cache_key = "government:spending"
    ttl = settings.CACHE_TTLS.get("government", 86400)

    # Try cache first
    cached = await cache.get(cache_key)
    if cached:
        logger.info("Government data served from cache")
        _apply_cached(cached)
        return

    # Fetch fresh data
    logger.info("Fetching fresh government spending data from Open Canada CKAN")
    try:
        data = await _fetch_government_data()

        if data:
            _apply_cached(data)
            await cache.set(cache_key, data, ttl=ttl)
            last_update_gauge.set(time.time())
            logger.info(
                "Government data updated: %d contract aggregates, %d grant years",
                len(data.get("contracts", {})),
                len(data.get("grants", {})),
            )
        else:
            logger.warning("Government spending fetch returned empty data")
    except Exception:
        logger.exception("Failed to fetch government spending data")


async def _fetch_government_data() -> Optional[Dict[str, Any]]:
    """Fetch contracts and grants datasets from CKAN and aggregate."""
    allowed_years = set(_recent_fiscal_years())
    results: Dict[str, Any] = {
        "contracts": {},  # {dept_fiscalyear: {value, count}}
        "grants": {},     # {fiscal_year: value}
        "travel": {},     # {fiscal_year: value}
    }

    # --- Contracts over $10K ---
    try:
        dataset = await fetch_ckan_dataset(CONTRACTS_PACKAGE_ID)
        if dataset and dataset.get("resources"):
            # Find the CSV/datastore resource (first with datastore_active)
            resource_id = _find_datastore_resource(dataset["resources"])
            if resource_id:
                contracts = await _fetch_all_contract_records(resource_id, allowed_years)
                results["contracts"] = contracts
                logger.info("Fetched contract aggregates for %d department-year combos", len(contracts))
    except Exception:
        logger.exception("Failed to fetch contracts dataset")

    # --- Grants and Contributions ---
    try:
        dataset = await fetch_ckan_dataset(GRANTS_PACKAGE_ID)
        if dataset and dataset.get("resources"):
            resource_id = _find_datastore_resource(dataset["resources"])
            if resource_id:
                grants = await _fetch_grant_aggregates(resource_id, allowed_years)
                results["grants"] = grants
                logger.info("Fetched grant aggregates for %d fiscal years", len(grants))
    except Exception:
        logger.exception("Failed to fetch grants dataset")

    return results if (results["contracts"] or results["grants"]) else None


def _find_datastore_resource(resources: List[dict]) -> Optional[str]:
    """Find the first datastore-active resource in a CKAN dataset.

    Falls back to the first CSV/XLS resource if no datastore-active resource.
    """
    for r in resources:
        if r.get("datastore_active"):
            return r["id"]
    # Fallback: first CSV resource (may need direct download instead of datastore_search)
    for r in resources:
        fmt = (r.get("format") or "").upper()
        if fmt in ("CSV", "XLS", "XLSX"):
            return r["id"]
    return None


async def _fetch_all_contract_records(
    resource_id: str,
    allowed_years: set,
) -> Dict[str, Dict[str, float]]:
    """Fetch contract records and aggregate by department + fiscal year.

    Paginates through the CKAN datastore to build totals.
    Returns: {f"{dept}|{fiscal_year}": {"value": total_dollars, "count": num_contracts}}
    """
    aggregates: Dict[str, Dict[str, float]] = defaultdict(lambda: {"value": 0.0, "count": 0})
    offset = 0
    batch_size = 1000
    max_records = 10000  # Safety limit

    while offset < max_records:
        records = await fetch_ckan_resource(resource_id, limit=batch_size, offset=offset)
        if not records:
            break

        for rec in records:
            fiscal_year = _extract_fiscal_year(rec)
            if fiscal_year and fiscal_year in allowed_years:
                dept = _normalize_department(rec)
                contract_value = _parse_dollar_value(rec.get("contract_value") or rec.get("original_value") or rec.get("amendment_value") or "0")
                key = f"{dept}|{fiscal_year}"
                aggregates[key]["value"] += contract_value
                aggregates[key]["count"] += 1

        if len(records) < batch_size:
            break
        offset += batch_size

    return dict(aggregates)


async def _fetch_grant_aggregates(
    resource_id: str,
    allowed_years: set,
) -> Dict[str, float]:
    """Fetch grant records and aggregate total value by fiscal year."""
    totals: Dict[str, float] = defaultdict(float)
    offset = 0
    batch_size = 1000
    max_records = 10000
    logged_keys = False

    while offset < max_records:
        records = await fetch_ckan_resource(resource_id, limit=batch_size, offset=offset)
        if not records:
            break

        if not logged_keys and records:
            logger.info("Grant record sample keys: %s", list(records[0].keys()))
            logged_keys = True

        for rec in records:
            fiscal_year = _extract_fiscal_year(rec)
            if fiscal_year and fiscal_year in allowed_years:
                # Try many possible column names for the grant amount
                raw_value = (
                    rec.get("value")
                    or rec.get("total")
                    or rec.get("agreement_value")
                    or rec.get("total_funding")
                    or rec.get("amount")
                    or rec.get("amendment_value")
                    or rec.get("original_value")
                    or rec.get("contract_value")
                    or rec.get("total_value")
                    or rec.get("agreement_start_date_value")
                    or "0"
                )
                totals[fiscal_year] += _parse_dollar_value(raw_value)

        if len(records) < batch_size:
            break
        offset += batch_size

    return dict(totals)


def _extract_fiscal_year(record: dict) -> Optional[str]:
    """Extract fiscal year from a CKAN record, trying common field names."""
    # Try explicit fiscal_year field first
    for field in ("fiscal_year", "year"):
        val = record.get(field)
        if val:
            val = str(val).strip()
            if "-" in val and len(val) >= 7:
                parts = val.split("-")
                try:
                    start = int(parts[0])
                    end_str = parts[1]
                    end = int(end_str) if len(end_str) == 4 else start - (start % 100) + int(end_str)
                    return f"{start}-{end}"
                except (ValueError, IndexError):
                    pass
            try:
                y = int(val[:4])
                return f"{y}-{y + 1}"
            except ValueError:
                pass

    # Derive from contract_date (format: "2018-04-01")
    for field in ("contract_date", "contract_period_start", "delivery_date"):
        val = record.get(field)
        if val:
            val = str(val).strip()
            try:
                year = int(val[:4])
                month = int(val[5:7])
                # Canadian fiscal year: Apr-Mar. If month >= 4, FY starts this year.
                fy_start = year if month >= 4 else year - 1
                return f"{fy_start}-{fy_start + 1}"
            except (ValueError, IndexError):
                pass

    # Try reference_number (format: "C-2018-2019-Q1-00069")
    ref = record.get("reference_number", "")
    if ref:
        import re
        m = re.search(r"(\d{4})-(\d{4})", ref)
        if m:
            return f"{m.group(1)}-{m.group(2)}"

    return None


def _normalize_department(record: dict) -> str:
    """Extract and normalize department name from a contract record."""
    for field in ("owner_org_title", "owner_org", "department_name_en", "department"):
        val = record.get(field)
        if val and str(val).strip():
            name = str(val).strip()
            # Truncate very long department names for label cardinality
            if len(name) > 60:
                name = name[:57] + "..."
            return name
    return "Unknown"


def _parse_dollar_value(raw: Any) -> float:
    """Parse a dollar value that may contain commas, dollar signs, etc."""
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        cleaned = str(raw).replace("$", "").replace(",", "").replace(" ", "").strip()
        return float(cleaned) if cleaned else 0.0
    except (ValueError, TypeError):
        return 0.0


def _apply_cached(data: Dict[str, Any]) -> None:
    """Apply cached spending data to Prometheus gauges."""
    # Contracts by department + fiscal year
    contracts = data.get("contracts", {})
    for key, agg in contracts.items():
        parts = key.split("|", 1)
        if len(parts) == 2:
            dept, fy = parts
            contracts_value_gauge.labels(department=dept, fiscal_year=fy).set(agg["value"])
            contracts_count_gauge.labels(department=dept, fiscal_year=fy).set(agg["count"])

    # Grants by fiscal year
    grants = data.get("grants", {})
    for fy, value in grants.items():
        grants_value_gauge.labels(fiscal_year=fy).set(value)

    # Travel by fiscal year (sourced from contracts data tagged as travel if available)
    travel = data.get("travel", {})
    for fy, value in travel.items():
        travel_total_gauge.labels(fiscal_year=fy).set(value)
