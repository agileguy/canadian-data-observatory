"""
Parser / client for the CKAN open-data API (open.canada.ca).

Provides helpers to fetch dataset metadata and resource records from
the Government of Canada's Open Data portal.
"""

import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://open.canada.ca/data/api/3/action/"

# Default timeout for CKAN API requests (seconds)
REQUEST_TIMEOUT = 30.0


async def fetch_ckan_dataset(package_id: str) -> Optional[dict]:
    """Fetch dataset metadata from the CKAN API.

    Args:
        package_id: The CKAN package (dataset) identifier.
                    Can be a UUID or a human-readable slug.

    Returns:
        The dataset metadata dict (the 'result' field from the CKAN response),
        or None if the request fails.
    """
    url = f"{BASE_URL}package_show"
    params = {"id": package_id}

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                logger.error(
                    "CKAN package_show returned success=false for %s: %s",
                    package_id,
                    data.get("error"),
                )
                return None

            return data.get("result")

    except httpx.HTTPStatusError as exc:
        logger.error(
            "CKAN package_show HTTP %d for %s: %s",
            exc.response.status_code,
            package_id,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("CKAN request failed for package %s: %s", package_id, exc)
    except Exception as exc:
        logger.error("Unexpected error fetching CKAN package %s: %s", package_id, exc)

    return None


async def fetch_ckan_resource(
    resource_id: str,
    limit: int = 100,
    offset: int = 0,
    filters: Optional[dict[str, Any]] = None,
    sort: Optional[str] = None,
) -> Optional[list[dict]]:
    """Fetch records from a CKAN datastore resource.

    Uses the datastore_search action which provides structured access to
    tabular resources.

    Args:
        resource_id: The CKAN resource UUID.
        limit: Maximum number of records to return (default 100).
        offset: Number of records to skip (for pagination).
        filters: Optional dict of field:value filters.
        sort: Optional sort string (e.g. 'date desc').

    Returns:
        List of record dicts, or None if the request fails.
    """
    url = f"{BASE_URL}datastore_search"
    params: dict[str, Any] = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset,
    }

    if filters:
        import json
        params["filters"] = json.dumps(filters)

    if sort:
        params["sort"] = sort

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                logger.error(
                    "CKAN datastore_search returned success=false for %s: %s",
                    resource_id,
                    data.get("error"),
                )
                return None

            result = data.get("result", {})
            records = result.get("records", [])

            logger.debug(
                "CKAN resource %s: fetched %d records (total: %s)",
                resource_id,
                len(records),
                result.get("total"),
            )
            return records

    except httpx.HTTPStatusError as exc:
        logger.error(
            "CKAN datastore_search HTTP %d for %s: %s",
            exc.response.status_code,
            resource_id,
            exc,
        )
    except httpx.RequestError as exc:
        logger.error("CKAN request failed for resource %s: %s", resource_id, exc)
    except Exception as exc:
        logger.error("Unexpected error fetching CKAN resource %s: %s", resource_id, exc)

    return None
