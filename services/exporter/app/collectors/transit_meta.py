"""Transit metadata collector - tracks data freshness from PostgreSQL.

Bridges the loader -> exporter gap by querying the transit schema
for the most recent load timestamp, exposing it as a Prometheus gauge
so Grafana dashboards and alerting rules can track staleness.
"""

import logging
import os
import time
from typing import Optional

from prometheus_client import Gauge

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus gauge
# ---------------------------------------------------------------------------
transit_last_update_gauge = Gauge(
    "cdo_transit_last_update_timestamp",
    "Timestamp of most recent transit data load (unix epoch)",
)

# Database URL - same PostgreSQL instance used by the loader
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://cdo:cdo_secret@postgres:5432/cdo",
)


async def fetch_and_update(cache=None) -> None:
    """Query PostgreSQL for the latest transit load timestamp.

    The cache parameter is accepted for interface compatibility with
    other collectors but is not used here -- we always query live
    because this is a lightweight single-row query.
    """
    import asyncio

    loop = asyncio.get_running_loop()
    ts = await loop.run_in_executor(None, _query_latest_transit_load)

    if ts is not None:
        transit_last_update_gauge.set(ts)
        logger.info("Transit last update timestamp set to %.0f", ts)
    else:
        logger.debug("No transit data found in database (table may not exist yet)")


def _query_latest_transit_load() -> Optional[float]:
    """Connect to PostgreSQL and fetch MAX(loaded_at) from transit.stops.

    Returns the timestamp as a Unix epoch float, or None if the table
    doesn't exist or has no rows.
    """
    try:
        import psycopg2

        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                # Check if the transit schema and stops table exist
                cur.execute(
                    "SELECT EXTRACT(EPOCH FROM MAX(loaded_at)) "
                    "FROM transit.stops"
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
                return None
        finally:
            conn.close()

    except Exception:
        # Table may not exist yet if loader hasn't run, or DB is unreachable.
        # This is expected during initial deployment -- don't spam logs.
        logger.debug("Could not query transit.stops", exc_info=True)
        return None
