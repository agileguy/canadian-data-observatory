"""CDO Exporter - Prometheus metrics exporter for Canadian open data."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    REGISTRY,
)

from app.cache import RedisCache
from app.config import settings
from app.collectors import economy, climate, housing, crime

logger = logging.getLogger(__name__)

# Global cache instance
cache = RedisCache()

# Background task handle
_bg_task: asyncio.Task = None


async def _collect_loop():
    """Background loop that periodically refreshes all collectors."""
    while True:
        try:
            await economy.fetch_and_update(cache)
        except Exception:
            logger.exception("Economy collector failed")

        try:
            await climate.fetch_and_update(cache)
        except Exception:
            logger.exception("Climate collector failed")

        try:
            await housing.fetch_and_update(cache)
        except Exception:
            logger.exception("Housing collector failed")

        try:
            await crime.fetch_and_update(cache)
        except Exception:
            logger.exception("Crime collector failed")

        # Sleep for 5 minutes between collection cycles
        await asyncio.sleep(300)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: connect cache, start collectors, cleanup."""
    global _bg_task

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("CDO Exporter starting up")

    # Connect to Redis
    try:
        await cache.connect()
    except Exception:
        logger.warning("Redis unavailable at startup - collectors will retry")

    # Start background collection
    _bg_task = asyncio.create_task(_collect_loop())
    logger.info("Background collection loop started")

    yield

    # Shutdown
    logger.info("CDO Exporter shutting down")
    if _bg_task:
        _bg_task.cancel()
        try:
            await _bg_task
        except asyncio.CancelledError:
            pass
    await cache.disconnect()


app = FastAPI(
    title="CDO Exporter",
    description="Canadian Data Observatory - Prometheus Metrics Exporter",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "cdo-exporter"}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/metrics/weather")
async def metrics_weather():
    """Weather-specific metrics endpoint (scraped at 15m intervals).

    Placeholder for Phase 1 Part B weather collector.
    """
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST,
    )
