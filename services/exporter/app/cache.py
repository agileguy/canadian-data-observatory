"""Redis cache layer for CDO Exporter."""

import json
import logging
from typing import Any, Optional

import redis.asyncio as redis

from app.config import settings

logger = logging.getLogger(__name__)


class RedisCache:
    """Async Redis cache with JSON serialization and TTL support."""

    def __init__(self, url: str = settings.REDIS_URL):
        self._url = url
        self._client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Establish Redis connection."""
        self._client = redis.from_url(
            self._url,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        try:
            await self._client.ping()
            logger.info("Redis connection established: %s", self._url)
        except redis.ConnectionError as exc:
            logger.error("Redis connection failed: %s", exc)
            self._client = None
            raise

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Redis connection closed")

    async def get(self, key: str) -> Optional[Any]:
        """Retrieve a cached value by key. Returns None on miss or error."""
        if not self._client:
            return None
        try:
            raw = await self._client.get(f"cdo:{key}")
            if raw is None:
                return None
            return json.loads(raw)
        except (redis.RedisError, json.JSONDecodeError) as exc:
            logger.warning("Cache get failed for key=%s: %s", key, exc)
            return None

    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Store a value in cache with TTL (seconds)."""
        if not self._client:
            return False
        try:
            serialized = json.dumps(value, default=str)
            await self._client.set(f"cdo:{key}", serialized, ex=ttl)
            return True
        except (redis.RedisError, TypeError) as exc:
            logger.warning("Cache set failed for key=%s: %s", key, exc)
            return False
