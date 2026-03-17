"""PostgreSQL connection helper for CDO Loader."""

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator:
    """Context manager for PostgreSQL connections.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = None
    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Database operation failed")
        raise
    finally:
        if conn:
            conn.close()


@contextmanager
def get_dict_cursor() -> Generator:
    """Context manager that yields a RealDictCursor.

    Usage:
        with get_dict_cursor() as cur:
            cur.execute("SELECT * FROM geo.provinces")
            rows = cur.fetchall()
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
