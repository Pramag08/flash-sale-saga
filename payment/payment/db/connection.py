import logging
import time

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

_connection_pool: pool.SimpleConnectionPool | None = None

MAX_RETRIES = 3
INITIAL_BACKOFF_MS = 100


def get_connection_pool(
    database_url: str,
    min_connections: int = 1,
    max_connections: int = 10,
) -> pool.SimpleConnectionPool:
    global _connection_pool

    if _connection_pool is not None:
        return _connection_pool

    for attempt in range(MAX_RETRIES):
        try:
            _connection_pool = pool.SimpleConnectionPool(
                minconn=min_connections,
                maxconn=max_connections,
                dsn=database_url,
            )
            logger.info("Database connection pool created")
            return _connection_pool
        except psycopg2.Error as e:
            if attempt < MAX_RETRIES - 1:
                backoff_ms = INITIAL_BACKOFF_MS * (2**attempt)
                logger.warning(
                    "Failed to connect to database (attempt %d/%d): %s. Retrying in %dms",
                    attempt + 1,
                    MAX_RETRIES,
                    e,
                    backoff_ms,
                )
                time.sleep(backoff_ms / 1000)
            else:
                logger.error("Failed to create connection pool after %d attempts", MAX_RETRIES)
                raise

    raise RuntimeError("Failed to create connection pool")


def get_connection(database_url: str) -> psycopg2.extensions.connection:
    pool_instance = get_connection_pool(database_url)
    return pool_instance.getconn()


def release_connection(conn: psycopg2.extensions.connection, database_url: str) -> None:
    global _connection_pool

    if _connection_pool is not None:
        _connection_pool.putconn(conn)


def close_all_connections() -> None:
    global _connection_pool

    if _connection_pool is not None:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("All database connections closed")


def set_connection_pool(pool_instance: pool.SimpleConnectionPool | None) -> None:
    global _connection_pool
    _connection_pool = pool_instance
