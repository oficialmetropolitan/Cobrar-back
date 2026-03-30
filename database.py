import asyncpg
import os
import dotenv

dotenv.load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
)

_pool: asyncpg.Pool = None


async def create_pool():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()


def get_pool() -> asyncpg.Pool:
    return _pool