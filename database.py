import asyncpg
import os
import logging
import dotenv

dotenv.load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
)

_pool: asyncpg.Pool = None
logger = logging.getLogger(__name__)


async def _ensure_tables(pool: asyncpg.Pool):
    """Cria tabelas auxiliares se não existirem."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS boletos_processados (
                id SERIAL PRIMARY KEY,
                bank_slip_id VARCHAR(255) UNIQUE NOT NULL,
                parcela_id INTEGER,
                valor NUMERIC(12,2),
                nome_pagador VARCHAR(255),
                cpf_pagador VARCHAR(20),
                data_pagamento DATE,
                processado_em TIMESTAMP DEFAULT NOW()
            );
        """)
        logger.info("✅ Tabela boletos_processados verificada/criada")


async def create_pool():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await _ensure_tables(_pool)


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()


def get_pool() -> asyncpg.Pool:
    return _pool