from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, computed_field
from typing import Optional, List
from datetime import date
from decimal import Decimal

from database import get_pool

router = APIRouter()


# ─────────────── Schemas ───────────────

class AdiantamentoIn(BaseModel):
    nota_fiscal:   str
    valor_enviado: Decimal
    valor_receber: Decimal
    data_enviada:  Optional[date] = None  
    data_receber:  date
    status:        Optional[str] = "pendente"


class AdiantamentoUpdate(BaseModel):
    nota_fiscal:   Optional[str]     = None
    status:        Optional[str]     = None
    valor_enviado: Optional[Decimal] = None
    valor_receber: Optional[Decimal] = None
    data_enviada:  Optional[date]    = None
    data_receber:  Optional[date]    = None


# ─────────────── Rotas ───────────────

@router.get("/")
async def listar_adiantamentos(
    status: Optional[str] = None,
    data_receber_ate: Optional[date] = None,
):
    pool = get_pool()
    conds = ["1=1"]
    args = []

    if status:
        args.append(status)
        conds.append(f"status = ${len(args)}")
    if data_receber_ate:
        args.append(data_receber_ate)
        conds.append(f"data_receber <= ${len(args)}")

    where = " AND ".join(conds)
    rows = await pool.fetch(
        f"SELECT * FROM adiantamentos WHERE {where} ORDER BY data_receber ASC",
        *args
    )
    return [dict(r) for r in rows]


@router.get("/a-receber")
async def adiantamentos_a_receber():
    """Lista todos com status pendente ordenados por data de recebimento."""
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT *,
               (data_receber - CURRENT_DATE) AS dias_restantes
        FROM adiantamentos
        WHERE status = 'pendente'
        ORDER BY data_receber ASC
    """)
    return [dict(r) for r in rows]


@router.get("/resumo")
async def resumo_adiantamentos():
    """Totais agrupados por status."""
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT
            status,
            COUNT(*)                AS quantidade,
            SUM(valor_enviado)      AS total_enviado,
            SUM(valor_receber)      AS total_a_receber,
            SUM(spread)             AS total_spread
        FROM adiantamentos
        GROUP BY status
        ORDER BY status
    """)
    return [dict(r) for r in rows]


@router.get("/{adiantamento_id}")
async def buscar_adiantamento(adiantamento_id: int):
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM adiantamentos WHERE id = $1", adiantamento_id
    )
    if not row:
        raise HTTPException(404, "Adiantamento não encontrado")
    return dict(row)


@router.post("/", status_code=201)
async def criar_adiantamento(payload: AdiantamentoIn):
    pool = get_pool()

    if payload.status not in ("pendente", "recebido", "cancelado"):
        raise HTTPException(400, "Status inválido")
    if payload.valor_receber < payload.valor_enviado:
        raise HTTPException(400, "valor_receber deve ser maior ou igual ao valor_enviado")

    data_enviada = payload.data_enviada or date.today()

    row = await pool.fetchrow(
        """
        INSERT INTO adiantamentos
            (nota_fiscal, status, valor_enviado, valor_receber, data_enviada, data_receber)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
        """,
        payload.nota_fiscal,
        payload.status,
        payload.valor_enviado,
        payload.valor_receber,
        data_enviada,
        payload.data_receber,
    )
    return dict(row)


@router.patch("/{adiantamento_id}")
async def atualizar_adiantamento(adiantamento_id: int, payload: AdiantamentoUpdate):
    pool = get_pool()
    data = payload.model_dump(exclude_none=True)

    if not data:
        raise HTTPException(400, "Nenhum campo para atualizar")
    if "status" in data and data["status"] not in ("pendente", "recebido", "cancelado"):
        raise HTTPException(400, "Status inválido")

    sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(data.keys()))
    row = await pool.fetchrow(
        f"UPDATE adiantamentos SET {sets} WHERE id = $1 RETURNING *",
        adiantamento_id,
        *list(data.values()),
    )
    if not row:
        raise HTTPException(404, "Adiantamento não encontrado")
    return dict(row)


@router.post("/{adiantamento_id}/receber")
async def marcar_como_recebido(adiantamento_id: int):
    """Atalho para marcar um adiantamento como recebido."""
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT id, status FROM adiantamentos WHERE id = $1", adiantamento_id
    )
    if not row:
        raise HTTPException(404, "Adiantamento não encontrado")
    if row["status"] == "recebido":
        raise HTTPException(400, "Adiantamento já recebido")
    if row["status"] == "cancelado":
        raise HTTPException(400, "Adiantamento cancelado")

    updated = await pool.fetchrow(
        "UPDATE adiantamentos SET status = 'recebido' WHERE id = $1 RETURNING *",
        adiantamento_id,
    )
    return {"mensagem": "Marcado como recebido", "adiantamento": dict(updated)}


@router.delete("/{adiantamento_id}", status_code=204)
async def cancelar_adiantamento(adiantamento_id: int):
    """Soft delete — marca como cancelado."""
    pool = get_pool()
    result = await pool.execute(
        "UPDATE adiantamentos SET status = 'cancelado' WHERE id = $1", adiantamento_id
    )
    if result == "UPDATE 0":
        raise HTTPException(404, "Adiantamento não encontrado")