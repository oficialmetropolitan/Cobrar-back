from fastapi import APIRouter, HTTPException
from typing import List, Optional
from database import get_pool
from schemas import ContratoCreate, ContratoUpdate, ContratoOut
from datetime import date
from dateutil.relativedelta import relativedelta

router = APIRouter()

from datetime import date, datetime

def _para_date_puro(valor) -> date:
    if valor is None:
        return date.today()

    # 🔥 trata datetime ANTES (essencial)
    if isinstance(valor, datetime):
        return date(valor.year, valor.month, valor.day)

    # date puro (sem timezone)
    if type(valor) is date:
        return valor

    # string ISO ou fallback
    return date.fromisoformat(str(valor)[:10])

@router.get("/", response_model=List[ContratoOut])
async def listar_contratos(ativo: Optional[bool] = True):
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM contratos WHERE ativo = $1 ORDER BY id", ativo
    )
    return [dict(r) for r in rows]


@router.get("/{contrato_id}", response_model=ContratoOut)
async def buscar_contrato(contrato_id: int):
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM contratos WHERE id = $1", contrato_id)
    if not row:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    return dict(row)


@router.post("/", response_model=ContratoOut, status_code=201)
async def criar_contrato(payload: ContratoCreate):
    pool = get_pool()
    cliente = await pool.fetchrow(
        "SELECT id FROM clientes WHERE id = $1", payload.cliente_id
    )
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")

    row = await pool.fetchrow(
        """
        INSERT INTO contratos
            (cliente_id, valor_enviado, montante, spread_total, num_parcelas,
             taxa_mensal, valor_parcela, spread_por_parcela, data_inicio)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        RETURNING *
        """,
        payload.cliente_id, payload.valor_enviado, payload.montante,
        payload.spread_total, payload.num_parcelas, payload.taxa_mensal,
        payload.valor_parcela, payload.spread_por_parcela, payload.data_inicio,
    )
    return dict(row)


@router.patch("/{contrato_id}")
async def atualizar_contrato(contrato_id: int, payload: ContratoUpdate):
    pool = get_pool()
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

    CAMPOS_QUE_REGENERAM = {"valor_parcela", "num_parcelas", "data_inicio", "spread_por_parcela"}
    deve_regenerar = bool(CAMPOS_QUE_REGENERAM & set(data.keys()))
    parcelas_criadas = 0

    async with pool.acquire() as conn:
        async with conn.transaction():

            sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(data.keys()))
            await conn.execute(
                f"UPDATE contratos SET {sets} WHERE id = $1",
                contrato_id, *list(data.values())
            )

            contrato = await conn.fetchrow("SELECT * FROM contratos WHERE id = $1", contrato_id)
            if not contrato:
                raise HTTPException(status_code=404, detail="Contrato não encontrado")

            cliente = await conn.fetchrow(
                "SELECT dia_vencimento FROM clientes WHERE id = $1", contrato["cliente_id"]
            )
            if not cliente:
                raise HTTPException(status_code=404, detail="Cliente não encontrado")

            dia_vencimento = cliente["dia_vencimento"]

            if deve_regenerar:
                pagas = await conn.fetch(
                    "SELECT numero_parcela FROM parcelas WHERE contrato_id = $1 AND status = 'pago'",
                    contrato_id
                )
                numeros_pagos = {r["numero_parcela"] for r in pagas}

                await conn.execute(
                    "DELETE FROM parcelas WHERE contrato_id = $1 AND status != 'pago'",
                    contrato_id
                )

                # ✅ Garante date puro — elimina o bug do UTC-3
                data_base = _para_date_puro(contrato["data_inicio"])

                for i in range(contrato["num_parcelas"]):
                    numero = i + 1
                    if numero in numeros_pagos:
                        continue

                    vencimento_base = data_base + relativedelta(months=i + 1)
                    # ✅ Constrói date puro com date() — nunca usa .replace() em datetime
                    data_vencimento = date(vencimento_base.year, vencimento_base.month, dia_vencimento)
                    mes_referencia  = data_vencimento.strftime("%Y-%m")
                    status          = "atrasado" if data_vencimento < date.today() else "pendente"

                    await conn.execute(
                        """
                        INSERT INTO parcelas (contrato_id, numero_parcela, total_parcelas,
                            mes_referencia, data_vencimento, valor, status)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        contrato_id, numero, contrato["num_parcelas"],
                        mes_referencia, data_vencimento,
                        contrato["valor_parcela"], status,
                    )
                    parcelas_criadas += 1

                if "num_parcelas" in data and numeros_pagos:
                    await conn.execute(
                        "UPDATE parcelas SET total_parcelas = $1 WHERE contrato_id = $2 AND status = 'pago'",
                        contrato["num_parcelas"], contrato_id
                    )

    return {
        "mensagem": "Contrato atualizado com sucesso!",
        "parcelas_regeneradas": parcelas_criadas,
        "deve_regenerar": deve_regenerar,
    }


@router.delete("/{contrato_id}", status_code=204)
async def desativar_contrato(contrato_id: int):
    pool = get_pool()
    result = await pool.execute(
        "UPDATE contratos SET ativo = FALSE WHERE id = $1", contrato_id
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Contrato não encontrado")


@router.get("/{contrato_id}/parcelas")
async def parcelas_do_contrato(contrato_id: int, status: Optional[str] = None):
    pool = get_pool()
    query = "SELECT * FROM parcelas WHERE contrato_id = $1"
    args = [contrato_id]
    if status:
        args.append(status)
        query += f" AND status = ${len(args)}"
    query += " ORDER BY data_vencimento"
    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]