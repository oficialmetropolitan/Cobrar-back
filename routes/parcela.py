from fastapi import APIRouter, HTTPException
from typing import Optional, List
from datetime import date
from database import get_pool
from schemas import PagamentoIn, ParcelaUpdate

router = APIRouter()


@router.get("/")
async def listar_parcelas(
    mes_referencia: Optional[str] = None,
    modalidade: Optional[str] = None,
):
    pool = get_pool()
    conds = ["1=1"]
    args = []

    conds.append("p.status IN ('pendente', 'atrasado')")

    if mes_referencia:
        args.append(mes_referencia)
        idx = len(args)

        conds.append(f"""
            p.data_vencimento >= DATE_TRUNC('month', TO_DATE(${idx}, 'YYYY-MM'))
            AND p.data_vencimento < DATE_TRUNC('month', TO_DATE(${idx}, 'YYYY-MM')) + INTERVAL '1 month'
        """)

    if modalidade:
        args.append(modalidade)
        conds.append(f"c.modalidade ILIKE ${len(args)}")

    query = f"""
        SELECT p.*, c.nome AS cliente_nome
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        WHERE {" AND ".join(conds)}
        ORDER BY p.data_vencimento, c.nome
    """

    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]

@router.get("/mes-atual")
async def parcelas_mes_atual():
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT c.nome, c.modalidade, c.telefone,
               p.id AS parcela_id, p.numero_parcela, p.total_parcelas,
               p.data_vencimento, p.valor, p.status, p.data_pagamento
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        WHERE TO_CHAR(p.mes_referencia, 'YYYY-MM') = TO_CHAR(NOW(), 'YYYY-MM')
        ORDER BY p.data_vencimento, c.nome
    """)
    return [dict(r) for r in rows]


@router.get("/atrasadas")
async def parcelas_atrasadas():
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT
            c.id AS cliente_id,
            c.nome,
            c.modalidade,
            c.telefone,
            COUNT(p.id) AS qtd_parcelas,
            SUM(p.valor) AS total_devido,
            MIN(p.data_vencimento) AS primeira_parcela_em_atraso
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        WHERE p.status = 'atrasado'
           OR (p.status = 'pendente' AND p.data_vencimento < CURRENT_DATE)
        GROUP BY c.id, c.nome, c.modalidade, c.telefone
        ORDER BY primeira_parcela_em_atraso ASC, total_devido DESC
    """)
    return [dict(r) for r in rows]


# ✅ ROTAS ESTÁTICAS SEMPRE ANTES DAS DINÂMICAS /{parcela_id}

@router.post("/atualizar-atrasadas")
async def atualizar_atrasadas():
    pool = get_pool()
    result = await pool.execute(
        "UPDATE parcelas SET status='atrasado' WHERE status='pendente' AND data_vencimento < CURRENT_DATE"
    )
    count = int(result.split()[-1])
    return {"mensagem": f"{count} parcelas marcadas como atrasadas"}


@router.post("/baixar-lote")
async def baixar_lote(ids: List[int]):
    """Dá baixa em uma lista específica de parcelas por ID."""
    if not ids:
        raise HTTPException(400, "Nenhum ID informado")

    pool = get_pool()
    rows = await pool.fetch("""
        UPDATE parcelas
        SET status = 'pago',
            data_pagamento = CURRENT_DATE,
            valor_pago = valor,
            observacao = 'Baixa manual em lote'
        WHERE id = ANY($1::int[])
          AND status IN ('pendente', 'atrasado')
        RETURNING id
    """, ids)

    return {
        "mensagem": "Baixa em lote realizada",
        "total_baixado": len(rows),
        "ids": [r["id"] for r in rows]
    }


@router.post("/baixar-consignado")
async def baixar_consignado_lote(mes: str, modalidade: str = "CONSIGNADO"):
    """Dá baixa em todas as parcelas da modalidade para o mês de referência."""
    pool = get_pool()

    query = """
    UPDATE parcelas p
    SET status = 'pago',
        data_pagamento = CURRENT_DATE,
        valor_pago = p.valor,
        observacao = 'Baixa Automática Consignado - ' || $1
    FROM contratos ct
    JOIN clientes c ON c.id = ct.cliente_id
    WHERE p.contrato_id = ct.id
      AND c.modalidade = $2
      AND p.data_vencimento >= DATE_TRUNC('month', TO_DATE($1, 'YYYY-MM'))
      AND p.data_vencimento < DATE_TRUNC('month', TO_DATE($1, 'YYYY-MM')) + INTERVAL '1 month'
      AND p.status IN ('pendente', 'atrasado')
    RETURNING p.id
"""

    rows = await pool.fetch(query, mes, modalidade)

    return {
        "mensagem": f"Processamento concluído para {modalidade}",
        "total_baixado": len(rows)
    }


# ✅ ROTAS DINÂMICAS POR ÚLTIMO

@router.get("/{parcela_id}")
async def buscar_parcela(parcela_id: int):
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM parcelas WHERE id = $1", parcela_id)
    if not row:
        raise HTTPException(404, "Parcela nao encontrada")
    return dict(row)


@router.post("/{parcela_id}/pagar")
async def registrar_pagamento(parcela_id: int, payload: PagamentoIn):
    pool = get_pool()
    parcela = await pool.fetchrow("SELECT id, status, valor FROM parcelas WHERE id = $1", parcela_id)
    if not parcela:
        raise HTTPException(404, "Parcela nao encontrada")
    if parcela["status"] == "cancelado":
        raise HTTPException(400, "Parcela cancelada")
    if parcela["status"] == "pago":
        raise HTTPException(400, "Parcela ja paga")
    data_pgto = payload.data_pagamento or date.today()
    valor_pago = payload.valor_pago or parcela["valor"]
    row = await pool.fetchrow(
        "UPDATE parcelas SET status='pago', data_pagamento=$2, valor_pago=$3, observacao=$4 WHERE id=$1 RETURNING *",
        parcela_id, data_pgto, valor_pago, payload.observacao,
    )
    return {"mensagem": "Pagamento registrado", "parcela": dict(row)}


@router.patch("/{parcela_id}")
async def atualizar_parcela(parcela_id: int, payload: ParcelaUpdate):
    pool = get_pool()
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(400, "Nenhum campo")
    if "status" in data and data["status"] not in ("pendente", "pago", "atrasado", "cancelado"):
        raise HTTPException(400, "Status invalido")
    sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(data.keys()))
    row = await pool.fetchrow(
        f"UPDATE parcelas SET {sets} WHERE id=$1 RETURNING *",
        parcela_id, *list(data.values())
    )
    if not row:
        raise HTTPException(404, "Parcela nao encontrada")
    return dict(row)