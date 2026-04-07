from schemas import ClienteCreate, ClienteUpdate, ClienteOut

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
from dateutil.relativedelta import relativedelta
from decimal import Decimal


from database import get_pool

router = APIRouter()
from datetime import date, datetime

def _para_date_puro(valor) -> date:
    if valor is None:
        return date.today()

    
    if isinstance(valor, datetime):
        return date(valor.year, valor.month, valor.day)

    if type(valor) is date:
        return valor

    return date.fromisoformat(str(valor)[:10])

@router.get("/", response_model=List[ClienteOut])
async def listar_clientes(
    modalidade: Optional[str] = None,
    ativo: Optional[bool] = None,
    search: Optional[str] = None,
):
    pool = get_pool()
    conditions = ["1=1"]
    args = []

    if ativo is not None:
        args.append(ativo)
        conditions.append(f"ativo = ${len(args)}")
    if modalidade:
        args.append(modalidade)
        conditions.append(f"modalidade = ${len(args)}")
    if search:
        args.append(f"%{search}%")
        conditions.append(f"nome ILIKE ${len(args)}")

    where = " AND ".join(conditions)
    rows = await pool.fetch(
        f"SELECT * FROM clientes WHERE {where} ORDER BY nome", *args
    )
    return [dict(r) for r in rows]


@router.get("/{cliente_id}", response_model=ClienteOut)
async def buscar_cliente(cliente_id: int):
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM clientes WHERE id = $1", cliente_id)
    if not row:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    return dict(row)


from asyncpg.exceptions import UniqueViolationError

@router.post("/", response_model=ClienteOut, status_code=201)
async def criar_cliente(payload: ClienteCreate):
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            """
            INSERT INTO clientes (nome, modalidade, dia_vencimento, telefone, email, cpf_cnpj)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            """,
            payload.nome,
            payload.modalidade,
            payload.dia_vencimento,
            payload.telefone,
            payload.email,
            payload.cpf_cnpj,
        )
        return dict(row)
    except UniqueViolationError:
        raise HTTPException(
            status_code=400,
            detail="Erro de integridade: CPF/CNPJ ou ID já cadastrado."
        )


@router.patch("/{cliente_id}", response_model=ClienteOut)
async def atualizar_cliente(cliente_id: int, payload: ClienteUpdate):
    pool = get_pool()
    data = payload.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

    sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(data.keys()))
    values = list(data.values())
    row = await pool.fetchrow(
        f"UPDATE clientes SET {sets} WHERE id = $1 RETURNING *",
        cliente_id,
        *values,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    return dict(row)


# ── PATCH especializado: troca dia_vencimento e atualiza TODAS as parcelas ──
@router.patch("/{cliente_id}/dia-vencimento")
async def atualizar_dia_vencimento(cliente_id: int, novo_dia: int):
    if not (1 <= novo_dia <= 28):
        raise HTTPException(400, "novo_dia deve estar entre 1 e 28")

    pool = get_pool()
    parcelas_atualizadas = 0

    async with pool.acquire() as conn:
        async with conn.transaction():

            cliente = await conn.fetchrow(
                "SELECT id FROM clientes WHERE id = $1", cliente_id
            )
            if not cliente:
                raise HTTPException(404, "Cliente não encontrado")

            await conn.execute(
                "UPDATE clientes SET dia_vencimento = $1 WHERE id = $2",
                novo_dia, cliente_id
            )

            parcelas = await conn.fetch(
                """
                SELECT p.id, p.data_vencimento
                FROM parcelas p
                JOIN contratos ct ON ct.id = p.contrato_id
                WHERE ct.cliente_id = $1
                  AND p.status IN ('pendente', 'atrasado')
                """,
                cliente_id
            )

            for parcela in parcelas:
                # ✅ Extrai date puro — elimina o bug do UTC-3 que salvava 2 dias antes
                data_atual = _para_date_puro(parcela["data_vencimento"])
                nova_data  = date(data_atual.year, data_atual.month, novo_dia)
                novo_status = "atrasado" if nova_data < date.today() else "pendente"

                await conn.execute(
                    "UPDATE parcelas SET data_vencimento = $1, status = $2 WHERE id = $3",
                    nova_data, novo_status, parcela["id"]
                )
                parcelas_atualizadas += 1

    return {
        "mensagem": f"Dia de vencimento atualizado para {novo_dia}",
        "parcelas_atualizadas": parcelas_atualizadas,
    }


# ── DELETE real: remove cliente + contratos + parcelas em cascata ──
@router.delete("/{cliente_id}", status_code=200)
async def excluir_cliente(cliente_id: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():

            existe = await conn.fetchval(
                "SELECT id FROM clientes WHERE id = $1", cliente_id
            )
            if not existe:
                raise HTTPException(status_code=404, detail="Cliente não encontrado")

            await conn.execute(
                "DELETE FROM parcelas WHERE contrato_id IN (SELECT id FROM contratos WHERE cliente_id = $1)",
                cliente_id,
            )
            await conn.execute("DELETE FROM contratos WHERE cliente_id = $1", cliente_id)
            await conn.execute("DELETE FROM clientes WHERE id = $1", cliente_id)

    return {"mensagem": f"Cliente {cliente_id} e todos os seus dados foram excluídos permanentemente."}


@router.get("/{cliente_id}/contratos")
async def contratos_do_cliente(cliente_id: int):
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM contratos WHERE cliente_id = $1 ORDER BY id", cliente_id
    )
    return [dict(r) for r in rows]


@router.get("/{cliente_id}/parcelas")
async def parcelas_do_cliente(cliente_id: int, status: Optional[str] = None):
    pool = get_pool()
    query = """
        SELECT p.*, c.nome AS cliente_nome, ct.valor_parcela
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        WHERE c.id = $1
    """
    args = [cliente_id]
    if status:
        args.append(status)
        query += f" AND p.status = ${len(args)}"
    query += " ORDER BY p.data_vencimento"

    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]


# ─────────────── Schema de entrada unificado ───────────────

class OnboardingIn(BaseModel):
    nome: str
    modalidade: str
    dia_vencimento: int
    telefone: Optional[str] = None
    email: Optional[str] = None
    cpf_cnpj: Optional[str] = None
    valor_enviado: Decimal
    montante: Decimal
    spread_total: Optional[Decimal] = None
    num_parcelas: int
    taxa_mensal: Optional[Decimal] = None
    valor_parcela: Decimal
    spread_por_parcela: Optional[Decimal] = None
    data_inicio: Optional[date] = None


@router.post("/onboarding", status_code=201)
async def onboarding(payload: OnboardingIn):
    pool = get_pool()

    if not (1 <= payload.dia_vencimento <= 28):
        raise HTTPException(400, "dia_vencimento deve estar entre 1 e 28")
    if payload.num_parcelas < 1:
        raise HTTPException(400, "num_parcelas deve ser >= 1")

    data_inicio = payload.data_inicio or date.today()

    async with pool.acquire() as conn:
        async with conn.transaction():

            await conn.execute("SELECT setval(pg_get_serial_sequence('clientes', 'id'), coalesce(max(id), 0) + 1, false) FROM clientes;")
            await conn.execute("SELECT setval(pg_get_serial_sequence('contratos', 'id'), coalesce(max(id), 0) + 1, false) FROM contratos;")
            await conn.execute("SELECT setval(pg_get_serial_sequence('parcelas', 'id'), coalesce(max(id), 0) + 1, false) FROM parcelas;")

            cliente = await conn.fetchrow(
                """
                INSERT INTO clientes (nome, modalidade, dia_vencimento, telefone, email, cpf_cnpj)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
                """,
                payload.nome, payload.modalidade, payload.dia_vencimento,
                payload.telefone, payload.email, payload.cpf_cnpj,
            )

            contrato = await conn.fetchrow(
                """
                INSERT INTO contratos
                    (cliente_id, valor_enviado, montante, spread_total, num_parcelas,
                     taxa_mensal, valor_parcela, spread_por_parcela, data_inicio)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                RETURNING *
                """,
                cliente["id"], payload.valor_enviado, payload.montante,
                payload.spread_total, payload.num_parcelas, payload.taxa_mensal,
                payload.valor_parcela, payload.spread_por_parcela, data_inicio,
            )

            parcelas = []
            for i in range(payload.num_parcelas):
                vencimento_base = data_inicio + relativedelta(months=i + 1)
                # ✅ Constrói date puro — sem timezone
                data_vencimento = date(vencimento_base.year, vencimento_base.month, payload.dia_vencimento)
                mes_referencia  = data_vencimento.strftime("%Y-%m")

                parcela = await conn.fetchrow(
                    """
                    INSERT INTO parcelas
                        (contrato_id, numero_parcela, total_parcelas,
                         mes_referencia, data_vencimento, valor, status)
                    VALUES ($1, $2, $3, $4, $5, $6, 'pendente')
                    RETURNING *
                    """,
                    contrato["id"], i + 1, payload.num_parcelas,
                    mes_referencia, data_vencimento, payload.valor_parcela,
                )
                parcelas.append(dict(parcela))

    return {
        "mensagem": "Cadastro realizado com sucesso",
        "cliente": dict(cliente),
        "contrato": dict(contrato),
        "parcelas_geradas": len(parcelas),
        "parcelas": parcelas,
    }