from fastapi import APIRouter
from database import get_pool

router = APIRouter()


@router.get("/resumo")
async def resumo_geral():
    pool = get_pool()

    total = await pool.fetchrow("""
        SELECT
            COUNT(DISTINCT c.id)           AS total_clientes,
            COUNT(DISTINCT ct.id)          AS total_contratos,
            SUM(ct.valor_enviado)          AS capital_total_emprestado,
            SUM(ct.montante)               AS montante_total_carteira,
            COALESCE(SUM(ct.valor_parcela), 0)     AS receita_mensal_esperada,
            COALESCE(SUM(ct.spread_total), 0)      AS spread_total_carteira
        FROM clientes c
        JOIN contratos ct ON ct.cliente_id = c.id
        WHERE c.ativo = TRUE AND ct.ativo = TRUE
    """)


    status = await pool.fetch("""
        SELECT status, COUNT(*) AS qtd, SUM(valor) AS total_valor
        FROM parcelas
        GROUP BY status ORDER BY status
    """)


    inadimplencia = await pool.fetchrow("""
        SELECT 
            COUNT(DISTINCT ct.cliente_id) AS clientes_inadimplentes,
            COALESCE(SUM(p.valor), 0) AS total_em_atraso
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE p.status = 'atrasado'
    """)


    adiantamentos = await pool.fetch("""
        SELECT
            status,
            COUNT(*)                AS quantidade,
            COALESCE(SUM(valor_enviado), 0)      AS total_enviado,
            COALESCE(SUM(valor_receber), 0)      AS total_a_receber,
            COALESCE(SUM(spread), 0)             AS total_spread
        FROM adiantamentos
        GROUP BY status
        ORDER BY status
    """)

    adiantamento_total = await pool.fetchrow("""
        SELECT
            COALESCE(SUM(valor_enviado), 0) AS total_enviado,
            COALESCE(SUM(valor_receber), 0) AS total_a_receber,
            COALESCE(SUM(spread), 0) AS total_spread
        FROM adiantamentos
    """)

    return {
        "carteira": dict(total),
        "parcelas_por_status": [dict(r) for r in status],
        "inadimplencia": dict(inadimplencia),

     
        "adiantamentos": {
            "por_status": [dict(r) for r in adiantamentos],
            "totais": dict(adiantamento_total)
        }
    }

@router.get("/por-modalidade")
async def resumo_por_modalidade():
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT
            c.modalidade,
            COUNT(DISTINCT c.id)     AS clientes,
            SUM(ct.valor_enviado)    AS capital_emprestado,
            SUM(ct.valor_parcela)    AS receita_mensal,
            COUNT(p.id) FILTER (WHERE p.status = 'atrasado') AS parcelas_atrasadas,
            SUM(p.valor) FILTER (WHERE p.status = 'atrasado') AS valor_em_atraso
        FROM clientes c
        JOIN contratos ct ON ct.cliente_id = c.id
        JOIN parcelas p ON p.contrato_id = ct.id
        GROUP BY c.modalidade
        ORDER BY receita_mensal DESC
    """)
    return [dict(r) for r in rows]


@router.get("/vencimentos-proximos")
async def vencimentos_proximos(dias: int = 7):
    """Parcelas pendentes/atrasadas vencendo nos próximos N dias."""
    pool = get_pool()
    rows = await pool.fetch("""
        SELECT c.nome, c.modalidade, c.telefone,
               p.id AS parcela_id, p.data_vencimento,
               p.numero_parcela, p.total_parcelas, p.valor, p.status
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        WHERE p.status IN ('pendente', 'atrasado')
          AND p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + $1::int
        ORDER BY p.data_vencimento, c.nome
    """, dias)
    return [dict(r) for r in rows]


@router.get("/evolucao-mensal")
async def evolucao_mensal(ano: int = None):
    pool = get_pool()
    
    query = """
        SELECT 
            p.mes_referencia AS mes,
            COALESCE(SUM(p.valor), 0) AS montante_mensal,
            -- Soma o valor enviado apenas quando for a parcela número 1
            COALESCE(SUM(
                CASE WHEN p.numero_parcela = 1 THEN ct.valor_enviado ELSE 0 END
            ), 0) AS valor_enviado_mensal,
COALESCE(
    SUM(
        CASE 
            WHEN p.status = 'pago' AND p.valor_pago IS NOT NULL THEN 
                (p.valor_pago - (ct.valor_enviado / p.total_parcelas))
            ELSE 0 
        END
    ), 
0) AS spread_mensal,
            COALESCE(SUM(p.valor) FILTER (WHERE p.status = 'atrasado'), 0) AS inadimplencia_mensal
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE ($1::int IS NULL OR EXTRACT(YEAR FROM p.data_vencimento) = $1)
        GROUP BY p.mes_referencia
        ORDER BY p.mes_referencia ASC

    """
    
    rows = await pool.fetch(query, ano)
    return [dict(r) for r in rows]