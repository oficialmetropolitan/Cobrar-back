from fastapi import APIRouter
from database import get_pool

router = APIRouter()


@router.get("/resumo")
async def resumo_geral():
    pool = get_pool()

    total = await pool.fetchrow("""
    SELECT
        COUNT(DISTINCT c.id) AS total_clientes,
        COUNT(DISTINCT ct.id) AS total_contratos,
        SUM(ct.valor_enviado) AS capital_total_emprestado,
        (SELECT COALESCE(SUM(valor_pago), 0) FROM parcelas WHERE status = 'pago') AS montante_total_recebido,
        COALESCE(SUM(ct.valor_parcela), 0) AS receita_mensal_esperada,
        COALESCE(SUM(ct.spread_total), 0) AS spread_total_carteira
    FROM clientes c
    JOIN contratos ct ON ct.cliente_id = c.id
    WHERE c.status = 'ativo' AND ct.ativo = TRUE;
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


@router.get("/previsao-recebimentos")
async def previsao_recebimentos():
    """Soma do valor a receber (pendente/atrasado) nos próximos 90, 180, 1 ano e 2 anos."""
    pool = get_pool()
    row = await pool.fetchrow("""
        SELECT 
            COALESCE(SUM(p.valor) FILTER (WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 90), 0) AS em_90_dias,
            COALESCE(SUM(p.valor) FILTER (WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 180), 0) AS em_180_dias,
            COALESCE(SUM(p.valor) FILTER (WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 365), 0) AS em_1_ano,
            COALESCE(SUM(p.valor) FILTER (WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 730), 0) AS em_2_anos
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        WHERE p.status IN ('pendente', 'atrasado') 
          AND ct.ativo = TRUE
          AND c.status = 'ativo'
    """)
    return dict(row)


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

@router.get("/relatorio-consolidado")
async def relatorio_consolidado():
    """
    Retorna o Spread Pago consolidado (parcelas + adiantamentos),
    separa as categorias e apresenta a soma do Adiantamento com o Resto do relatório.
    """
    pool = get_pool()

    # 1. Spread pago nas parcelas
    spread_parcelas = await pool.fetchrow("""
        SELECT 
            COALESCE(SUM(p.valor_pago - (ct.valor_enviado / p.total_parcelas)), 0) AS spread_pago
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE p.status = 'pago' AND p.valor_pago IS NOT NULL
    """)

    # 2. Resumo de Adiantamentos (pagos e pendentes)
    adiantamentos_rows = await pool.fetch("""
        SELECT 
            status,
            COUNT(*) AS quantidade,
            COALESCE(SUM(valor_enviado), 0) AS total_enviado,
            COALESCE(SUM(valor_receber), 0) AS total_receber,
            COALESCE(SUM(spread), 0) AS spread_adiantamento
        FROM adiantamentos
        GROUP BY status
    """)

    adiantamentos_pagos = {"quantidade": 0, "total_enviado": 0, "total_receber": 0, "spread_adiantamento": 0}
    adiantamentos_pendentes = {"quantidade": 0, "total_enviado": 0, "total_receber": 0, "spread_adiantamento": 0}

    for r in adiantamentos_rows:
        if r["status"] == "pago":
            adiantamentos_pagos = dict(r)
        elif r["status"] == "pendente":
            adiantamentos_pendentes = dict(r)

    # 3. Resto do relatório (Capital total emprestado e Montante Recebido de Parcelas)
    resto = await pool.fetchrow("""
        SELECT
            (SELECT COALESCE(SUM(valor_pago), 0) FROM parcelas WHERE status = 'pago') AS total_recebido_parcelas,
            (SELECT COALESCE(SUM(valor_enviado), 0) FROM contratos WHERE ativo = TRUE) AS capital_emprestado_carteira
    """)

    # --- Lógica de Consolidação ---
    
    spread_pago_parcelas_val = float(spread_parcelas["spread_pago"])
    spread_pago_adiantamentos_val = float(adiantamentos_pagos.get("spread_adiantamento", 0))

    soma_spread_total = spread_pago_parcelas_val + spread_pago_adiantamentos_val
    
    total_receber_adiantamentos = float(adiantamentos_pendentes.get("total_receber", 0)) + float(adiantamentos_pagos.get("total_receber", 0))

    return {
        "categorias_separadas": {
            "spread_pago_parcelas": spread_pago_parcelas_val,
            "adiantamentos_pagos": adiantamentos_pagos,
            "adiantamentos_pendentes": adiantamentos_pendentes,
            "resto_relatorio": dict(resto)
        },
        "visualizacao_geral_consolidada": {
            "soma_spread_pago_total": soma_spread_total,
            "soma_adiantamentos_com_resto": total_receber_adiantamentos + float(resto["total_recebido_parcelas"])
        }
    }