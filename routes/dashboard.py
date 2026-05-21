from fastapi import APIRouter, HTTPException
from datetime import date
from dateutil.relativedelta import relativedelta
from database import get_pool

router = APIRouter()


@router.get("/resumo")
async def resumo_geral():
    """
    Visão geral da carteira ativa: clientes, contratos, capital,
    montante recebido, receita esperada, spread, inadimplência e adiantamentos.
    """
    pool = get_pool()

    carteira = await pool.fetchrow("""
    SELECT
        COUNT(DISTINCT c.id)                                        AS total_clientes,
        COUNT(DISTINCT ct.id)                                       AS total_contratos,
        COALESCE(SUM(ct.valor_enviado), 0)                         AS capital_total_emprestado,
        (SELECT COALESCE(SUM(valor_pago), 0)
           FROM parcelas WHERE status = 'pago')                    AS montante_total_recebido,
        COALESCE(SUM(ct.valor_parcela), 0)                         AS receita_mensal_esperada,
        COALESCE(SUM(ct.spread_total), 0)                          AS spread_total_carteira,
        -- Médias de juros ↓
        ROUND(AVG(ct.taxa_mensal)::numeric, 6)                     AS taxa_media_carteira,
        ROUND(MIN(ct.taxa_mensal)::numeric, 6)                     AS taxa_minima,
        ROUND(MAX(ct.taxa_mensal)::numeric, 6)                     AS taxa_maxima
    FROM clientes c
    JOIN contratos ct ON ct.cliente_id = c.id
    WHERE c.status = 'ativo' AND ct.ativo = TRUE
""")
    parcelas_por_status = await pool.fetch("""
        SELECT
            status,
            COUNT(*)        AS qtd,
            COALESCE(SUM(valor), 0) AS total_valor
        FROM parcelas
        GROUP BY status
        ORDER BY status
    """)

    inadimplencia = await pool.fetchrow("""
        SELECT
            COUNT(DISTINCT ct.cliente_id)   AS clientes_inadimplentes,
            COALESCE(SUM(p.valor), 0)       AS total_em_atraso
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE p.status = 'atrasado'
    """)

    adiantamentos_por_status = await pool.fetch("""
        SELECT
            status,
            COUNT(*)                            AS quantidade,
            COALESCE(SUM(valor_enviado), 0)     AS total_enviado,
            COALESCE(SUM(valor_receber), 0)     AS total_a_receber,
            COALESCE(SUM(spread), 0)            AS total_spread
        FROM adiantamentos
        GROUP BY status
        ORDER BY status
    """)

    adiantamentos_totais = await pool.fetchrow("""
        SELECT
            COUNT(*)                            AS quantidade,
            COALESCE(SUM(valor_enviado), 0)     AS total_enviado,
            COALESCE(SUM(valor_receber), 0)     AS total_a_receber,
            COALESCE(SUM(spread), 0)            AS total_spread
        FROM adiantamentos
    """)

    return {
        "carteira": dict(carteira),
        "parcelas_por_status": [dict(r) for r in parcelas_por_status],
        "inadimplencia": dict(inadimplencia),
        "adiantamentos": {
            "por_status": [dict(r) for r in adiantamentos_por_status],
            "totais":     dict(adiantamentos_totais),
        },
    }


@router.get("/por-modalidade")
async def resumo_por_modalidade():
    """
    Capital emprestado, receita mensal, clientes e inadimplência agrupados
    por modalidade. Considera apenas contratos ativos de clientes ativos.
    """
    pool = get_pool()

    rows = await pool.fetch("""
     WITH resumo_contratos AS (
    SELECT 
        c.modalidade,
        COUNT(DISTINCT c.id)                        AS total_clientes,
        SUM(ct.valor_enviado)                       AS capital_emprestado,
        SUM(ct.valor_parcela)                       AS receita_mensal,
        -- Médias de juros por modalidade ↓
        ROUND(AVG(ct.taxa_mensal)::numeric, 6)      AS taxa_media,
        ROUND(MIN(ct.taxa_mensal)::numeric, 6)      AS taxa_minima,
        ROUND(MAX(ct.taxa_mensal)::numeric, 6)      AS taxa_maxima
    FROM clientes c
    JOIN contratos ct ON ct.cliente_id = c.id
    WHERE c.status = 'ativo' 
      AND ct.ativo = TRUE
    GROUP BY c.modalidade
),
resumo_parcelas AS (
    -- Calcula a inadimplência por modalidade
    SELECT 
        c.modalidade,
        COUNT(p.id) FILTER (WHERE p.status = 'atrasado') AS parcelas_atrasadas,
        COALESCE(SUM(p.valor) FILTER (WHERE p.status = 'atrasado'), 0) AS valor_em_atraso
    FROM clientes c
    JOIN contratos ct ON ct.cliente_id = c.id
    LEFT JOIN parcelas p ON p.contrato_id = ct.id
    WHERE c.status = 'ativo' 
      AND ct.ativo = TRUE
    GROUP BY c.modalidade
)
-- Une os resultados de todas as modalidades
SELECT 
    rc.modalidade,
    rc.total_clientes       AS clientes,
    rc.capital_emprestado,
    rc.receita_mensal,
    rc.taxa_media,           -- ← novo
    rc.taxa_minima,          -- ← novo
    rc.taxa_maxima,          -- ← novo
    rp.parcelas_atrasadas,
    rp.valor_em_atraso
FROM resumo_contratos rc
JOIN resumo_parcelas rp ON rc.modalidade = rp.modalidade
ORDER BY rc.capital_emprestado DESC;
    """)

    return [dict(r) for r in rows]


@router.get("/vencimentos-proximos")
async def vencimentos_proximos(dias: int = 7):
    """
    Parcelas pendentes ou atrasadas com vencimento nos próximos N dias.
    Padrão: 7 dias.
    """
    pool = get_pool()

    rows = await pool.fetch("""
        SELECT
            c.nome,
            c.modalidade,
            c.telefone,
            p.id                AS parcela_id,
            p.data_vencimento,
            p.numero_parcela,
            p.total_parcelas,
            p.valor,
            p.status
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c   ON c.id  = ct.cliente_id
        WHERE p.status IN ('pendente', 'atrasado')
          AND p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + $1::int
        ORDER BY p.data_vencimento, c.nome
    """, dias)

    return [dict(r) for r in rows]


@router.get("/previsao-recebimentos")
async def previsao_recebimentos():
    """
    Soma do valor a receber (parcelas pendentes/atrasadas de contratos ativos)
    nos horizontes de 90, 180, 365 e 730 dias a partir de hoje.
    """
    pool = get_pool()

    row = await pool.fetchrow("""
        SELECT
            COALESCE(SUM(p.valor) FILTER (
                WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 90
            ), 0)  AS em_90_dias,
            COALESCE(SUM(p.valor) FILTER (
                WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 180
            ), 0)  AS em_180_dias,
            COALESCE(SUM(p.valor) FILTER (
                WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 365
            ), 0)  AS em_1_ano,
            COALESCE(SUM(p.valor) FILTER (
                WHERE p.data_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + 730
            ), 0)  AS em_2_anos
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes  c  ON c.id  = ct.cliente_id
        WHERE p.status IN ('pendente', 'atrasado')
          AND ct.ativo   = TRUE
          AND c.status   = 'ativo'
    """)

    return dict(row)

@router.get("/evolucao-mensal-por-modalidade")
async def evolucao_mensal_por_modalidade(ano: int = None):
    pool = get_pool()

    rows = await pool.fetch("""
        SELECT
            c.modalidade,
            p.mes_referencia                                                AS mes,
            COALESCE(SUM(p.valor), 0)                                      AS montante_mensal,
            COALESCE(SUM(
                CASE WHEN p.numero_parcela = 1 THEN ct.valor_enviado ELSE 0 END
            ), 0)                                                           AS capital_desembolsado,
            COALESCE(SUM(
                CASE
                    WHEN p.status = 'pago' AND p.valor_pago IS NOT NULL
                    THEN (p.valor_pago / NULLIF(ct.valor_parcela, 0)) * ct.spread_por_parcela
                    ELSE 0
                END
            ), 0)                                                           AS spread_realizado,
            COALESCE(SUM(p.valor) FILTER (WHERE p.status = 'atrasado'), 0) AS inadimplencia_mensal
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes  c  ON c.id  = ct.cliente_id
        WHERE ($1::int IS NULL OR EXTRACT(YEAR FROM p.data_vencimento) = $1)
        GROUP BY c.modalidade, p.mes_referencia
        ORDER BY p.mes_referencia ASC, c.modalidade
    """, ano)

    return [dict(r) for r in rows]


@router.get("/evolucao-mensal")
async def evolucao_mensal(ano: int = None):
    """
    Por mês de referência: montante de parcelas previsto, capital desembolsado
    (considerado na 1ª parcela), spread realizado (apenas parcelas pagas)
    e inadimplência acumulada.
    """
    pool = get_pool()

    rows = await pool.fetch("""
        SELECT
            p.mes_referencia                                            AS mes,
            COALESCE(SUM(p.valor), 0)                                  AS montante_mensal,
            COALESCE(SUM(
                CASE WHEN p.numero_parcela = 1 THEN ct.valor_enviado ELSE 0 END
            ), 0)                                                       AS capital_desembolsado,
            COALESCE(SUM(
                CASE
                    WHEN p.status = 'pago' AND p.valor_pago IS NOT NULL
                    -- Se pagou o valor cheio, spread é o esperado
                    -- Se pagou diferente, spread proporcional ao que entrou
                    THEN (p.valor_pago / NULLIF(ct.valor_parcela, 0)) * ct.spread_por_parcela
                    ELSE 0
                END
            ), 0)                                                       AS spread_realizado,
            COALESCE(SUM(p.valor) FILTER (WHERE p.status = 'atrasado'), 0) AS inadimplencia_mensal
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE ($1::int IS NULL OR EXTRACT(YEAR FROM p.data_vencimento) = $1)
        GROUP BY p.mes_referencia
        ORDER BY p.mes_referencia ASC
    """, ano)

    return [dict(r) for r in rows]


@router.get("/relatorio-consolidado")
async def relatorio_consolidado():
    """
    Relatório de resultado consolidado:
    - Spread realizado nas parcelas pagas
    - Resultado dos adiantamentos (pagos e pendentes)
    - Capital total emprestado e montante recebido de parcelas
    - Totais consolidados (spread geral + recebível total)
    """
    pool = get_pool()

    # 1. Spread gerado pelas parcelas pagas
    spread_parcelas = await pool.fetchrow("""
        SELECT
            COALESCE(SUM(
                p.valor_pago - (ct.valor_enviado / p.total_parcelas)
            ), 0) AS spread_realizado
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE p.status = 'pago' AND p.valor_pago IS NOT NULL
    """)

    # 2. Adiantamentos separados por status
    adiant_rows = await pool.fetch("""
        SELECT
            status,
            COUNT(*)                            AS quantidade,
            COALESCE(SUM(valor_enviado), 0)     AS total_enviado,
            COALESCE(SUM(valor_receber), 0)     AS total_receber,
            COALESCE(SUM(spread), 0)            AS spread_adiantamento
        FROM adiantamentos
        GROUP BY status
    """)

    adiant_pagos    = {"quantidade": 0, "total_enviado": 0.0, "total_receber": 0.0, "spread_adiantamento": 0.0}
    adiant_pendente = {"quantidade": 0, "total_enviado": 0.0, "total_receber": 0.0, "spread_adiantamento": 0.0}

    for r in adiant_rows:
        if r["status"] == "recebido":
            adiant_pagos = dict(r)
        elif r["status"] == "pendente":
            adiant_pendente = dict(r)

    # 3. Capital emprestado e total recebido (parcelas)
    base = await pool.fetchrow("""
        SELECT
            (SELECT COALESCE(SUM(valor_pago), 0)
               FROM parcelas
              WHERE status = 'pago')                AS total_recebido_parcelas,
            (SELECT COALESCE(SUM(valor_enviado), 0)
               FROM contratos
              WHERE ativo = TRUE)                   AS capital_emprestado_carteira
    """)

    # 4. Consolidação
    spread_parcelas_val  = float(spread_parcelas["spread_realizado"])
    spread_adiant_receb  = float(adiant_pagos.get("spread_adiantamento", 0))
    spread_total         = spread_parcelas_val + spread_adiant_receb

    total_recebido_parcelas  = float(base["total_recebido_parcelas"])
    total_recebido_adiant    = float(adiant_pagos.get("total_receber", 0))
    # Total já entrou no caixa: parcelas pagas + adiantamentos recebidos
    total_recebido_geral     = total_recebido_parcelas + total_recebido_adiant
    
    # Total ainda a receber: parcelas pendentes/atrasadas + adiantamentos pendentes
    total_recebivel_parcelas = float(await pool.fetchval("""
        SELECT COALESCE(SUM(valor), 0)
        FROM parcelas
        WHERE status IN ('pendente', 'atrasado')
    """))
    total_recebivel_adiant   = float(adiant_pendente.get("total_receber", 0))
    total_recebivel          = total_recebivel_parcelas + total_recebivel_adiant

    return {
        "detalhes": {
            "spread_realizado_parcelas":   spread_parcelas_val,
            "adiantamentos_recebidos":     adiant_pagos,
            "adiantamentos_pendentes":     adiant_pendente,
            "capital_emprestado_carteira": float(base["capital_emprestado_carteira"]),
            "total_recebido_parcelas":     total_recebido_parcelas,
            "total_recebido_adiantamentos": total_recebido_adiant,
        },
        "consolidado": {
            "spread_total":              spread_total,
            "total_recebido_geral":      total_recebido_geral,
            "total_recebivel":           total_recebivel,
            "total_recebivel_parcelas":  total_recebivel_parcelas,
            "total_recebivel_adiant":    total_recebivel_adiant,
        },
    }


@router.get("/comparacao-periodos")
async def comparacao_periodos(
    mes_base: str = None,
    comparar_com: str = "mes_anterior",
):
    """
    Compara métricas entre dois meses com variação percentual.

    - mes_base: YYYY-MM (padrão: mês atual)
    - comparar_com: "mes_anterior" (padrão) ou "ano_anterior"
    """
    if comparar_com not in ("mes_anterior", "ano_anterior"):
        raise HTTPException(400, "comparar_com deve ser 'mes_anterior' ou 'ano_anterior'")

    hoje = date.today()
    if mes_base:
        try:
            ref = date.fromisoformat(mes_base + "-01")
        except ValueError:
            raise HTTPException(400, "mes_base deve estar no formato YYYY-MM")
    else:
        ref = hoje.replace(day=1)

    mes_base_str = ref.strftime("%Y-%m")
    ref_comp = ref - (relativedelta(years=1) if comparar_com == "ano_anterior" else relativedelta(months=1))
    mes_comp_str = ref_comp.strftime("%Y-%m")

    pool = get_pool()

    row = await pool.fetchrow("""
        SELECT
            -- Montante previsto (soma de todas as parcelas no período)
            COALESCE(SUM(p.valor) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $1), 0)                   AS montante_base,
            COALESCE(SUM(p.valor) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $2), 0)                   AS montante_comp,

            -- Total efetivamente recebido (parcelas pagas)
            COALESCE(SUM(p.valor_pago) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $1
                  AND p.status = 'pago'), 0)                                   AS recebido_base,
            COALESCE(SUM(p.valor_pago) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $2
                  AND p.status = 'pago'), 0)                                   AS recebido_comp,

            -- Spread realizado nas parcelas pagas
            COALESCE(SUM(CASE
                WHEN TRIM(p.mes_referencia::text) = $1
                 AND p.status = 'pago' AND p.valor_pago IS NOT NULL
                THEN (p.valor_pago / NULLIF(ct.valor_parcela, 0)) * ct.spread_por_parcela
                ELSE 0 END), 0)                                                AS spread_base,
            COALESCE(SUM(CASE
                WHEN TRIM(p.mes_referencia::text) = $2
                 AND p.status = 'pago' AND p.valor_pago IS NOT NULL
                THEN (p.valor_pago / NULLIF(ct.valor_parcela, 0)) * ct.spread_por_parcela
                ELSE 0 END), 0)                                                AS spread_comp,

            -- Inadimplência (parcelas atrasadas)
            COALESCE(SUM(p.valor) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $1
                  AND p.status = 'atrasado'), 0)                               AS inadimplencia_base,
            COALESCE(SUM(p.valor) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $2
                  AND p.status = 'atrasado'), 0)                               AS inadimplencia_comp,

            -- Quantidade de parcelas pagas
            COUNT(p.id) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $1
                  AND p.status = 'pago')                                       AS pagas_base,
            COUNT(p.id) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $2
                  AND p.status = 'pago')                                       AS pagas_comp,

            -- Capital desembolsado (novos contratos: 1ª parcela)
            COALESCE(SUM(ct.valor_enviado) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $1
                  AND p.numero_parcela = 1), 0)                                AS capital_base,
            COALESCE(SUM(ct.valor_enviado) FILTER (
                WHERE TRIM(p.mes_referencia::text) = $2
                  AND p.numero_parcela = 1), 0)                                AS capital_comp
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        WHERE TRIM(p.mes_referencia::text) IN ($1, $2)
    """, mes_base_str, mes_comp_str)

    def _variacao(base, comp):
        b, c = float(base), float(comp)
        pct = round(((b - c) / c) * 100, 2) if c != 0 else None
        return {
            "periodo_base":        round(b, 2),
            "periodo_comparacao":  round(c, 2),
            "variacao_absoluta":   round(b - c, 2),
            "variacao_percentual": pct,
        }

    return {
        "periodo_base":       mes_base_str,
        "periodo_comparacao": mes_comp_str,
        "tipo_comparacao":    comparar_com,
        "metricas": {
            "montante_previsto":    _variacao(row["montante_base"],    row["montante_comp"]),
            "total_recebido":       _variacao(row["recebido_base"],    row["recebido_comp"]),
            "spread_realizado":     _variacao(row["spread_base"],      row["spread_comp"]),
            "inadimplencia":        _variacao(row["inadimplencia_base"], row["inadimplencia_comp"]),
            "parcelas_pagas":       _variacao(row["pagas_base"],       row["pagas_comp"]),
            "capital_desembolsado": _variacao(row["capital_base"],     row["capital_comp"]),
        },
    }