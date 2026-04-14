import httpx
import logging
from datetime import date, timedelta
from decimal import Decimal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from database import get_pool

logger = logging.getLogger(__name__)

# ── Configurações ────────────────────────────────────────────────
N8N_WEBHOOK_URL = "https://n8n.srv890438.hstgr.cloud/webhook-test/cobrancas" 
DIAS_ANTECEDENCIA = 3

# ── Modo de teste ─────────────────────────────────────────────────
MODO_TESTE = True
TELEFONE_TESTE = "+553588284302"

API_KEY = "sua_chave_aqui"  



# ── Serialização ─────────────────────────────────────────────────

def limpar_parcela(p: dict) -> dict:
    return {
        k: (float(v) if isinstance(v, Decimal) else
            v.isoformat() if isinstance(v, date) else v)
        for k, v in p.items()
    }


def filtrar_e_redirecionar(parcelas: list) -> list:
    resultado_final = []
    digits_teste = ''.join(filter(str.isdigit, TELEFONE_TESTE))[-11:]

    for p in parcelas:
    
        if p.get('telefone_rh'):
            p['telefone'] = p['telefone_rh']
            p['cliente_nome'] = f"[RH] {p['cliente_nome']}"
            p['is_rh'] = True
        
        if not MODO_TESTE:
            resultado_final.append(p)
        else:

            tel_atual = p.get('telefone') or ""
            digits_p = ''.join(filter(str.isdigit, tel_atual))
            if digits_p[-11:] == digits_teste:
                resultado_final.append(p)

    return resultado_final

# ── Queries ──────────────────────────────────────────────────────

async def buscar_parcelas(conn, data_vencimento: date = None, apenas_atrasadas: bool = False):
    """Busca unificada com LEFT JOIN para capturar telefone do RH se existir."""
    query = """
        SELECT
            p.id AS parcela_id, p.numero_parcela, p.total_parcelas,
            p.data_vencimento, p.valor, p.mes_referencia,
            c.id AS cliente_id, c.nome AS cliente_nome, c.telefone, c.modalidade,
            m.telefone_rh
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes c ON c.id = ct.cliente_id
        LEFT JOIN modalidades_config m ON m.modalidade = c.modalidade
        WHERE c.status = 'ativo'
          AND (p.data_ultima_cobranca IS NULL OR p.data_ultima_cobranca < CURRENT_DATE)
    """
    
    params = []
    if apenas_atrasadas:
        query += " AND p.status = 'atrasado' "
    else:
        query += " AND p.status = 'pendente' AND p.data_vencimento = $1 "
        params.append(data_vencimento)

    rows = await conn.fetch(query, *params)
    return [dict(r) for r in rows]

# ── Webhook ───────────────────────────────────────────────────────

async def disparar_webhook(tipo: str, parcelas: list):
    parcelas_finais = filtrar_e_redirecionar(parcelas)
    
    if not parcelas_finais:
        logger.info(f"[{tipo}] Nenhuma parcela para enviar após filtros.")
        return

    payload = {
        "tipo": tipo,
        "modo_teste": MODO_TESTE,
        "data_execucao": date.today().isoformat(),
        "total": len(parcelas_finais),
        "parcelas": [limpar_parcela(p) for p in parcelas_finais],
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(N8N_WEBHOOK_URL, json=payload)
            resp.raise_for_status()
            logger.info(f"[{tipo}] Webhook OK — {len(parcelas_finais)} parcela(s)")
            return True
    except Exception as e:
        logger.error(f"[{tipo}] Falha no webhook: {e}")
        return False

# ── Job principal ─────────────────────────────────────────────────

async def job_cobrancas():
    logger.info(f"=== Job iniciado (Modo Teste: {MODO_TESTE}) ===")
    pool = get_pool()

    async with pool.acquire() as conn:
        # 1. Atualiza vencidas
        await conn.execute("UPDATE parcelas SET status = 'atrasado' WHERE status = 'pendente' AND data_vencimento < CURRENT_DATE")

        # 2. Lembrete Antecipado
        p_lembrete = await buscar_parcelas(conn, date.today() + timedelta(days=DIAS_ANTECEDENCIA))
        await disparar_webhook("lembrete_antecipado", p_lembrete)

        # 3. Vencimento Hoje
        p_hoje = await buscar_parcelas(conn, date.today())
        await disparar_webhook("vencimento_hoje", p_hoje)

        # 4. Atrasadas (Com trava de atualização no banco)
        p_atrasadas = await buscar_parcelas(conn, apenas_atrasadas=True)
        sucesso = await disparar_webhook("cobranca_atrasada", p_atrasadas)
        
        if sucesso and p_atrasadas:
            ids = [p['parcela_id'] for p in filtrar_e_redirecionar(p_atrasadas)]
            if ids:
                await conn.execute("UPDATE parcelas SET data_ultima_cobranca = CURRENT_DATE WHERE id = ANY($1)", ids)
                logger.info(f"Trava aplicada a {len(ids)} parcelas.")

    logger.info("=== Job finalizado ===")

# ── Scheduler ─────────────────────────────────────────────────────

def criar_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(
        job_cobrancas,
        trigger=CronTrigger(hour=8, minute=0), 
        id="job_cobrancas",
        name="Cobranças automáticas diárias",
        replace_existing=True,
    )
    return scheduler