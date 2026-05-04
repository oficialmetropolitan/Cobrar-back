import os

import httpx
import logging
from datetime import date, timedelta
from decimal import Decimal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from database import get_pool

import unicodedata
import base64
logger = logging.getLogger(__name__)





# ── Configurações ────────────────────────────────────────────────
N8N_WEBHOOK_URL = "https://n8n.srv890438.hstgr.cloud/webhook-test/cobrancas" 
DIAS_ANTECEDENCIA = 3

# ── Modo de teste ─────────────────────────────────────────────────
MODO_TESTE = True
TELEFONE_TESTE = "+553588284302"

API_KEY = "sua_chave_aqui"  


BTG_CLIENT_ID     = os.getenv("BTG_CLIENT_ID", "")
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET", "")
BTG_ACCOUNT_ID    = os.getenv("BTG_ACCOUNT_ID", "")
BTG_AUTH_HOST     = "https://id.btgpactual.com"        # produção
BTG_API_HOST      = "https://api.empresas.btgpactual.com"  # produção
BTG_REDIRECT_URI  = "https://painelapi.bancometropolitan.com.br/callback"
N8N_WEBHOOK_PAGAMENTOS = os.getenv("N8N_WEBHOOK_PAGAMENTOS", "")
EMAIL_NOTIFICACAO = os.getenv("EMAIL_NOTIFICACAO", "")
 
 
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



 

# Token em memória (renovado automaticamente via refresh_token)
_btg_tokens = {
    "access_token":  os.getenv("BTG_ACCESS_TOKEN", ""),
    "refresh_token": os.getenv("BTG_REFRESH_TOKEN", ""),
}
 
 
# ─── Auth BTG ──────────────────────────────────────────────────────────────────
 
def _normalizar(texto: str) -> str:
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()
 
 
def _valores_proximos(v1, v2, tolerancia=0.05) -> bool:
    try:
        return abs(float(v1) - float(v2)) <= tolerancia
    except Exception:
        return False
 
 
async def _renovar_token_btg() -> str | None:
    """Renova o access_token usando o refresh_token."""
    refresh = _btg_tokens.get("refresh_token", "")
    if not refresh:
        logger.error("BTG: refresh_token não configurado — faça login manual.")
        return None
 
    credentials = base64.b64encode(f"{BTG_CLIENT_ID}:{BTG_CLIENT_SECRET}".encode()).decode()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{BTG_AUTH_HOST}/oauth2/token",
                headers={
                    "Content-Type":  "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {credentials}",
                },
                data={
                    "grant_type":    "refresh_token",
                    "refresh_token": refresh,
                    "redirect_uri":  BTG_REDIRECT_URI,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                _btg_tokens["access_token"]  = data["access_token"]
                _btg_tokens["refresh_token"] = data.get("refresh_token", refresh)
                logger.info("BTG: token renovado com sucesso")
                return _btg_tokens["access_token"]
            logger.error(f"BTG: erro ao renovar token ({resp.status_code}): {resp.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"BTG: falha ao renovar token: {e}")
        return None
 
 
async def _get_token_btg() -> str | None:
    """Retorna token válido, renovando se necessário."""
    token = _btg_tokens.get("access_token", "")
    if token:
        return token
    return await _renovar_token_btg()
 
 
# ─── API BTG ───────────────────────────────────────────────────────────────────
 
async def _buscar_boletos_btg(token: str) -> list:
    boletos = []
    page = 0
    async with httpx.AsyncClient(timeout=20) as client:
        while True:
            resp = await client.get(
                f"{BTG_API_HOST}/v1/bank-slips",
                headers={"Authorization": f"Bearer {token}"},
                params={"page": page, "size": 100, "accountId": BTG_ACCOUNT_ID},
            )
 
            # Token expirado — tenta renovar uma vez
            if resp.status_code == 401:
                logger.info("BTG: token expirado, renovando...")
                _btg_tokens["access_token"] = ""
                novo_token = await _renovar_token_btg()
                if not novo_token:
                    break
                resp = await client.get(
                    f"{BTG_API_HOST}/v1/bank-slips",
                    headers={"Authorization": f"Bearer {novo_token}"},
                    params={"page": page, "size": 100, "accountId": BTG_ACCOUNT_ID},
                )
 
            if resp.status_code != 200:
                logger.error(f"BTG boletos erro {resp.status_code}: {resp.text[:200]}")
                break
 
            data = resp.json()
            items = (
                data if isinstance(data, list)
                else data.get("items") or data.get("data") or data.get("bankSlips") or []
            )
            if not items:
                break
 
            boletos.extend(items)
            total = data.get("total") or data.get("totalElements") if isinstance(data, dict) else None
            if not total or len(boletos) >= total or not isinstance(data, dict):
                break
            page += 1
 
    return boletos
 
 
# ─── Notificação por email via N8N ─────────────────────────────────────────────
 
async def _notificar_pagamento_email(parcelas_pagas: list):
    """Envia notificação de pagamentos via N8N → email."""
    if not N8N_WEBHOOK_PAGAMENTOS or not parcelas_pagas:
        return
 
    payload = {
        "tipo": "pagamento_confirmado",
        "data_execucao": date.today().isoformat(),
        "total": len(parcelas_pagas),
        "email_destino": EMAIL_NOTIFICACAO,
        "parcelas": [limpar_parcela(p) for p in parcelas_pagas],
    }
 
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(N8N_WEBHOOK_PAGAMENTOS, json=payload)
            resp.raise_for_status()
            logger.info(f"Email notificado: {len(parcelas_pagas)} pagamento(s)")
    except Exception as e:
        logger.error(f"Falha ao notificar pagamentos: {e}")
 
 
# ─── Helpers de matching ───────────────────────────────────────────────────────

def _limpar_cpf(cpf: str) -> str:
    """Remove pontos, traços e espaços do CPF/CNPJ."""
    return "".join(filter(str.isdigit, cpf or ""))


async def _buscar_parcela_por_nome_cpf(conn, nome: str, cpf: str, valor: float):
    """
    Busca parcela pendente/atrasada em 3 etapas:
      1º — CPF/CNPJ do pagador + valor (mais confiável)
      2º — Nome normalizado + valor (fallback)
      3º — Nome com LIKE parcial + valor (fallback mais flexível)
    Retorna dict da parcela ou None.
    """
    cpf_limpo = _limpar_cpf(cpf)
    nome_norm = _normalizar(nome)

    # ── 1º: Busca por CPF/CNPJ (mais preciso) ──
    if cpf_limpo:
        row = await conn.fetchrow(
            """
            SELECT p.id, p.status, p.valor, c.nome, c.telefone, c.cpf_cnpj
            FROM parcelas p
            JOIN contratos ct ON ct.id = p.contrato_id
            JOIN clientes  c  ON c.id  = ct.cliente_id
            WHERE p.status IN ('pendente', 'atrasado')
              AND c.status = 'ativo'
              AND REPLACE(REPLACE(REPLACE(c.cpf_cnpj, '.', ''), '-', ''), '/', '') = $1
              AND ABS(p.valor - $2) <= 0.05
            ORDER BY p.data_vencimento ASC
            LIMIT 1
            """,
            cpf_limpo,
            valor,
        )
        if row:
            logger.info(f"  🔍 Match por CPF: {cpf_limpo}")
            return dict(row)

    # ── 2º: Busca por nome exato normalizado ──
    if nome_norm:
        row = await conn.fetchrow(
            """
            SELECT p.id, p.status, p.valor, c.nome, c.telefone, c.cpf_cnpj
            FROM parcelas p
            JOIN contratos ct ON ct.id = p.contrato_id
            JOIN clientes  c  ON c.id  = ct.cliente_id
            WHERE p.status IN ('pendente', 'atrasado')
              AND c.status = 'ativo'
              AND UPPER(c.nome) = $1
              AND ABS(p.valor - $2) <= 0.05
            ORDER BY p.data_vencimento ASC
            LIMIT 1
            """,
            nome_norm,
            valor,
        )
        if row:
            logger.info(f"  🔍 Match por nome exato: {nome_norm}")
            return dict(row)

    # ── 3º: Busca por nome parcial (LIKE) ──
    if nome_norm and len(nome_norm) >= 5:
        row = await conn.fetchrow(
            """
            SELECT p.id, p.status, p.valor, c.nome, c.telefone, c.cpf_cnpj
            FROM parcelas p
            JOIN contratos ct ON ct.id = p.contrato_id
            JOIN clientes  c  ON c.id  = ct.cliente_id
            WHERE p.status IN ('pendente', 'atrasado')
              AND c.status = 'ativo'
              AND UPPER(c.nome) LIKE '%' || $1 || '%'
              AND ABS(p.valor - $2) <= 0.05
            ORDER BY p.data_vencimento ASC
            LIMIT 1
            """,
            nome_norm,
            valor,
        )
        if row:
            logger.info(f"  🔍 Match por nome parcial: {nome_norm}")
            return dict(row)

    return None


# ─── JOB 1: Verificar boletos pagos (roda a cada 30 min, 24h) ─────────────────
 
async def job_verificar_pagamentos_btg():
    """
    Verifica boletos pagos no BTG e marca as parcelas correspondentes como pagas.
    Busca por: 1) bankSlipId na observação  2) CPF/CNPJ  3) Nome do pagador.
    Usa tabela boletos_processados para não reprocessar boletos já marcados.
    Roda a cada 30 minutos, 24h por dia.
    """
    logger.info("=== BTG: Verificando pagamentos ===")
 
    if not BTG_CLIENT_ID or not BTG_CLIENT_SECRET or not BTG_ACCOUNT_ID:
        logger.warning("BTG: credenciais não configuradas — pulando job")
        return
 
    token = await _get_token_btg()
    if not token:
        return
 
    boletos = await _buscar_boletos_btg(token)
    if not boletos:
        logger.info("BTG: nenhum boleto encontrado")
        return
 
    # Filtra apenas boletos pagos
    boletos_pagos = [
        b for b in boletos
        if str(b.get("status", "")).upper() in {"PAID", "SETTLED", "LIQUIDATED"}
    ]
 
    if not boletos_pagos:
        logger.info(f"BTG: {len(boletos)} boleto(s) verificado(s), nenhum pago")
        return
 
    logger.info(f"BTG: {len(boletos_pagos)} boleto(s) pago(s) encontrado(s)")
 
    pool = get_pool()
    parcelas_pagas = []
    nao_encontrados = []
    ja_processados = 0
 
    for boleto in boletos_pagos:
        slip_id  = boleto.get("bankSlipId") or boleto.get("correlationId") or ""
        paid_at  = boleto.get("paidAt") or boleto.get("settledAt") or ""
        amount   = float(boleto.get("amount", 0))
        payer    = boleto.get("payer") or {}
        nome     = payer.get("name", "")
        cpf      = payer.get("taxId") or payer.get("document") or payer.get("cpf") or ""

        if not amount:
            continue

        # ── Deduplicação: verifica se já processamos esse boleto ──
        if slip_id:
            async with pool.acquire() as conn:
                existe = await conn.fetchval(
                    "SELECT 1 FROM boletos_processados WHERE bank_slip_id = $1",
                    slip_id,
                )
                if existe:
                    ja_processados += 1
                    continue
 
        async with pool.acquire() as conn:
            parcela = None

            # ── Etapa 1: Tenta pelo bankSlipId já vinculado na observação ──
            if slip_id:
                parcela_row = await conn.fetchrow(
                    """
                    SELECT p.id, p.status, p.valor, c.nome, c.telefone, c.cpf_cnpj
                    FROM parcelas p
                    JOIN contratos ct ON ct.id = p.contrato_id
                    JOIN clientes  c  ON c.id  = ct.cliente_id
                    WHERE p.observacao ILIKE $1
                      AND p.status IN ('pendente', 'atrasado')
                    LIMIT 1
                    """,
                    f"%{slip_id}%",
                )
                if parcela_row:
                    parcela = dict(parcela_row)
                    logger.info(f"  🔍 Match por bankSlipId: {slip_id}")

            # ── Etapa 2: Busca por CPF → Nome (se não achou pelo slip_id) ──
            if not parcela:
                parcela = await _buscar_parcela_por_nome_cpf(conn, nome, cpf, amount)

            # ── Não encontrou de jeito nenhum ──
            if not parcela:
                nao_encontrados.append(f"{nome} | CPF: {cpf} | R$ {amount:.2f}")
                continue
 
            if parcela["status"] == "pago":
                # Registra como processado mesmo que já estivesse pago
                if slip_id:
                    try:
                        await conn.execute(
                            """INSERT INTO boletos_processados
                               (bank_slip_id, parcela_id, valor, nome_pagador, cpf_pagador, data_pagamento)
                               VALUES ($1, $2, $3, $4, $5, $6)
                               ON CONFLICT (bank_slip_id) DO NOTHING""",
                            slip_id, parcela["id"], amount, nome, cpf, date.today(),
                        )
                    except Exception:
                        pass
                continue
 
            # Data de pagamento
            data_pgto = date.today()
            if paid_at:
                try:
                    data_pgto = date.fromisoformat(paid_at[:10])
                except Exception:
                    pass
 
            valor_pago = amount if amount else float(parcela["valor"])

            obs = f"Pago via Boleto BTG | bankSlipId: {slip_id}"
            if cpf:
                obs += f" | CPF: {cpf}"
 
            # ── Marcar parcela como PAGA ──
            await conn.execute(
                """
                UPDATE parcelas
                SET status         = 'pago',
                    data_pagamento = $2,
                    valor_pago     = $3,
                    observacao     = $4
                WHERE id = $1
                """,
                parcela["id"],
                data_pgto,
                valor_pago,
                obs,
            )
 
            logger.info(f"✅ Parcela {parcela['id']} ({parcela['nome']}) marcada como PAGA — R$ {valor_pago:.2f}")

            # ── Registrar boleto como processado (deduplicação) ──
            if slip_id:
                try:
                    await conn.execute(
                        """INSERT INTO boletos_processados
                           (bank_slip_id, parcela_id, valor, nome_pagador, cpf_pagador, data_pagamento)
                           VALUES ($1, $2, $3, $4, $5, $6)
                           ON CONFLICT (bank_slip_id) DO NOTHING""",
                        slip_id, parcela["id"], valor_pago, nome, cpf, data_pgto,
                    )
                except Exception as e:
                    logger.warning(f"Erro ao registrar boleto processado: {e}")
 
            parcelas_pagas.append({
                "parcela_id":    parcela["id"],
                "nome":          parcela["nome"],
                "telefone":      parcela.get("telefone", ""),
                "valor":         valor_pago,
                "data_pagamento": str(data_pgto),
            })
 
    if parcelas_pagas:
        await _notificar_pagamento_email(parcelas_pagas)
        logger.info(f"=== BTG: {len(parcelas_pagas)} parcela(s) marcada(s) como PAGA(s)! ===")
    else:
        logger.info("=== BTG: nenhuma parcela nova marcada como paga ===")

    if ja_processados:
        logger.info(f"ℹ️  {ja_processados} boleto(s) já processado(s) anteriormente (pulados)")

    if nao_encontrados:
        logger.warning(f"⚠️  {len(nao_encontrados)} boleto(s) pago(s) sem parcela correspondente:")
        for item in nao_encontrados:
            logger.warning(f"     → {item}")
 
 
# ─── JOB 2: Vincular boletos às parcelas (roda todo dia às 7h) ────────────────
 
async def job_vincular_boletos_btg():
    """
    Busca boletos pendentes no BTG e vincula às parcelas pelo nome + valor.
    Roda todo dia às 7h da manhã.
    """
    logger.info("=== BTG: Vinculando boletos às parcelas ===")
 
    if not BTG_CLIENT_ID or not BTG_CLIENT_SECRET or not BTG_ACCOUNT_ID:
        logger.warning("BTG: credenciais não configuradas — pulando job")
        return
 
    token = await _get_token_btg()
    if not token:
        return
 
    boletos = await _buscar_boletos_btg(token)
    if not boletos:
        return
 
    pool = get_pool()
    async with pool.acquire() as conn:
        parcelas = await conn.fetch(
            """
            SELECT p.id, p.valor, p.data_vencimento, p.observacao, c.nome
            FROM parcelas p
            JOIN contratos ct ON ct.id = p.contrato_id
            JOIN clientes  c  ON c.id  = ct.cliente_id
            WHERE p.status IN ('pendente', 'atrasado')
              AND c.status = 'ativo'
              AND (p.observacao IS NULL OR p.observacao NOT ILIKE '%bankSlipId%')
            ORDER BY p.data_vencimento
            """
        )
        parcelas = [dict(r) for r in parcelas]
 
    vinculados = 0
    for boleto in boletos:
        status = str(boleto.get("status", "")).upper()
        if status in ("CANCELLED", "EXPIRED"):
            continue
 
        nome_btg  = boleto.get("payer", {}).get("name", "")
        valor_btg = boleto.get("amount", 0)
        slip_id   = boleto.get("bankSlipId") or boleto.get("correlationId") or ""
 
        if not slip_id or not nome_btg:
            continue
 
        candidatas = [
            p for p in parcelas
            if _normalizar(p["nome"]) == _normalizar(nome_btg)
            and _valores_proximos(p["valor"], valor_btg)
            and (not p["observacao"] or "bankSlipId" not in str(p["observacao"]))
        ]
 
        if not candidatas:
            continue
 
        candidata = sorted(candidatas, key=lambda x: x["data_vencimento"])[0]
 
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE parcelas SET observacao = $2 WHERE id = $1",
                candidata["id"],
                f"BTG bankSlipId: {slip_id}",
            )
 
        logger.info(f"🔗 Parcela {candidata['id']} ({candidata['nome']}) → {slip_id}")
        vinculados += 1
        parcelas = [p for p in parcelas if p["id"] != candidata["id"]]
 
    logger.info(f"=== BTG: {vinculados} boleto(s) vinculado(s) ===")
 
 

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

    scheduler.add_job(
    job_vincular_boletos_btg,
    trigger=CronTrigger(hour=7, minute=0),  # todo dia 7h
    id="job_vincular_btg",
    replace_existing=True,
)
    scheduler.add_job(
        job_verificar_pagamentos_btg,
        trigger=IntervalTrigger(minutes=30),  # a cada 30 min, 24h
        id="job_pagamentos_btg",
        name="Verificar pagamentos BTG (24h)",
        replace_existing=True,
    )
    return scheduler