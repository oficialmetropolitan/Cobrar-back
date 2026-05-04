import hmac
import hashlib
import logging
import json
import unicodedata
from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Header, Request
from pydantic import BaseModel
from typing import Optional
from database import get_pool
import os

logger = logging.getLogger(__name__)
router = APIRouter()

BTG_WEBHOOK_SECRET = os.getenv("BTG_WEBHOOK_SECRET", "")

EVENTOS_PIX_CONFIRMADO = {
    "pix-cash-in.cob.concluida",
    "pix-cash-in.cob.pago",
    "concluida",
    "paga",
}

_logs_recebidos = []


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _normalizar(texto: str) -> str:
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def _verificar_assinatura(payload_bytes: bytes, assinatura: str) -> bool:
    if not BTG_WEBHOOK_SECRET:
        logger.warning("BTG_WEBHOOK_SECRET não configurado — assinatura ignorada!")
        return True
    expected = hmac.new(
        BTG_WEBHOOK_SECRET.encode(),
        payload_bytes,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, assinatura or "")


def _limpar_cpf(cpf: str) -> str:
    """Remove pontos, traços e espaços do CPF/CNPJ."""
    return "".join(filter(str.isdigit, cpf or ""))


async def _marcar_pago_por_nome_valor(
    pool,
    nome_pagador: str,
    valor_pago: float,
    data_pagamento_str: str,
    origem: str,
    slip_id: str = "",
    cpf_pagador: str = "",
):
    """
    Busca a parcela pendente/atrasada:
      1º — CPF/CNPJ do pagador + valor (mais confiável)
      2º — Nome normalizado + valor (fallback)
    Se houver mais de uma com mesmo valor, pega a de vencimento mais antigo.
    """
    nome_normalizado = _normalizar(nome_pagador)
    cpf_limpo = _limpar_cpf(cpf_pagador)
    parcela = None

    # ── 1º: Busca por CPF/CNPJ (mais preciso) ──
    if cpf_limpo:
        rows = await pool.fetch(
            """
            SELECT p.id, p.valor, p.status, p.data_vencimento, c.nome, c.cpf_cnpj
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
            valor_pago,
        )
        if rows:
            parcela = dict(rows[0])
            logger.info(f"  🔍 Webhook match por CPF: {cpf_limpo}")

    # ── 2º: Busca por nome normalizado (fallback) ──
    if not parcela and nome_normalizado:
        rows = await pool.fetch(
            """
            SELECT p.id, p.valor, p.status, p.data_vencimento, c.nome, c.cpf_cnpj
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
            nome_normalizado,
            valor_pago,
        )
        if rows:
            parcela = dict(rows[0])
            logger.info(f"  🔍 Webhook match por nome: {nome_normalizado}")

    if not parcela:
        logger.warning(
            f"Parcela não encontrada — nome: '{nome_pagador}' | CPF: '{cpf_pagador}' | valor: R$ {valor_pago:.2f}"
        )
        return {
            "ok": True,
            "acao": "parcela_nao_encontrada",
            "nome": nome_pagador,
            "cpf": cpf_pagador,
            "valor": valor_pago,
        }

    # Data de pagamento
    data_pgto = date.today()
    if data_pagamento_str:
        try:
            data_pgto = datetime.fromisoformat(
                data_pagamento_str.replace("Z", "+00:00")
            ).date()
        except Exception:
            pass

    observacao = f"Pago via {origem} BTG"
    if slip_id:
        observacao += f" | id: {slip_id}"
    if cpf_pagador:
        observacao += f" | CPF: {cpf_pagador}"

    row = await pool.fetchrow(
        """
        UPDATE parcelas
        SET status         = 'pago',
            data_pagamento = $2,
            valor_pago     = $3,
            observacao     = $4
        WHERE id = $1
        RETURNING id, status, data_pagamento, valor_pago
        """,
        parcela["id"],
        data_pgto,
        valor_pago,
        observacao,
    )

    logger.info(
        f"✅ Parcela {row['id']} marcada como PAGA — "
        f"Cliente: {parcela['nome']} | Valor: R$ {valor_pago:.2f} | Origem: {origem}"
    )

    return {
        "ok": True,
        "acao": "parcela_paga",
        "parcela_id": row["id"],
        "cliente": parcela["nome"],
        "valor_pago": valor_pago,
        "data_pagamento": str(row["data_pagamento"]),
        "origem": origem,
    }


# ─── Inspeção ──────────────────────────────────────────────────────────────────

@router.get("/webhook/btg/inspecionar")
async def webhook_inspecionar_get():
    return {"ok": True, "status": "webhook ativo"}


@router.post("/webhook/btg/inspecionar")
async def webhook_inspecionar(request: Request):
    """Endpoint para inspecionar payloads reais do BTG."""
    try:
        body_bytes = await request.body()
        body_json = json.loads(body_bytes)
    except Exception:
        body_json = {"erro": "body não era JSON válido"}

    headers_relevantes = {
        k: v for k, v in request.headers.items()
        if k.lower() in {
            "x-btg-signature", "content-type", "user-agent",
            "x-event-type", "x-webhook-id", "x-btg-event"
        }
    }

    entrada = {
        "recebido_em": datetime.now().isoformat(),
        "headers": headers_relevantes,
        "payload": body_json,
    }

    _logs_recebidos.append(entrada)
    if len(_logs_recebidos) > 50:
        _logs_recebidos.pop(0)

    logger.info(f"📥 Webhook BTG: {json.dumps(body_json, ensure_ascii=False)}")
    return {"ok": True, "mensagem": "Payload registrado. Acesse GET /webhook/btg/logs para ver."}


@router.get("/webhook/btg/logs")
async def webhook_logs():
    return {"total": len(_logs_recebidos), "eventos": list(reversed(_logs_recebidos))}


@router.delete("/webhook/btg/logs")
async def webhook_logs_limpar():
    _logs_recebidos.clear()
    return {"ok": True, "mensagem": "Logs limpos"}


# ─── Schemas ───────────────────────────────────────────────────────────────────

class BankSlipPayload(BaseModel):
    bankSlipId:    Optional[str] = None
    correlationId: Optional[str] = None
    ourNumber:     Optional[str] = None
    externalId:    Optional[str] = None
    amount:        Optional[float] = None
    paidAt:        Optional[str] = None
    settledAt:     Optional[str] = None
    status:        Optional[str] = None
    payer:         Optional[dict] = None

class WebhookBoletoPayload(BaseModel):
    event:      str
    bankSlip:   Optional[BankSlipPayload] = None
    data:       Optional[BankSlipPayload] = None
    bankSlipId: Optional[str] = None
    paidAt:     Optional[str] = None
    amount:     Optional[float] = None
    payer:      Optional[dict] = None

class PixInfo(BaseModel):
    txid:          Optional[str] = None
    correlationId: Optional[str] = None
    endToEndId:    Optional[str] = None
    status:        Optional[str] = None
    valor:         Optional[float] = None
    amount:        Optional[float] = None
    horario:       Optional[str] = None
    dataHora:      Optional[str] = None
    pagador:       Optional[dict] = None

class WebhookPixPayload(BaseModel):
    event:         Optional[str] = None
    pix:           Optional[PixInfo] = None
    txid:          Optional[str] = None
    correlationId: Optional[str] = None
    status:        Optional[str] = None
    valor:         Optional[float] = None
    horario:       Optional[str] = None


# ─── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/webhook/btg/boleto")
async def webhook_boleto(
    request: Request,
    payload: WebhookBoletoPayload,
    x_btg_signature: Optional[str] = Header(default=None),
):
    body_bytes = await request.body()
    if not _verificar_assinatura(body_bytes, x_btg_signature):
        logger.warning("Tentativa de webhook BOLETO com assinatura inválida!")
        raise HTTPException(status_code=401, detail="Assinatura inválida")
    """
    Recebe notificações de boleto pago (bank-slips.paid).
    Busca a parcela pelo nome do pagador + valor — sem precisar de bankSlipId.

    Exemplo para testar no Swagger:
    ```json
    {
      "event": "bank-slips.paid",
      "bankSlip": {
        "amount": 500.00,
        "paidAt": "2026-04-16T10:00:00Z",
        "payer": {"name": "Nome Exato Do Cliente"}
      }
    }
    ```
    """
    if payload.event != "bank-slips.paid":
        return {"ok": True, "acao": "ignorado", "evento": payload.event}

    boleto = payload.bankSlip or payload.data
    payer  = (boleto.payer if boleto else None) or payload.payer or {}

    nome_pagador = payer.get("name", "")
    cpf_pagador  = payer.get("taxId") or payer.get("document") or payer.get("cpf") or ""
    valor        = (boleto.amount if boleto else None) or payload.amount or 0
    data_pgto    = (boleto.paidAt or boleto.settledAt if boleto else None) or payload.paidAt or ""
    slip_id      = (boleto.bankSlipId or boleto.correlationId if boleto else None) or payload.bankSlipId or ""

    if not nome_pagador and not cpf_pagador:
        raise HTTPException(status_code=422, detail="Nome ou CPF do pagador ausente no payload")
    if not valor:
        raise HTTPException(status_code=422, detail="Valor ausente no payload")

    pool = get_pool()
    return await _marcar_pago_por_nome_valor(
        pool,
        nome_pagador=nome_pagador,
        valor_pago=float(valor),
        data_pagamento_str=data_pgto,
        origem="Boleto",
        slip_id=slip_id,
        cpf_pagador=cpf_pagador,
    )


@router.post("/webhook/btg/pix")
async def webhook_pix(
    request: Request,
    payload: WebhookPixPayload,
    x_btg_signature: Optional[str] = Header(default=None),
):
    body_bytes = await request.body()
    if not _verificar_assinatura(body_bytes, x_btg_signature):
        logger.warning("Tentativa de webhook PIX com assinatura inválida!")
        raise HTTPException(status_code=401, detail="Assinatura inválida")
    """
    Recebe notificações de PIX Cobrança pago.
    Busca a parcela pelo nome do pagador + valor.

    Exemplo para testar no Swagger:
    ```json
    {
      "event": "pix-cash-in.cob.concluida",
      "pix": {
        "valor": 500.00,
        "horario": "2026-04-16T10:00:00Z",
        "pagador": {"nome": "Nome Exato Do Cliente"}
      }
    }
    ```
    """
    pix    = payload.pix
    evento = (payload.event or (pix.status if pix else "") or "").lower()

    if evento not in EVENTOS_PIX_CONFIRMADO:
        return {"ok": True, "acao": "ignorado", "evento": evento}

    pagador      = (pix.pagador if pix else None) or {}
    nome_pagador = pagador.get("nome") or pagador.get("name") or ""
    cpf_pagador  = pagador.get("cpf") or pagador.get("taxId") or pagador.get("document") or ""
    valor        = (pix.valor or pix.amount if pix else None) or payload.valor or 0
    data_pgto    = (pix.horario or pix.dataHora if pix else None) or payload.horario or ""
    txid         = (pix.txid or pix.correlationId if pix else None) or payload.txid or ""

    if not nome_pagador and not cpf_pagador:
        raise HTTPException(status_code=422, detail="Nome ou CPF do pagador ausente no payload PIX")
    if not valor:
        raise HTTPException(status_code=422, detail="Valor ausente no payload PIX")

    pool = get_pool()
    return await _marcar_pago_por_nome_valor(
        pool,
        nome_pagador=nome_pagador,
        valor_pago=float(valor),
        data_pagamento_str=data_pgto,
        origem="PIX",
        slip_id=txid,
        cpf_pagador=cpf_pagador,
    )