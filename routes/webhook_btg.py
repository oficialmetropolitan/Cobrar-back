import hmac
import hashlib
import logging
import json
from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Header, Request
from pydantic import BaseModel
from typing import Optional
from database import get_pool
import os

logger = logging.getLogger(__name__)
router = APIRouter()

BTG_WEBHOOK_SECRET = os.getenv("BTG_WEBHOOK_SECRET", "")

EVENTOS_PAGAMENTO_CONFIRMADO = {
    "pix-cash-in.cob.concluida",
    "pix-cash-in.cob.pago",
    "concluida",
    "paga",
    "bank-slips.paid",
}

_logs_recebidos = []


# ─── Assinatura ────────────────────────────────────────────────────────────────

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


# ─── Marcar parcela como paga ──────────────────────────────────────────────────

async def _marcar_parcela_paga(pool, txid: str, valor_recebido, data_pagamento_str: str, origem: str):
    """
    Busca a parcela pelo txid salvo na coluna observacao e marca como paga.
    O txid é o bankSlipId (boleto) ou txid (PIX) salvo quando a cobrança foi criada.
    """
    parcela = await pool.fetchrow(
        """
        SELECT p.id, p.status, p.valor, c.nome
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes  c  ON c.id  = ct.cliente_id
        WHERE p.observacao ILIKE $1
           OR p.observacao = $2
        LIMIT 1
        """,
        f"%{txid}%",
        txid,
    )

    if not parcela:
        logger.warning(f"Parcela não encontrada para txid={txid} (origem: {origem})")
        return {"ok": True, "acao": "parcela_nao_encontrada", "txid": txid}

    if parcela["status"] == "pago":
        logger.info(f"Parcela {parcela['id']} já estava paga — webhook ignorado")
        return {"ok": True, "acao": "ja_pago", "parcela_id": parcela["id"]}

    data_pgto = date.today()
    if data_pagamento_str:
        try:
            data_pgto = datetime.fromisoformat(
                data_pagamento_str.replace("Z", "+00:00")
            ).date()
        except Exception:
            pass

    valor_pago = float(valor_recebido) if valor_recebido else float(parcela["valor"])

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
        f"Pago via {origem} BTG | id: {txid}",
    )

    logger.info(
        f"✅ Parcela {row['id']} marcada como PAGA — "
        f"Cliente: {parcela['nome']} | Valor: R$ {valor_pago:.2f} | "
        f"Origem: {origem} | id: {txid}"
    )

    return {
        "ok": True,
        "acao": "parcela_paga",
        "parcela_id": row["id"],
        "cliente": parcela["nome"],
        "valor_pago": valor_pago,
        "data_pagamento": str(row["data_pagamento"]),
        "origem": origem,
        "txid": txid,
    }


# ─── Inspeção ──────────────────────────────────────────────────────────────────

@router.get("/webhook/btg/inspecionar")
async def webhook_inspecionar_get():
    """Validação da URL pelo painel BTG."""
    return {"ok": True, "status": "webhook ativo"}


@router.post("/webhook/btg/inspecionar")
async def webhook_inspecionar(request: Request):
    """Endpoint temporário para inspecionar payloads reais do BTG."""
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
    """Retorna os últimos eventos recebidos."""
    return {
        "total": len(_logs_recebidos),
        "eventos": list(reversed(_logs_recebidos)),
    }


@router.delete("/webhook/btg/logs")
async def webhook_logs_limpar():
    """Limpa os logs de inspeção."""
    _logs_recebidos.clear()
    return {"ok": True, "mensagem": "Logs limpos"}


# ─── Schemas ───────────────────────────────────────────────────────────────────

class BankSlipPayload(BaseModel):
    """Payload real do BTG para eventos de boleto."""
    bankSlipId:    Optional[str] = None   # ← identificador principal
    correlationId: Optional[str] = None
    ourNumber:     Optional[str] = None
    externalId:    Optional[str] = None
    amount:        Optional[float] = None
    paidAt:        Optional[str] = None
    settledAt:     Optional[str] = None
    status:        Optional[str] = None

class WebhookBoletoPayload(BaseModel):
    event:      str
    bankSlip:   Optional[BankSlipPayload] = None
    data:       Optional[BankSlipPayload] = None
    # BTG às vezes manda os campos direto no root
    bankSlipId: Optional[str] = None
    paidAt:     Optional[str] = None
    amount:     Optional[float] = None

class PixInfo(BaseModel):
    txid:          Optional[str] = None
    correlationId: Optional[str] = None
    endToEndId:    Optional[str] = None
    status:        Optional[str] = None
    valor:         Optional[float] = None
    amount:        Optional[float] = None
    horario:       Optional[str] = None
    dataHora:      Optional[str] = None

class WebhookPixPayload(BaseModel):
    event:         Optional[str] = None
    pix:           Optional[PixInfo] = None
    txid:          Optional[str] = None
    correlationId: Optional[str] = None
    status:        Optional[str] = None
    valor:         Optional[float] = None
    horario:       Optional[str] = None


# ─── Endpoints de produção ─────────────────────────────────────────────────────

@router.post("/webhook/btg/boleto")
async def webhook_boleto(
    payload: WebhookBoletoPayload,
    x_btg_signature: Optional[str] = Header(default=None),
):
    """
    Recebe notificações de boleto pago (bank-slips.paid).

    Exemplo de body para testar no Swagger:
    ```json
    {
      "event": "bank-slips.paid",
      "bankSlip": {
        "bankSlipId": "ID_SALVO_NA_OBSERVACAO_DA_PARCELA",
        "amount": 500.00,
        "paidAt": "2026-04-15T10:00:00Z"
      }
    }
    ```
    """
    if payload.event != "bank-slips.paid":
        return {"ok": True, "acao": "ignorado", "evento": payload.event}

    # Busca o identificador — pode vir em bankSlip ou direto no root
    boleto = payload.bankSlip or payload.data
    txid = ""
    if boleto:
        txid = (
            boleto.bankSlipId or
            boleto.correlationId or
            boleto.ourNumber or
            boleto.externalId or ""
        )
    if not txid:
        txid = payload.bankSlipId or ""

    if not txid:
        raise HTTPException(status_code=422, detail="Identificador ausente no payload do boleto")

    valor = (boleto.amount if boleto else None) or payload.amount
    data_pgto = (boleto.paidAt or boleto.settledAt if boleto else None) or payload.paidAt or ""

    pool = get_pool()
    return await _marcar_parcela_paga(
        pool,
        txid=txid,
        valor_recebido=valor,
        data_pagamento_str=data_pgto,
        origem="Boleto",
    )


@router.post("/webhook/btg/pix")
async def webhook_pix(
    payload: WebhookPixPayload,
    x_btg_signature: Optional[str] = Header(default=None),
):
    """Recebe notificações de PIX Cobrança pago."""
    pix = payload.pix
    evento = (payload.event or (pix.status if pix else "") or "").lower()

    if evento not in EVENTOS_PAGAMENTO_CONFIRMADO:
    
        return {"ok": True, "acao": "ignorado", "evento": evento}

    txid = ""
    if pix:
        txid = pix.txid or pix.correlationId or pix.endToEndId or ""
    if not txid:
        txid = payload.txid or payload.correlationId or ""

    if not txid:
        raise HTTPException(status_code=422, detail="txid ausente no payload PIX")

    pool = get_pool()
    return await _marcar_parcela_paga(
        pool,
        txid=txid,
        valor_recebido=(pix.valor or pix.amount) if pix else payload.valor,
        data_pagamento_str=(pix.horario or pix.dataHora or "") if pix else (payload.horario or ""),
        origem="PIX",
    )