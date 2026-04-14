import hmac
import hashlib
import logging
from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional, Any, Dict
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


# ─── Schemas para aparecer no Swagger ─────────────────────────────────────────

class BankSlipInfo(BaseModel):
    ourNumber:   Optional[str] = None
    externalId:  Optional[str] = None
    id:          Optional[str] = None
    correlationId: Optional[str] = None
    amount:      Optional[float] = None
    valor:       Optional[float] = None
    paymentDate: Optional[str] = None
    paidAt:      Optional[str] = None

class WebhookBoletoPayload(BaseModel):
    event:    str
    bankSlip: Optional[BankSlipInfo] = None
    data:     Optional[BankSlipInfo] = None

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
    event:  Optional[str] = None
    pix:    Optional[PixInfo] = None
    txid:          Optional[str] = None
    correlationId: Optional[str] = None
    status:        Optional[str] = None
    valor:         Optional[float] = None
    horario:       Optional[str] = None


# ─── Função compartilhada ──────────────────────────────────────────────────────

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


async def _marcar_parcela_paga(pool, txid: str, valor_recebido, data_pagamento_str: str, origem: str):
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
        f"Pago via {origem} BTG | txid: {txid}",
    )

    logger.info(
        f"✅ Parcela {row['id']} marcada como PAGA — "
        f"Cliente: {parcela['nome']} | Valor: R$ {valor_pago:.2f} | "
        f"Origem: {origem} | txid: {txid}"
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


# ─── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/webhook/btg/boleto")
async def webhook_boleto(
    payload: WebhookBoletoPayload,
    x_btg_signature: Optional[str] = Header(default=None),
):
    """
    Recebe notificações de boleto pago (bank-slips.paid).

    Exemplo de body para testar:
    ```json
    {
      "event": "bank-slips.paid",
      "bankSlip": {
        "ourNumber": "SEU_TXID_AQUI",
        "amount": 500.00,
        "paymentDate": "2026-04-14"
      }
    }
    ```
    """
    if payload.event != "bank-slips.paid":
        return {"ok": True, "acao": "ignorado", "evento": payload.event}

    boleto = payload.bankSlip or payload.data
    txid = ""
    if boleto:
        txid = boleto.ourNumber or boleto.externalId or boleto.id or boleto.correlationId or ""

    if not txid:
        raise HTTPException(status_code=422, detail="Identificador ausente no payload do boleto")

    pool = get_pool()
    return await _marcar_parcela_paga(
        pool,
        txid=txid,
        valor_recebido=boleto.amount or boleto.valor if boleto else None,
        data_pagamento_str=boleto.paymentDate or boleto.paidAt or "" if boleto else "",
        origem="Boleto",
    )


@router.post("/webhook/btg/pix")
async def webhook_pix(
    payload: WebhookPixPayload,
    x_btg_signature: Optional[str] = Header(default=None),
):
    """
    Recebe notificações de PIX Cobrança pago.

    Exemplo de body para testar:
    ```json
    {
      "event": "pix-cash-in.cob.concluida",
      "pix": {
        "txid": "SEU_TXID_AQUI",
        "valor": 500.00,
        "horario": "2026-04-14T10:00:00Z"
      }
    }
    ```
    """
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