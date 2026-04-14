import hmac
import hashlib
import logging
from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Request, Header
from typing import Optional
from database import get_pool
import os

logger = logging.getLogger(__name__)
router = APIRouter()

BTG_WEBHOOK_SECRET = os.getenv("BTG_WEBHOOK_SECRET", "")

# ─── Eventos que significam "pagamento confirmado" ────────────────────────────
EVENTOS_PAGAMENTO_CONFIRMADO = {
    # PIX Cobrança
    "pix-cash-in.cob.concluida",
    "pix-cash-in.cob.pago",
    "CONCLUIDA",
    "PAGA",
    # Boleto
    "bank-slips.paid",
}


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
    """Localiza a parcela pelo txid e marca como paga. Retorna o resultado."""

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

    # Resolve data de pagamento
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


@router.post("/webhook/btg/pix")
async def webhook_pix(
    request: Request,
    x_btg_signature: Optional[str] = Header(default=None),
):
    """Recebe notificações de PIX Cobrança pago."""
    payload_bytes = await request.body()

    if not _verificar_assinatura(payload_bytes, x_btg_signature):
        logger.warning("Assinatura inválida no webhook PIX")
        raise HTTPException(status_code=401, detail="Assinatura inválida")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    logger.info(f"Webhook PIX BTG: {data}")

    pix = data.get("pix") or data
    evento_raw = data.get("event") or pix.get("status") or ""

    if evento_raw not in EVENTOS_PAGAMENTO_CONFIRMADO and evento_raw.upper() not in {e.upper() for e in EVENTOS_PAGAMENTO_CONFIRMADO}:
        return {"ok": True, "acao": "ignorado", "evento": evento_raw}

    txid = pix.get("txid") or pix.get("correlationId") or pix.get("endToEndId") or ""
    if not txid:
        raise HTTPException(status_code=422, detail="txid ausente no payload PIX")

    pool = get_pool()
    return await _marcar_parcela_paga(
        pool,
        txid=txid,
        valor_recebido=pix.get("valor") or pix.get("amount"),
        data_pagamento_str=pix.get("horario") or pix.get("dataHora") or "",
        origem="PIX",
    )


@router.post("/webhook/btg/boleto")
async def webhook_boleto(
    request: Request,
    x_btg_signature: Optional[str] = Header(default=None),
):
    """Recebe notificações de boleto pago (bank-slips.paid)."""
    payload_bytes = await request.body()

    if not _verificar_assinatura(payload_bytes, x_btg_signature):
        logger.warning("Assinatura inválida no webhook boleto")
        raise HTTPException(status_code=401, detail="Assinatura inválida")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    logger.info(f"Webhook Boleto BTG: {data}")

    evento = data.get("event") or data.get("type") or ""

    # Só processa bank-slips.paid
    if evento != "bank-slips.paid":
        return {"ok": True, "acao": "ignorado", "evento": evento}

    boleto = data.get("bankSlip") or data.get("data") or data
    txid = (
        boleto.get("ourNumber") or      # número do boleto
        boleto.get("externalId") or     # id externo que você define ao criar
        boleto.get("id") or
        boleto.get("correlationId") or
        ""
    )

    if not txid:
        raise HTTPException(status_code=422, detail="Identificador ausente no payload do boleto")

    pool = get_pool()
    return await _marcar_parcela_paga(
        pool,
        txid=txid,
        valor_recebido=boleto.get("amount") or boleto.get("valor"),
        data_pagamento_str=boleto.get("paymentDate") or boleto.get("paidAt") or "",
        origem="Boleto",
    )