"""
monitor_btg.py — Monitor automático de pagamentos BTG
Fica rodando e verifica boletos pagos a cada 60 minutos.
Marca as parcelas como pagas no banco pelo nome + CPF do pagador.

Rode com: python monitor_btg.py
Deixe o terminal aberto.
"""

import httpx
import asyncio
import asyncpg
import base64
import os
import unicodedata
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

# ─── Configuração ──────────────────────────────────────────────────────────────
CLIENT_ID     = os.getenv("BTG_CLIENT_ID", "8713b971-78bc-4d64-b8a7-1de325bc9e85")
CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET", "")
ACCOUNT_ID    = os.getenv("BTG_ACCOUNT_ID", "")
DATABASE_URL  = os.getenv("DATABASE_URL", "")
REDIRECT_URI  = "https://painelapi.bancometropolitan.com.br/callback"

AUTH_HOST         = "https://id.btgpactual.com"
API_HOST          = "https://api.empresas.btgpactual.com"
INTERVALO_MINUTOS = 60

_tokens = {
    "access_token":  os.getenv("BTG_ACCESS_TOKEN", ""),
    "refresh_token": os.getenv("BTG_REFRESH_TOKEN", ""),
}

STATUSES_PAGO = {"PAID", "SETTLED", "LIQUIDATED"}


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _normalizar(texto: str) -> str:
    """Remove acentos e deixa maiúsculo."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(texto))
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def _limpar_cpf(cpf: str) -> str:
    """Remove pontos, traços e espaços do CPF."""
    return "".join(filter(str.isdigit, cpf or ""))


def _log(msg: str):
    print(f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] {msg}")


# ─── Auth BTG ──────────────────────────────────────────────────────────────────

def _gerar_login_url() -> str:
    scopes = "openid empresas.btgpactual.com/bank-slips.readonly"
    return (
        f"{AUTH_HOST}/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={scopes.replace(' ', '%20')}"
        f"&prompt=login"
    )


async def _obter_token_via_code(code: str) -> bool:
    credentials = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{AUTH_HOST}/oauth2/token",
            headers={
                "Content-Type":  "application/x-www-form-urlencoded",
                "Authorization": f"Basic {credentials}",
            },
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": REDIRECT_URI,
            },
        )
        if resp.status_code == 200:
            data = resp.json()
            _tokens["access_token"]  = data["access_token"]
            _tokens["refresh_token"] = data.get("refresh_token", "")
            _log("✅ Login BTG realizado!")
            return True
        _log(f"❌ Erro no login BTG: {resp.text[:200]}")
        return False


async def _renovar_token() -> bool:
    refresh = _tokens.get("refresh_token", "")
    if not refresh:
        return False
    credentials = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{AUTH_HOST}/oauth2/token",
            headers={
                "Content-Type":  "application/x-www-form-urlencoded",
                "Authorization": f"Basic {credentials}",
            },
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh,
                "redirect_uri":  REDIRECT_URI,
            },
        )
        if resp.status_code == 200:
            data = resp.json()
            _tokens["access_token"]  = data["access_token"]
            _tokens["refresh_token"] = data.get("refresh_token", refresh)
            _log("🔄 Token BTG renovado")
            return True
        _tokens["access_token"] = ""
        _log(f"❌ Erro ao renovar token: {resp.text[:150]}")
        return False


async def _get_token() -> str | None:
    token = _tokens.get("access_token", "")
    if token:
        return token
    ok = await _renovar_token()
    return _tokens["access_token"] if ok else None


# ─── API BTG ───────────────────────────────────────────────────────────────────

async def _buscar_boletos_btg() -> list:
    token = await _get_token()
    if not token:
        return []

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(
            f"{API_HOST}/v1/bank-slips",
            headers={"Authorization": f"Bearer {token}"},
            params={"accountId": ACCOUNT_ID, "size": 100},
        )

        if resp.status_code == 401:
            _tokens["access_token"] = ""
            ok = await _renovar_token()
            if not ok:
                return []
            resp = await client.get(
                f"{API_HOST}/v1/bank-slips",
                headers={"Authorization": f"Bearer {_tokens['access_token']}"},
                params={"accountId": ACCOUNT_ID, "size": 100},
            )

        if resp.status_code != 200:
            _log(f"❌ Erro ao buscar boletos: {resp.status_code} — {resp.text[:200]}")
            return []

        data = resp.json()
        return (
            data if isinstance(data, list)
            else data.get("items") or data.get("data") or data.get("bankSlips") or []
        )


# ─── Buscar parcela no banco por nome + CPF ────────────────────────────────────

async def _buscar_parcela(pool, nome: str, cpf: str, valor: float):
    """
    Busca parcela pendente/atrasada pelo nome OU CPF + valor.
    Prioriza CPF se disponível, fallback para nome.
    """
    cpf_limpo = _limpar_cpf(cpf)
    nome_norm = _normalizar(nome)

    # Tenta por CPF primeiro (mais preciso)
    if cpf_limpo:
        row = await pool.fetchrow(
            """
            SELECT p.id, p.status, p.valor, c.nome, c.cpf_cnpj
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
            return dict(row)

    # Fallback por nome
    if nome_norm:
        row = await pool.fetchrow(
            """
            SELECT p.id, p.status, p.valor, c.nome, c.cpf_cnpj
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
            return dict(row)

    return None


# ─── Marcar parcela como paga ──────────────────────────────────────────────────

async def _marcar_pago(pool, parcela_id: int, valor: float, data_pgto: date, slip_id: str):
    await pool.execute(
        """
        UPDATE parcelas
        SET status         = 'pago',
            data_pagamento = $2,
            valor_pago     = $3,
            observacao     = $4
        WHERE id = $1
        """,
        parcela_id,
        data_pgto,
        valor,
        f"Pago via Boleto BTG | id: {slip_id}",
    )


# ─── Ciclo de verificação ──────────────────────────────────────────────────────

async def verificar_pagamentos(pool):
    _log("🔍 Verificando boletos pagos no BTG...")

    boletos = await _buscar_boletos_btg()
    if not boletos:
        _log("Nenhum boleto retornado pelo BTG.")
        return

    pagos = [b for b in boletos if str(b.get("status", "")).upper() in STATUSES_PAGO]
    _log(f"📋 {len(boletos)} boleto(s) total | {len(pagos)} pago(s)")

    if not pagos:
        return

    marcados = 0
    nao_encontrados = []

    for boleto in pagos:
        payer   = boleto.get("payer") or {}
        nome    = payer.get("name", "")
        cpf     = payer.get("taxId") or payer.get("document") or payer.get("cpf") or ""
        valor   = float(boleto.get("amount", 0))
        paid_at = boleto.get("paidAt") or boleto.get("settledAt") or ""
        slip_id = boleto.get("bankSlipId") or boleto.get("correlationId") or ""

        if not valor:
            continue

        parcela = await _buscar_parcela(pool, nome, cpf, valor)

        if not parcela:
            nao_encontrados.append(f"{nome} | CPF: {cpf} | R$ {valor:.2f}")
            continue

        if parcela["status"] == "pago":
            continue

        # Data de pagamento
        data_pgto = date.today()
        if paid_at:
            try:
                data_pgto = date.fromisoformat(paid_at[:10])
            except Exception:
                pass

        await _marcar_pago(pool, parcela["id"], valor, data_pgto, slip_id)
        _log(f"  ✅ PAGO: {parcela['nome']} | CPF: {parcela['cpf_cnpj']} | R$ {valor:.2f}")
        marcados += 1

    if marcados:
        _log(f"🎉 {marcados} parcela(s) marcada(s) como PAGAS!")
    if nao_encontrados:
        _log(f"⚠️  {len(nao_encontrados)} boleto(s) sem parcela correspondente:")
        for item in nao_encontrados:
            _log(f"     → {item}")


# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 55)
    print("   Monitor de Pagamentos BTG")
    print(f"   Verificando a cada {INTERVALO_MINUTOS} minuto(s)")
    print("=" * 55)

    if not CLIENT_SECRET:
        print("❌ BTG_CLIENT_SECRET não configurado no .env!")
        return
    if not ACCOUNT_ID:
        print("❌ BTG_ACCOUNT_ID não configurado no .env!")
        return
    if not DATABASE_URL:
        print("❌ DATABASE_URL não configurado no .env!")
        return

    # Login se não tiver token
    if not _tokens["access_token"] and not _tokens["refresh_token"]:
        print(f"\nAcesse esta URL no navegador:\n\n{_gerar_login_url()}\n")
        code = input("Cole o code aqui (antes do '&'): ").strip()
        ok = await _obter_token_via_code(code)
        if not ok:
            return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    _log("✅ Conectado ao banco. Monitorando... (Ctrl+C para parar)\n")

    try:
        while True:
            await verificar_pagamentos(pool)
            _log(f"⏳ Próxima verificação em {INTERVALO_MINUTOS} minutos...\n")
            await asyncio.sleep(INTERVALO_MINUTOS * 60)
    except KeyboardInterrupt:
        print("\n⛔ Monitor encerrado.")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())