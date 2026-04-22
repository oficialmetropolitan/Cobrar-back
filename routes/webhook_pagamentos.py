"""
monitorar_pagamentos.py
Fica rodando e verifica pagamentos BTG a cada hora.
Rode com: python monitorar_pagamentos.py

Deixa o terminal aberto — ele vai avisar quando encontrar boletos pagos.
"""

import httpx
import asyncio
import asyncpg
import base64
import json
import os
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("BTG_CLIENT_ID", "8713b971-78bc-4d64-b8a7-1de325bc9e85")
CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET", "")
ACCOUNT_ID    = os.getenv("BTG_ACCOUNT_ID", "")
DATABASE_URL  = os.getenv("DATABASE_URL", "")
REDIRECT_URI  = "https://painelapi.bancometropolitan.com.br/callback"

AUTH_HOST = "https://id.btgpactual.com"
API_HOST  = "https://api.empresas.btgpactual.com"

INTERVALO_MINUTOS = 60  # verifica a cada 60 minutos

_tokens = {
    "access_token":  os.getenv("BTG_ACCESS_TOKEN", ""),
    "refresh_token": os.getenv("BTG_REFRESH_TOKEN", ""),
}


# ─── Auth ──────────────────────────────────────────────────────────────────────

def gerar_login_url():
    scopes = "openid empresas.btgpactual.com/bank-slips.readonly"
    return (
        f"{AUTH_HOST}/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={scopes.replace(' ', '%20')}"
        f"&prompt=login"
    )


async def obter_token_via_code(code: str) -> bool:
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
            print("✅ Login realizado com sucesso!")
            return True
        print(f"❌ Erro ao fazer login: {resp.text[:200]}")
        return False


async def renovar_token() -> bool:
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
            print("🔄 Token renovado automaticamente")
            return True
        _tokens["access_token"] = ""
        return False


# ─── Verificar pagamentos ──────────────────────────────────────────────────────

async def verificar_pagamentos(pool):
    token = _tokens.get("access_token", "")
    if not token:
        token_ok = await renovar_token()
        if not token_ok:
            print("❌ Token expirado — precisa fazer login novamente")
            return

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(
            f"{API_HOST}/v1/bank-slips",
            headers={"Authorization": f"Bearer {_tokens['access_token']}"},
            params={"accountId": ACCOUNT_ID, "size": 100},
        )

        # Token expirado
        if resp.status_code == 401:
            renovado = await renovar_token()
            if not renovado:
                print("❌ Sessão expirada — precisa fazer login novamente")
                return
            resp = await client.get(
                f"{API_HOST}/v1/bank-slips",
                headers={"Authorization": f"Bearer {_tokens['access_token']}"},
                params={"accountId": ACCOUNT_ID, "size": 100},
            )

        if resp.status_code != 200:
            print(f"❌ Erro ao buscar boletos: {resp.status_code} — {resp.text[:200]}")
            return

        data = resp.json()
        boletos = (
            data if isinstance(data, list)
            else data.get("items") or data.get("data") or data.get("bankSlips") or []
        )

    # Filtra apenas pagos
    pagos = [
        b for b in boletos
        if str(b.get("status", "")).upper() in {"PAID", "SETTLED", "LIQUIDATED"}
    ]

    if not pagos:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Nenhum boleto pago encontrado.")
        return

    print(f"\n💰 {len(pagos)} boleto(s) pago(s) encontrado(s)!")

    parcelas_marcadas = 0
    for boleto in pagos:
        nome     = boleto.get("payer", {}).get("name", "")
        valor    = float(boleto.get("amount", 0))
        paid_at  = boleto.get("paidAt", "")
        slip_id  = boleto.get("bankSlipId", "")

        if not nome or not valor:
            continue

        # Busca parcela pelo nome + valor
        parcela = await pool.fetchrow(
            """
            SELECT p.id, p.status, p.valor, c.nome
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
            nome.upper(),
            valor,
        )

        if not parcela:
            print(f"  ⚠️  Sem parcela para: {nome} | R$ {valor:.2f}")
            continue

        if parcela["status"] == "pago":
            continue  # já estava pago

        # Data de pagamento
        data_pgto = date.today()
        if paid_at:
            try:
                data_pgto = date.fromisoformat(paid_at[:10])
            except Exception:
                pass

        await pool.execute(
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
            valor,
            f"Pago via Boleto BTG | id: {slip_id}",
        )

        print(f"  ✅ {parcela['nome']} — R$ {valor:.2f} — MARCADO COMO PAGO!")
        parcelas_marcadas += 1

    if parcelas_marcadas:
        print(f"\n🎉 {parcelas_marcadas} parcela(s) marcada(s) como pagas!")
    else:
        print("ℹ️  Boletos pagos encontrados mas parcelas já estavam pagas.")


# ─── Loop principal ────────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("  Monitor de Pagamentos BTG")
    print(f"  Verificando a cada {INTERVALO_MINUTOS} minutos")
    print("=" * 60)

    if not CLIENT_SECRET:
        print("❌ BTG_CLIENT_SECRET não configurado no .env!")
        return

    if not ACCOUNT_ID:
        print("❌ BTG_ACCOUNT_ID não configurado no .env!")
        return

    # Login inicial se não tiver token
    if not _tokens.get("access_token") and not _tokens.get("refresh_token"):
        print("\nPrecisa fazer login no BTG primeiro.")
        print(f"\nAcesse esta URL no navegador:\n\n{gerar_login_url()}\n")
        code = input("Cole o code aqui (antes do '&'): ").strip()
        ok = await obter_token_via_code(code)
        if not ok:
            return

    # Conecta ao banco
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    print(f"\n✅ Monitorando... (Ctrl+C para parar)\n")

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] Verificando pagamentos...")
            await verificar_pagamentos(pool)
            print(f"⏳ Próxima verificação em {INTERVALO_MINUTOS} minutos...")
            await asyncio.sleep(INTERVALO_MINUTOS * 60)
    except KeyboardInterrupt:
        print("\n\n⛔ Monitor encerrado.")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())