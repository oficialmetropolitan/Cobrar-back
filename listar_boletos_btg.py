"""
listar_boletos_btg.py — Lista boletos recebidos via API BTG
Rode com: python listar_boletos_btg.py

Instale dependências: pip install httpx python-dotenv
"""

import httpx
import asyncio
import base64
import json
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("BTG_CLIENT_ID", "8713b971-78bc-4d64-b8a7-1de325bc9e85")
CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET", "")
REDIRECT_URI  = "https://painelapi.bancometropolitan.com.br/callback"

AUTH_HOST = "https://id.btgpactual.com"
API_HOST  = "https://api.empresas.btgpactual.com"

SCOPES = "openid empresas.btgpactual.com/bank-slips.readonly empresas.btgpactual.com/accounts.readonly"


def gerar_login_url():
    return (
        f"{AUTH_HOST}/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES.replace(' ', '%20')}"
        f"&prompt=login"
    )


async def obter_token(code: str) -> str | None:
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
            print("✅ Token obtido!")
            print(f"\n📋 Salve no .env:")
            print(f"BTG_ACCESS_TOKEN={data['access_token']}")
            print(f"BTG_REFRESH_TOKEN={data.get('refresh_token', '')}\n")
            return data["access_token"]
        print(f"❌ Erro ao obter token ({resp.status_code}):")
        print(resp.text[:500])
        return None


async def obter_account_id(token: str) -> str | None:
    """Busca o accountId da conta automaticamente."""
    print("🔍 Buscando accountId da conta...")
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{API_HOST}/v1/accounts",
            headers={"Authorization": f"Bearer {token}"},
        )
        print(f"Status accounts: {resp.status_code}")
        try:
            data = resp.json()
        except Exception:
            print(resp.text[:500])
            return None

        # Tenta extrair o accountId de diferentes estruturas
        if isinstance(data, list) and data:
            account_id = data[0].get("id") or data[0].get("accountId")
        elif isinstance(data, dict):
            accounts = data.get("accounts") or data.get("data") or data.get("items") or []
            if accounts:
                account_id = accounts[0].get("id") or accounts[0].get("accountId")
            else:
                account_id = data.get("id") or data.get("accountId")
        else:
            account_id = None

        if account_id:
            print(f"✅ AccountId encontrado: {account_id}")
        else:
            print("⚠️ AccountId não encontrado, estrutura retornada:")
            print(json.dumps(data, indent=2, ensure_ascii=False))

        return account_id


async def listar_boletos(token: str, account_id: str):
    print(f"\n📋 Buscando boletos da conta {account_id}...\n")
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(
            f"{API_HOST}/v1/bank-slips",
            headers={"Authorization": f"Bearer {token}"},
            params={"accountId": account_id},
        )

        print(f"Status: {resp.status_code}")
        try:
            data = resp.json()
        except Exception:
            print(resp.text[:1000])
            return

        boletos = (
            data if isinstance(data, list)
            else data.get("items") or data.get("data") or data.get("bankSlips") or []
        )

        if not boletos:
            print("Nenhum boleto encontrado ou estrutura diferente:")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return

        print(f"Total: {len(boletos)} boleto(s)\n")
        for b in boletos[:10]:
            print("-" * 50)
            print(f"  id:         {b.get('id')}")
            print(f"  ourNumber:  {b.get('ourNumber')}")
            print(f"  externalId: {b.get('externalId')}")
            print(f"  status:     {b.get('status')}")
            print(f"  amount:     {b.get('amount')}")
            print(f"  dueDate:    {b.get('dueDate')}")
            print(f"  payer:      {b.get('payer', {}).get('name') if isinstance(b.get('payer'), dict) else b.get('payer')}")

        print("\n\n📦 Payload completo do primeiro boleto:")
        print(json.dumps(boletos[0], indent=2, ensure_ascii=False))


async def main():
    if not CLIENT_SECRET:
        print("❌ BTG_CLIENT_SECRET não configurado no .env!")
        return

    print("=" * 60)
    print("Acesse esta URL no navegador para fazer login no BTG:")
    print("=" * 60)
    print(f"\n{gerar_login_url()}\n")
    print("Após o login, o BTG redireciona para:")
    print(f"{REDIRECT_URI}?code=XXXXXXXX\n")
    print("Copie APENAS o valor do 'code' da URL (antes do '&') e cole abaixo:")

    code = input("Cole o code aqui: ").strip()
    if not code:
        print("❌ Code não informado")
        return

    token = await obter_token(code)
    if not token:
        return

    account_id = await obter_account_id(token)
    if not account_id:
        # Tenta pedir manualmente
        account_id = input("\nNão consegui obter automaticamente. Cole o accountId manualmente: ").strip()
        if not account_id:
            return

    await listar_boletos(token, account_id)


if __name__ == "__main__":
    asyncio.run(main())