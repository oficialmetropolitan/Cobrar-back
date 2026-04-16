"""
auth_btg.py — Autenticação BTG via Authorization Code
Rode com: python auth_btg.py

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

# URLs BTG
AUTH_HOST  = "https://id.btgpactual.com"
API_HOST   = "https://api.empresas.btgpactual.com"

SCOPES = "openid empresas.btgpactual.com/pix-cash-in empresas.btgpactual.com/pix-cash-in.readonly"


def gerar_login_url():
    """Gera a URL que o usuário precisa acessar para autorizar."""
    url = (
        f"{AUTH_HOST}/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES.replace(' ', '%20')}"
        f"&prompt=login"
    )
    return url


async def trocar_code_por_token(authorization_code: str):
    """Troca o authorization_code pelo access_token."""

    # Basic Auth: base64(client_id:client_secret)
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
                "code":         authorization_code,
                "redirect_uri": REDIRECT_URI,
            },
        )

        print(f"Status token: {resp.status_code}")
        try:
            data = resp.json()
            print(json.dumps(data, indent=2))
        except Exception:
            print(resp.text[:500])
            return None

        if resp.status_code == 200:
            # Salva os tokens no .env para usar depois
            access_token  = data.get("access_token")
            refresh_token = data.get("refresh_token")
            print("\n✅ Tokens obtidos!")
            print(f"\nAdicione no seu .env:")
            print(f"BTG_ACCESS_TOKEN={access_token}")
            print(f"BTG_REFRESH_TOKEN={refresh_token}")
            return access_token
        return None


async def criar_boleto_teste(access_token: str):
    """Cria um boleto de teste para ver o payload do webhook."""
    print("\n📄 Criando boleto de teste...")

    payload = {
        "externalId": "parcela-teste-001",
        "amount": 10.00,
        "dueDate": "2026-05-30",
        "payer": {
            "name": "Cliente Teste",
            "document": "12345678909",
            "documentType": "CPF",
            "address": {
                "street": "Rua Teste",
                "number": "123",
                "neighborhood": "Centro",
                "city": "São Paulo",
                "state": "SP",
                "zipCode": "01310100",
            }
        },
        "description": "Parcela de teste - Metropolitan",
    }

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            f"{API_HOST}/v1/bank-slips",
            json=payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type":  "application/json",
            },
        )

        print(f"Status: {resp.status_code}")
        try:
            data = resp.json()
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception:
            print(resp.text[:1000])


async def main():
    if not CLIENT_SECRET:
        print("❌ BTG_CLIENT_SECRET não configurado no .env!")
        return

    # Verifica se já tem um access_token salvo
    access_token = os.getenv("BTG_ACCESS_TOKEN", "")

    if not access_token:
        print("=" * 60)
        print("PASSO 1 — Faça login no BTG para autorizar o app")
        print("=" * 60)
        print("\nAcesse esta URL no navegador:")
        print(f"\n{gerar_login_url()}\n")
        print("Após o login, o BTG vai redirecionar para:")
        print(f"{REDIRECT_URI}?code=XXXXXXXX")
        print("\nCopie o valor do 'code' da URL e cole abaixo:")
        code = input("Authorization code: ").strip()

        if not code:
            print("❌ Código não informado")
            return

        access_token = await trocar_code_por_token(code)
        if not access_token:
            return

    # Com o token, cria o boleto de teste
    await criar_boleto_teste(access_token)


if __name__ == "__main__":
    asyncio.run(main())