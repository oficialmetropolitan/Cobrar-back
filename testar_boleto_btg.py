"""
testar_boleto_btg.py
Rode com: python testar_boleto_btg.py

Instale dependências: pip install httpx python-dotenv
"""

import httpx
import asyncio
import json
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Credenciais ───────────────────────────────────────────────────────────────
CLIENT_ID     = os.getenv("BTG_CLIENT_ID", "8713b971-78bc-4d64-b8a7-1de325bc9e85")
CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET", "")

# URLs reais BTG (mesmo no Sandbox, usa as URLs de produção)
AUTH_URL   = "https://api.empresas.btgpactual.com/oauth2/token"
BOLETO_URL = "https://api.empresas.btgpactual.com/v1/bank-slips"


async def obter_token():
    print("🔑 Obtendo token OAuth...")
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            AUTH_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope":         "empresas.btgpactual.com/pix-cash-in",
            },
        )
        print(f"Status auth: {resp.status_code}")
        if resp.status_code != 200:
            print(f"❌ Erro ao obter token:")
            print(resp.text[:500])
            return None
        token = resp.json()["access_token"]
        print("✅ Token obtido!")
        return token


async def criar_boleto_teste(token: str):
    print("\n📄 Criando boleto de teste...")

    payload = {
        "externalId": "parcela-teste-001",
        "amount": 10.00,
        "dueDate": "2026-04-30",
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
            BOLETO_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
        )

        print(f"Status: {resp.status_code}")
        print("\nResposta completa do BTG:")
        try:
            print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
        except Exception:
            print(resp.text[:1000])

        if resp.status_code in (200, 201):
            data = resp.json()
            print("\n✅ Boleto criado! Campos importantes:")
            for campo in ["id", "ourNumber", "externalId", "status", "barCode", "digitableLine"]:
                if campo in data:
                    print(f"  {campo}: {data[campo]}")
            print("\n💡 Acesse os logs do webhook em alguns segundos:")
            print("   GET https://painelapi.bancometropolitan.com.br/webhook/btg/logs")


async def main():
    if not CLIENT_SECRET:
        print("❌ BTG_CLIENT_SECRET não configurado no .env!")
        print("   Adicione: BTG_CLIENT_SECRET=seu_secret_aqui")
        return

    token = await obter_token()
    if token:
        await criar_boleto_teste(token)


if __name__ == "__main__":
    asyncio.run(main())