"""
vincular_boletos_btg.py
Busca boletos no BTG, cruza com parcelas pendentes pelo nome + valor
e salva o bankSlipId na observacao da parcela.

Rode com: python vincular_boletos_btg.py
"""

import httpx
import asyncio
import asyncpg
import base64
import json
import os
import unicodedata
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("BTG_CLIENT_ID", "8713b971-78bc-4d64-b8a7-1de325bc9e85")
CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET", "")
DATABASE_URL  = os.getenv("DATABASE_URL", "")
REDIRECT_URI  = "https://painelapi.bancometropolitan.com.br/callback"
ACCOUNT_ID    = os.getenv("BTG_ACCOUNT_ID", "30306294000145-208-50-009886650")

AUTH_HOST = "https://id.sandbox.btgpactual.com"   # troque para id.btgpactual.com em produção
API_HOST  = "https://api.sandbox.empresas.btgpactual.com"  # troque em produção

SCOPES = "openid empresas.btgpactual.com/bank-slips.readonly"


# ─── Helpers ───────────────────────────────────────────────────────────────────

def normalizar(texto: str) -> str:
    """Remove acentos e deixa em maiúsculo para comparação."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def nomes_similares(nome1: str, nome2: str) -> bool:
    """Verifica se dois nomes são iguais (ignorando acentos e maiúsculas)."""
    return normalizar(nome1) == normalizar(nome2)


def valores_proximos(v1, v2, tolerancia=0.05) -> bool:
    """Verifica se dois valores são próximos (tolerância de 5 centavos)."""
    try:
        return abs(float(v1) - float(v2)) <= tolerancia
    except Exception:
        return False


# ─── Auth BTG ──────────────────────────────────────────────────────────────────

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
            print("✅ Token obtido!")
            return resp.json()["access_token"]
        print(f"❌ Erro token ({resp.status_code}): {resp.text[:300]}")
        return None


# ─── Buscar boletos BTG ────────────────────────────────────────────────────────

async def buscar_boletos_btg(token: str) -> list:
    """Busca todos os boletos do BTG (paginado)."""
    boletos = []
    page = 0
    async with httpx.AsyncClient(timeout=20) as client:
        while True:
            resp = await client.get(
                f"{API_HOST}/v1/bank-slips",
                headers={"Authorization": f"Bearer {token}"},
                params={"page": page, "size": 100, "accountId": ACCOUNT_ID},
            )
            if resp.status_code != 200:
                print(f"❌ Erro ao buscar boletos: {resp.status_code}")
                print(resp.text[:300])
                break

            data = resp.json()
            items = (
                data if isinstance(data, list)
                else data.get("items") or data.get("data") or data.get("bankSlips") or []
            )

            if not items:
                break

            boletos.extend(items)

            # Verifica se tem mais páginas
            total = data.get("total") or data.get("totalElements") or len(boletos)
            if len(boletos) >= total or not isinstance(data, dict):
                break
            page += 1

    print(f"📋 {len(boletos)} boleto(s) encontrado(s) no BTG")
    return boletos


# ─── Buscar parcelas pendentes do banco ───────────────────────────────────────

async def buscar_parcelas_pendentes(pool) -> list:
    """Busca parcelas pendentes/atrasadas sem bankSlipId vinculado."""
    rows = await pool.fetch(
        """
        SELECT p.id, p.valor, p.data_vencimento, p.status, p.observacao,
               c.nome, c.cpf_cnpj
        FROM parcelas p
        JOIN contratos ct ON ct.id = p.contrato_id
        JOIN clientes  c  ON c.id  = ct.cliente_id
        WHERE p.status IN ('pendente', 'atrasado')
          AND c.status = 'ativo'
          AND (p.observacao IS NULL OR p.observacao NOT ILIKE '%bankSlipId%')
        ORDER BY p.data_vencimento
        """
    )
    print(f"📊 {len(rows)} parcela(s) pendente(s) sem boleto vinculado")
    return [dict(r) for r in rows]


# ─── Vincular ─────────────────────────────────────────────────────────────────

async def vincular_boletos(pool, boletos: list, parcelas: list):
    """Cruza boletos BTG com parcelas pelo nome + valor e salva o bankSlipId."""

    vinculados   = 0
    nao_encontrados = []

    for boleto in boletos:
        # Ignora boletos já pagos ou cancelados
        status = boleto.get("status", "")
        if status in ("PAID", "CANCELLED", "EXPIRED"):
            continue

        nome_btg  = boleto.get("payer", {}).get("name", "")
        valor_btg = boleto.get("amount", 0)
        slip_id   = boleto.get("bankSlipId") or boleto.get("correlationId") or ""

        if not slip_id or not nome_btg:
            continue

        # Busca parcela correspondente pelo nome + valor
        candidatas = [
            p for p in parcelas
            if nomes_similares(p["nome"], nome_btg)
            and valores_proximos(p["valor"], valor_btg)
            and (not p["observacao"] or "bankSlipId" not in str(p["observacao"]))
        ]

        if not candidatas:
            nao_encontrados.append({
                "nome_btg": nome_btg,
                "valor": valor_btg,
                "slip_id": slip_id,
            })
            continue

        # Se tiver mais de uma candidata, pega a de vencimento mais próximo
        candidata = sorted(candidatas, key=lambda x: x["data_vencimento"])[0]

        # Salva o bankSlipId na observacao
        await pool.execute(
            "UPDATE parcelas SET observacao = $2 WHERE id = $1",
            candidata["id"],
            f"BTG bankSlipId: {slip_id}",
        )

        print(f"  ✅ Parcela {candidata['id']} ({candidata['nome']}) → bankSlipId: {slip_id}")
        vinculados += 1

        # Remove da lista para não vincular duas vezes
        parcelas = [p for p in parcelas if p["id"] != candidata["id"]]

    print(f"\n{'='*50}")
    print(f"✅ {vinculados} parcela(s) vinculada(s) com sucesso!")

    if nao_encontrados:
        print(f"\n⚠️  {len(nao_encontrados)} boleto(s) do BTG sem parcela correspondente:")
        for b in nao_encontrados:
            print(f"   - {b['nome_btg']} | R$ {b['valor']} | id: {b['slip_id']}")

    return vinculados


# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    if not CLIENT_SECRET:
        print("❌ BTG_CLIENT_SECRET não configurado no .env!")
        return

    print("=" * 60)
    print("Acesse esta URL no navegador para fazer login no BTG:")
    print("=" * 60)
    print(f"\n{gerar_login_url()}\n")
    print("Cole o code da URL (antes do '&') abaixo:")
    code = input("Code: ").strip()

    if not code:
        print("❌ Code não informado")
        return

    token = await obter_token(code)
    if not token:
        return

    # Conecta ao banco
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        boletos  = await buscar_boletos_btg(token)
        parcelas = await buscar_parcelas_pendentes(pool)

        if not boletos:
            print("Nenhum boleto encontrado no BTG.")
            return

        if not parcelas:
            print("Nenhuma parcela pendente sem boleto vinculado.")
            return

        print("\n🔗 Vinculando boletos às parcelas...\n")
        await vincular_boletos(pool, boletos, parcelas)

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())