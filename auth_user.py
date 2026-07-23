"""
auth_user.py — Validação do token de login (JWT) do usuário.

A emissão/validação do token é feita pela API de autenticação
(api.bancometropolitan.com.br). Como o segredo do JWT vive lá e não aqui,
validamos o token consultando o endpoint /api/users/me daquela API:
se ele responde 200, o token é válido e recebemos os dados do usuário.

Isso transforma o login (senha + 2FA) na proteção REAL das rotas de dados —
antes o único obstáculo era a x-api-key, que fica exposta no navegador.

Um cache em memória (TTL curto) evita uma chamada HTTP a cada requisição.
"""

import os
import time
import httpx
from fastapi import Depends, Header, HTTPException, status

# Base da API de autentica
# ção. Pode ser sobrescrito no .env se um dia mudar.
AUTH_BASE_URL = os.getenv("AUTH_BASE_URL", "https://api.bancometropolitan.com.br").rstrip("/")
_ME_ENDPOINT = "/api/users/me"

# Cache: token -> (expira_em_monotonic, dados_do_usuario)
_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 60  # segundos


def _limpar_cache_expirado(agora: float) -> None:
    """Remove entradas vencidas quando o cache cresce demais."""
    if len(_cache) <= 500:
        return
    for chave, (expira_em, _) in list(_cache.items()):
        if expira_em <= agora:
            _cache.pop(chave, None)


async def get_current_user(authorization: str | None = Header(default=None)) -> dict:
    """
    Dependência FastAPI: exige um token de login válido no header
    `Authorization: Bearer <token>`. Retorna os dados do usuário autenticado.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticação ausente. Faça login.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticação vazio. Faça login.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    agora = time.monotonic()

    # 1) Cache — evita bater na API de auth a cada requisição
    em_cache = _cache.get(token)
    if em_cache and em_cache[0] > agora:
        return em_cache[1]

    # 2) Valida contra a API de autenticação
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{AUTH_BASE_URL}{_ME_ENDPOINT}",
                headers={"Authorization": f"Bearer {token}"},
            )
    except httpx.RequestError:
        # A API de auth está fora do ar — não é culpa do usuário.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servidor de autenticação temporariamente indisponível.",
        )

    if resp.status_code == 200:
        try:
            user = resp.json()
        except Exception:
            user = {}
        _cache[token] = (agora + _CACHE_TTL, user)
        _limpar_cache_expirado(agora)
        return user

    # Qualquer coisa diferente de 200 => token inválido/expirado
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Sessão inválida ou expirada. Faça login novamente.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _eh_admin(user: dict) -> bool:
    """
    Interpreta o campo `is_admin` do /api/users/me de forma tolerante:
    aceita True, "true", "1", 1 como verdadeiro. Qualquer outra coisa
    (inclusive campo ausente) é tratada como NÃO-admin (fail-safe).
    """
    valor = user.get("is_admin", False)
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, (int, float)):
        return valor == 1
    if isinstance(valor, str):
        return valor.strip().lower() in {"true", "1", "yes", "sim"}
    return False


async def get_current_admin(user: dict = Depends(get_current_user)) -> dict:
    """
    Dependência FastAPI: exige um usuário autenticado E administrador.
    Reaproveita get_current_user (que valida o token) e, além disso,
    confere `is_admin` no retorno de /api/users/me.
    """
    if not _eh_admin(user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acesso restrito a administradores.",
        )
    return user
