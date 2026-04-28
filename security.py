from fastapi import Security, HTTPException, status
import os
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_KEY_NAME = "x-api-key"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if not API_KEY:
        # Se a API KEY não for configurada no .env, alerta mas permite ou recusa dependendo da política. 
        # Aqui, como é segurança, vamos exigir que a configuração exista e seja válida.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Configuração de API_KEY ausente no servidor"
        )
        
    if api_key_header == API_KEY:
        return api_key_header
        
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Acesso Negado: Chave de API inválida ou ausente.",
    )
