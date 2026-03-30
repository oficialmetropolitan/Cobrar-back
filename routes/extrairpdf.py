"""
Rota FastAPI para extração de dados de CCB via OpenAI.

Adicione ao seu main.py:
    from backend_ccb import router as ccb_router
    app.include_router(ccb_router)

Dependências:
    pip install openai pypdf python-multipart

Variável de ambiente necessária:
    OPENAI_API_KEY=sk-...
"""

import os
import json
import io
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from openai import OpenAI
from pypdf import PdfReader
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/api", tags=["ccb"])


client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


# ─── Schemas ──────────────────────────────────────────────────────────────────

class Parcela(BaseModel):
    numero: int
    vencimento: str
    total: str


class CCBExtractedData(BaseModel):
    nome: Optional[str] = None
    cpf_cnpj: Optional[str] = None
    telefone: Optional[str] = None
    email: Optional[str] = None
    valor_enviado: Optional[float] = None
    montante: Optional[float] = None
    taxa_mensal: Optional[float] = None
    num_parcelas: Optional[int] = None
    dia_vencimento: Optional[int] = None
    data_inicio: Optional[str] = None
    modalidade: Optional[str] = None
    spread_total: Optional[float] = None
    parcelas: Optional[List[Parcela]] = []


# ─── Rota ─────────────────────────────────────────────────────────────────────

@router.post("/extrair-ccb", response_model=CCBExtractedData)
async def extrair_ccb(file: UploadFile = File(...)):
    """
    Recebe um PDF de CCB, extrai o texto e usa o GPT-4o para retornar os dados estruturados.
    """

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Apenas arquivos PDF são aceitos.")

    # Lê e extrai texto do PDF
    content = await file.read()
    try:
        reader = PdfReader(io.BytesIO(content))
        text = ""
        for page in reader.pages:  # máximo 5 páginas
            text += (page.extract_text() or "") + "\n"
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Erro ao ler o PDF: {str(e)}")

    if not text.strip():
        raise HTTPException(status_code=422, detail="Não foi possível extrair texto do PDF.")

    # Chama OpenAI
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            response_format={"type": "json_object"},  # garante resposta em JSON
            messages=[
                {
                    "role": "system",
                    "content": "Você é um assistente especializado em extrair dados de contratos bancários brasileiros (CCB). Retorne SOMENTE JSON válido, sem texto adicional.",
                },
                {
                    "role": "user",
                    "content": f"""Extraia os dados deste contrato CCB e retorne um JSON com exatamente estas chaves (use null para campos não encontrados):

{{
  "nome": "nome completo do emitente",
  "cpf_cnpj": "CPF ou CNPJ somente números",
  "telefone": "telefone somente números ",
  "email": "email",
  "valor_enviado": valor líquido como número decimal ex: 3000.00,
"montante": valor total da parcela multiplicado pela quantidade de parcelas como número decimal ex: se parcela=584.17 e 6x entao 3505.02,
  "taxa_mensal": taxa mensal como número decimal ex: 2.99,
  "num_parcelas": número de parcelas como inteiro ex: 6,
  "data_inicio": "data da primeira parcela formato YYYY-MM-DD ex: 2026-04-05 coloque um mes antes por favor",
  "dia_vencimento": dia de vencimento das parcelas como inteiro ex: 5,
  "modalidade": "WIZZER, PF ou PJ ou outras empresas",
  "spread_total": diferença entre montante e valor líquido como número decimal,
  "parcelas": [{{"numero": 1, "vencimento": "05/04/2026", "total": "584.17"}}]
}}

Texto do contrato:
{text[:6000]}""",
                },
            ],
        )

        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)
        return CCBExtractedData(**data)

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"GPT retornou JSON inválido: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na OpenAI: {str(e)}")