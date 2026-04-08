from pydantic import BaseModel
from typing import Optional
from datetime import date
from decimal import Decimal
from enum import Enum

class StatusCliente(str, Enum):
    ativo = "ativo"
    inativo = "inativo"
    negativo = "negativo"

# ─────────────── Clientes ───────────────

class ClienteCreate(BaseModel):
    nome: str
    modalidade: str
    dia_vencimento: int
    telefone: Optional[str] = None
    email: Optional[str] = None
    cpf_cnpj: Optional[str] = None
    status: StatusCliente = StatusCliente.ativo  # default: ATIVO

class ClienteUpdate(BaseModel):
    nome: Optional[str] = None
    modalidade: Optional[str] = None
    dia_vencimento: Optional[int] = None
    telefone: Optional[str] = None
    email: Optional[str] = None
    cpf_cnpj: Optional[str] = None
    status: Optional[StatusCliente] = None  # era: ativo: Optional[bool]

class ClienteOut(BaseModel):
    id: int
    nome: str
    modalidade: str
    dia_vencimento: int
    telefone: Optional[str]
    email: Optional[str]
    cpf_cnpj: Optional[str]
    status: StatusCliente  # era: ativo: bool

    class Config:
        from_attributes = True

# ─────────────── Contratos ───────────────

class ContratoCreate(BaseModel):
    cliente_id: int
    valor_enviado: Decimal
    montante: Decimal
    spread_total: Optional[Decimal] = None
    num_parcelas: int
    taxa_mensal: Optional[Decimal] = None
    valor_parcela: Decimal
    spread_por_parcela: Optional[Decimal] = None
    data_inicio: Optional[str] = None


class ContratoUpdate(BaseModel):
    valor_enviado: Optional[Decimal] = None
    montante: Optional[Decimal] = None
    spread_total: Optional[Decimal] = None
    num_parcelas: Optional[int] = None
    taxa_mensal: Optional[Decimal] = None
    valor_parcela: Optional[Decimal] = None
    spread_por_parcela: Optional[Decimal] = None
    ativo: Optional[bool] = None
    data_inicio: Optional[date]    = None


class ContratoOut(BaseModel):
    id: int
    cliente_id: int
    valor_enviado: Decimal
    montante: Decimal
    spread_total: Optional[Decimal]
    num_parcelas: int
    taxa_mensal: Optional[Decimal]
    valor_parcela: Decimal
    spread_por_parcela: Optional[Decimal]
    data_inicio: Optional[date]
    ativo: bool

    class Config:
        from_attributes = True


# ─────────────── Parcelas ───────────────

class PagamentoIn(BaseModel):
    """Body para registrar um pagamento."""
    data_pagamento: Optional[date] = None   # default = hoje
    valor_pago: Optional[Decimal] = None    # default = valor da parcela
    observacao: Optional[str] = None


class ParcelaUpdate(BaseModel):
    status: Optional[str] = None
    data_pagamento: Optional[date] = None
    valor_pago: Optional[Decimal] = None
    observacao: Optional[str] = None


class ParcelaOut(BaseModel):
    id: int
    contrato_id: int
    numero_parcela: int
    total_parcelas: int
    mes_referencia: str
    data_vencimento: date
    valor: Decimal
    status: str
    data_pagamento: Optional[date]
    valor_pago: Optional[Decimal]
    observacao: Optional[str]

    class Config:
        from_attributes = True

        