"""Schemas Pydantic para validacao de request/response da API (BETA)."""

from datetime import date
from pydantic import BaseModel


class OrgaoResponse(BaseModel):
    id: int
    codigo: str
    nome: str
    sigla: str | None = None

    model_config = {"from_attributes": True}


class FornecedorResponse(BaseModel):
    id: int
    cpf_cnpj: str
    nome: str
    tipo: str | None = None
    uf: str | None = None
    municipio: str | None = None

    model_config = {"from_attributes": True}


class ContratoResponse(BaseModel):
    id: int
    numero: str | None = None
    objeto: str | None = None
    valor: float
    data_inicio: date | None = None
    data_fim: date | None = None
    modalidade_licitacao: str | None = None
    qtd_concorrentes: int | None = None
    orgao: OrgaoResponse | None = None
    fornecedor: FornecedorResponse | None = None
    fonte: str | None = None

    model_config = {"from_attributes": True}


class ContratoListResponse(BaseModel):
    contratos: list[ContratoResponse]
    total: int
    pagina: int
    total_paginas: int


class DashboardResumo(BaseModel):
    total_contratos: int
    valor_total: float
    total_orgaos: int
    total_fornecedores: int
