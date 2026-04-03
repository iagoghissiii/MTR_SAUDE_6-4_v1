"""Endpoints para consulta de contratos (BETA - apenas dados)."""

import math
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
from src.database.postgres import get_db, Contrato, Orgao, Fornecedor
from src.api.schemas import (
    ContratoResponse, ContratoListResponse, DashboardResumo,
)

router = APIRouter(prefix="/contratos", tags=["contratos"])


@router.get("/", response_model=ContratoListResponse)
async def listar_contratos(
    valor_min: float | None = None,
    valor_max: float | None = None,
    ordem: str = Query("desc", pattern="^(asc|desc)$"),
    pagina: int = Query(1, ge=1),
    limite: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Lista contratos com filtros."""
    query = db.query(Contrato).options(
        joinedload(Contrato.orgao),
        joinedload(Contrato.fornecedor),
    )

    if valor_min is not None:
        query = query.filter(Contrato.valor >= valor_min)
    if valor_max is not None:
        query = query.filter(Contrato.valor <= valor_max)

    if ordem == "asc":
        query = query.order_by(Contrato.data_inicio.asc().nulls_last())
    else:
        query = query.order_by(Contrato.data_inicio.desc().nulls_last())

    total = query.count()
    total_paginas = math.ceil(total / limite) if total > 0 else 1

    offset = (pagina - 1) * limite
    contratos = query.offset(offset).limit(limite).all()

    return ContratoListResponse(
        contratos=[ContratoResponse.model_validate(c) for c in contratos],
        total=total,
        pagina=pagina,
        total_paginas=total_paginas,
    )


@router.get("/dashboard", response_model=DashboardResumo)
async def dashboard_resumo(db: Session = Depends(get_db)):
    """Retorna resumo estatistico para o dashboard."""
    total_contratos = db.query(func.count(Contrato.id)).scalar() or 0
    valor_total = db.query(func.sum(Contrato.valor)).scalar() or 0.0
    total_orgaos = db.query(func.count(func.distinct(Contrato.orgao_id))).scalar() or 0
    total_fornecedores = db.query(func.count(func.distinct(Contrato.fornecedor_id))).scalar() or 0

    return DashboardResumo(
        total_contratos=total_contratos,
        valor_total=float(valor_total),
        total_orgaos=total_orgaos,
        total_fornecedores=total_fornecedores,
    )


@router.get("/{contrato_id}", response_model=ContratoResponse)
async def detalhe_contrato(contrato_id: int, db: Session = Depends(get_db)):
    """Retorna detalhes de um contrato especifico."""
    contrato = db.query(Contrato).options(
        joinedload(Contrato.orgao),
        joinedload(Contrato.fornecedor),
    ).filter(Contrato.id == contrato_id).first()

    if not contrato:
        raise HTTPException(status_code=404, detail="Contrato nao encontrado")

    return ContratoResponse.model_validate(contrato)
