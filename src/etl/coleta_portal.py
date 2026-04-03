"""
Script de pre-processamento e coleta de dados do Portal da Transparencia.
Coleta contratos publicos dos orgaos de saude (MS, FIOCRUZ, ANVISA, ANS, FUNASA)
no periodo de 2022 a 2026 e persiste no banco de dados local.

Uso:
    python -m src.etl.coleta_portal
    python -m src.etl.coleta_portal --ano 2024
    python -m src.etl.coleta_portal --orgao 36000
"""

import os
import time
import logging
import argparse
from datetime import date, datetime
from pathlib import Path

import requests
from sqlalchemy.orm import Session
from dotenv import load_dotenv

load_dotenv()

from src.database.postgres import SessionLocal, create_tables, Orgao, Fornecedor, Contrato

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Configuracao ────────────────────────────────────────────────────────────
API_KEY      = os.getenv("PORTAL_API_KEY", "")
BASE_URL     = "https://api.portaldatransparencia.gov.br/api-de-dados"
PAGE_SIZE    = 500
ANO_INICIO   = 2022
ANO_FIM      = 2026

# Codigos SIAFI dos orgaos monitorados
ORGAOS = {
    "36000": {"nome": "Ministerio da Saude",                        "sigla": "MS"},
    "36201": {"nome": "Fundacao Oswaldo Cruz",                       "sigla": "FIOCRUZ"},
    "36206": {"nome": "Agencia Nacional de Vigilancia Sanitaria",    "sigla": "ANVISA"},
    "36216": {"nome": "Agencia Nacional de Saude Suplementar",       "sigla": "ANS"},
    "36205": {"nome": "Fundacao Nacional de Saude",                  "sigla": "FUNASA"},
}


# ── Helpers ─────────────────────────────────────────────────────────────────
def _headers() -> dict:
    return {"chave-api-dados": API_KEY, "Accept": "application/json"}


def _parse_date(valor: str | None) -> date | None:
    if not valor:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(valor[:10], fmt).date()
        except ValueError:
            continue
    return None


def _get_pagina(codigo_orgao: str, data_ini: date, data_fim: date, pagina: int) -> list:
    params = {
        "codigoOrgao":  codigo_orgao,
        "dataInicio":   data_ini.strftime("%d/%m/%Y"),
        "dataFim":      data_fim.strftime("%d/%m/%Y"),
        "tamanhoPagina": PAGE_SIZE,
        "pagina":       pagina,
    }
    try:
        resp = requests.get(f"{BASE_URL}/contratos", headers=_headers(), params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.warning("HTTP %s na pagina %d", resp.status_code, pagina)
    except requests.RequestException as e:
        logger.warning("Erro de rede: %s", e)
    return []


def _upsert_orgao(db: Session, codigo: str, nome: str, sigla: str) -> Orgao:
    orgao = db.query(Orgao).filter_by(codigo=codigo).first()
    if not orgao:
        orgao = Orgao(codigo=codigo, nome=nome, sigla=sigla)
        db.add(orgao)
        db.flush()
    return orgao


def _upsert_fornecedor(db: Session, raw: dict) -> Fornecedor | None:
    cpf_cnpj = (raw.get("cnpj") or raw.get("cpf") or raw.get("cpfCnpj") or "").strip()
    if not cpf_cnpj:
        return None
    f = db.query(Fornecedor).filter_by(cpf_cnpj=cpf_cnpj).first()
    if not f:
        nome = raw.get("nome") or raw.get("razaoSocial") or "DESCONHECIDO"
        tipo = "PJ" if len(cpf_cnpj.replace(".", "").replace("/", "").replace("-", "")) == 14 else "PF"
        f = Fornecedor(cpf_cnpj=cpf_cnpj, nome=nome, tipo=tipo,
                       uf=raw.get("uf", ""), municipio=raw.get("municipio", ""))
        db.add(f)
        db.flush()
    return f


def _salvar(db: Session, raw: dict, codigo_orgao: str) -> bool:
    id_ext = str(raw.get("id", "")).strip()
    if not id_ext or db.query(Contrato).filter_by(id_externo=id_ext).first():
        return False

    orgao_raw = (raw.get("unidadeGestora", {}).get("orgaoVinculado", {})
                 or raw.get("orgao", {}) or {})
    codigo = str(orgao_raw.get("codigoSIAFI") or orgao_raw.get("codigo") or codigo_orgao)
    orgao = _upsert_orgao(db, codigo, orgao_raw.get("nome", ""), orgao_raw.get("sigla", ""))

    forn_raw = raw.get("fornecedor") or raw.get("contratado") or {}
    fornecedor = _upsert_fornecedor(db, forn_raw)

    modalidade = raw.get("modalidadeLicitacao") or {}
    if isinstance(modalidade, dict):
        modalidade = modalidade.get("descricao") or modalidade.get("nome") or ""

    licitacao  = raw.get("licitacao") or {}
    qtd        = raw.get("quantidadeFornecedores") or None

    db.add(Contrato(
        id_externo         = id_ext,
        numero             = raw.get("numero") or "",
        objeto             = raw.get("objeto") or "",
        valor              = float(raw.get("valorInicial") or raw.get("valor") or 0),
        data_inicio        = _parse_date(raw.get("dataInicioVigencia") or raw.get("dataInicio")),
        data_fim           = _parse_date(raw.get("dataFimVigencia") or raw.get("dataFim")),
        modalidade_licitacao = str(modalidade)[:100] if modalidade else None,
        numero_licitacao   = str(licitacao.get("numero", ""))[:50] or None,
        qtd_concorrentes   = int(qtd) if qtd else None,
        orgao_id           = orgao.id,
        fornecedor_id      = fornecedor.id if fornecedor else None,
        data_coleta        = datetime.utcnow(),
        fonte              = "portal_transparencia",
    ))
    return True


# ── Pipeline principal ───────────────────────────────────────────────────────
def _trimestres(ano: int) -> list[tuple[date, date]]:
    hoje = date.today()
    periodos = [
        (date(ano, 1, 1),  date(ano, 3, 31)),
        (date(ano, 4, 1),  date(ano, 6, 30)),
        (date(ano, 7, 1),  date(ano, 9, 30)),
        (date(ano, 10, 1), date(ano, 12, 31)),
    ]
    return [(ini, min(fim, hoje)) for ini, fim in periodos if ini <= hoje]


def coletar(anos: list[int], codigos: list[str]) -> None:
    if not API_KEY:
        logger.error("PORTAL_API_KEY nao configurada no arquivo .env")
        return

    create_tables()
    total_ins = total_dup = 0

    for codigo in codigos:
        info = ORGAOS.get(codigo, {"nome": codigo, "sigla": ""})
        logger.info("=== Coletando: %s (%s) ===", info["nome"], codigo)

        for ano in anos:
            for ini, fim in _trimestres(ano):
                ins = dup = 0
                db = SessionLocal()
                try:
                    pagina = 1
                    while True:
                        dados = _get_pagina(codigo, ini, fim, pagina)
                        if not dados:
                            break
                        for raw in dados:
                            if _salvar(db, raw, codigo):
                                ins += 1
                            else:
                                dup += 1
                        if (ins + dup) % 200 == 0:
                            db.commit()
                        if len(dados) < PAGE_SIZE:
                            break
                        pagina += 1
                        time.sleep(0.4)
                    db.commit()
                    logger.info("%s %s→%s: +%d inseridos, %d duplicados",
                                info["sigla"], ini, fim, ins, dup)
                except Exception as e:
                    db.rollback()
                    logger.error("Erro: %s", e)
                finally:
                    db.close()
                total_ins += ins
                total_dup += dup
        time.sleep(1)

    logger.info("=== Coleta finalizada: %d inseridos, %d duplicados ===", total_ins, total_dup)


def _args():
    p = argparse.ArgumentParser(description="Coleta Portal da Transparencia — MTR-Saude")
    p.add_argument("--ano",   type=int, help="Coleta apenas este ano")
    p.add_argument("--orgao", type=str, help=f"Codigo SIAFI. Opcoes: {', '.join(ORGAOS)}")
    return p.parse_args()


if __name__ == "__main__":
    a = _args()
    anos    = [a.ano]   if a.ano   else list(range(ANO_INICIO, ANO_FIM + 1))
    codigos = [a.orgao] if a.orgao else list(ORGAOS.keys())
    coletar(anos, codigos)
