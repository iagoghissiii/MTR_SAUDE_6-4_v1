"""Conexao e modelos SQLite/PostgreSQL (BETA - apenas dados dos contratos)."""

from datetime import datetime, date
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text,
    DateTime, Date, ForeignKey,
)
from sqlalchemy.orm import sessionmaker, DeclarativeBase, relationship
from src.utils.config import settings

engine = create_engine(
    settings.database_url,
    echo=False,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class Orgao(Base):
    __tablename__ = "orgaos"

    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String(20), unique=True, nullable=False, index=True)
    nome = Column(String(300), nullable=False)
    sigla = Column(String(30))

    contratos = relationship("Contrato", back_populates="orgao")


class Fornecedor(Base):
    __tablename__ = "fornecedores"

    id = Column(Integer, primary_key=True, index=True)
    cpf_cnpj = Column(String(18), unique=True, nullable=False, index=True)
    nome = Column(String(300), nullable=False)
    tipo = Column(String(2))  # PF ou PJ
    uf = Column(String(2))
    municipio = Column(String(200))

    contratos = relationship("Contrato", back_populates="fornecedor")


class Contrato(Base):
    __tablename__ = "contratos"

    id = Column(Integer, primary_key=True, index=True)
    numero = Column(String(50), index=True)
    objeto = Column(Text)
    valor = Column(Float, nullable=False)
    data_inicio = Column(Date)
    data_fim = Column(Date)
    modalidade_licitacao = Column(String(100))
    numero_licitacao = Column(String(50))
    qtd_concorrentes = Column(Integer)

    orgao_id = Column(Integer, ForeignKey("orgaos.id"), index=True)
    fornecedor_id = Column(Integer, ForeignKey("fornecedores.id"), index=True)

    orgao = relationship("Orgao", back_populates="contratos")
    fornecedor = relationship("Fornecedor", back_populates="contratos")

    data_coleta = Column(DateTime, default=datetime.utcnow)
    fonte = Column(String(50))


def create_tables() -> None:
    """Cria todas as tabelas no banco de dados."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Dependency para injecao de sessao nos endpoints FastAPI."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
