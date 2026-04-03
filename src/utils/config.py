"""Configuracoes centralizadas do projeto (BETA)."""

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings:
    # Database mode: SQLite para desenvolvimento
    DB_MODE: str = "sqlite"

    @property
    def database_url(self) -> str:
        db_path = BASE_DIR / "data" / "contratos.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{db_path}"


settings = Settings()
