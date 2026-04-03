"""API principal do sistema - BETA 1.0."""

import logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from src.api.routes.contratos import router as contratos_router
from src.database.postgres import create_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"

app = FastAPI(
    title="Contratos Publicos - BETA",
    description="Visualizacao de contratos publicos do Ministerio da Saude.",
    version="0.1.0-beta",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(contratos_router, prefix="/api/v1")


@app.on_event("startup")
async def startup():
    logging.info("Criando tabelas no banco...")
    create_tables()
    logging.info("API BETA iniciada com sucesso!")


@app.get("/api/health")
@app.get("/health")
async def health():
    return {"status": "healthy", "version": "0.1.0-beta"}


# Servir frontend estatico
if FRONTEND_DIR.exists():
    app.mount("/css", StaticFiles(directory=FRONTEND_DIR / "css"), name="css")
    app.mount("/js", StaticFiles(directory=FRONTEND_DIR / "js"), name="js")
    if (FRONTEND_DIR / "pages").exists():
        app.mount("/pages", StaticFiles(directory=FRONTEND_DIR / "pages"), name="pages")

    @app.get("/")
    async def serve_frontend():
        return FileResponse(FRONTEND_DIR / "index.html")
else:
    @app.get("/")
    async def root():
        return {
            "projeto": "Contratos Publicos - BETA",
            "versao": "0.1.0-beta",
            "docs": "/docs",
        }
