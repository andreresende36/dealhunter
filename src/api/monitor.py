"""
DealHunter Monitor API
Endpoints para coletar estado do runner.py e a fila do banco.
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
import os
import structlog

from src.monitoring.state import MonitorState
from src.database.storage_manager import StorageManager
from src.config import settings

logger = structlog.get_logger(__name__)

app = FastAPI(title="DealHunter Monitor")

@app.get("/api/state")
async def get_state():
    """Retorna os timers de envio e scraping em tempo real."""
    return MonitorState.get_state()

@app.get("/api/queue")
async def get_queue():
    """Retorna os itens aprovados e pendentes de envio do banco."""
    async with StorageManager() as storage:
        # Usa o limite 50 (pode ser ajustado)
        if storage._using_supabase:
            offers = await storage._supabase.get_pending_scored_offers(limit=50)
        else:
            offers = await storage._sqlite.get_pending_scored_offers(limit=50)
            
        return {"queue": offers}


# Adicionar montaagem da pasta web para o Frontend
WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web")

if os.path.exists(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
    
    # Endpoint redirect for convenience
    @app.get("/")
    async def index():
        return RedirectResponse(url="/index.html")

