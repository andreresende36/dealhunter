"""
Crivo — Admin API Router
Endpoints para CRUD de ofertas, gerenciamento de fila, envio manual e scraping.

Autenticação: valida JWT do Supabase Auth via header Authorization: Bearer <token>.
"""

from __future__ import annotations

from typing import Annotated, Any

import httpx
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from src.config import settings
from src.database.storage_manager import StorageManager

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Auth: valida JWT do Supabase Auth
# ---------------------------------------------------------------------------


async def _verify_supabase_jwt(request: Request) -> dict:
    """Valida o JWT do Supabase Auth e retorna o payload do usuário."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token não fornecido")

    token = auth_header.split(" ", 1)[1]
    supabase_url = settings.supabase.url.rstrip("/")

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{supabase_url}/auth/v1/user",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": settings.supabase.anon_key,
            },
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")
    return resp.json()


# Type alias para dependency injection — resolve S8410
CurrentUser = Annotated[dict, Depends(_verify_supabase_jwt)]

_NOT_FOUND = "Oferta não encontrada"


# ---------------------------------------------------------------------------
# Schemas Pydantic
# ---------------------------------------------------------------------------


class StatusUpdate(BaseModel):
    status: str  # "approved" | "rejected" | "pending"


class NotesUpdate(BaseModel):
    admin_notes: str


class BulkAction(BaseModel):
    ids: list[str]
    action: str  # "approve" | "reject" | "delete"


class QueueReorder(BaseModel):
    offer_id: str
    new_priority: int


class SettingsUpdate(BaseModel):
    settings: dict[str, Any]


# ---------------------------------------------------------------------------
# Ofertas CRUD
# ---------------------------------------------------------------------------


@router.patch("/offers/{offer_id}/status", responses={400: {"description": "Status inválido"}, 404: {"description": _NOT_FOUND}})
async def update_offer_status(
    offer_id: str,
    body: StatusUpdate,
    _user: CurrentUser,
):
    """Atualiza o status de uma oferta (approved/rejected/pending)."""
    if body.status not in ("approved", "rejected", "pending"):
        raise HTTPException(status_code=400, detail="Status inválido")

    async with StorageManager() as storage:
        if body.status == "rejected":
            ok = await storage.discard_offer(offer_id, reason="admin_rejected")
        else:
            ok = await _update_scored_offer(storage, offer_id, status=body.status)
        if not ok:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return {"ok": True, "status": body.status}


@router.patch("/offers/{offer_id}/notes", responses={404: {"description": _NOT_FOUND}})
async def update_offer_notes(
    offer_id: str,
    body: NotesUpdate,
    _user: CurrentUser,
):
    """Adiciona/edita notas do admin em uma oferta."""
    async with StorageManager() as storage:
        ok = await _update_scored_offer(
            storage, offer_id, admin_notes=body.admin_notes
        )
        if not ok:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return {"ok": True}


@router.delete("/offers/{offer_id}", responses={404: {"description": _NOT_FOUND}})
async def delete_offer(
    offer_id: str,
    _user: CurrentUser,
):
    """Remove uma oferta da fila (marca como rejected)."""
    async with StorageManager() as storage:
        ok = await storage.discard_offer(offer_id, reason="admin_deleted")
        if not ok:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return {"ok": True}


@router.post("/offers/bulk", responses={400: {"description": "Ação inválida"}})
async def bulk_action(
    body: BulkAction,
    _user: CurrentUser,
):
    """Ação em lote: aprovar, rejeitar ou deletar múltiplas ofertas."""
    if body.action not in ("approve", "reject", "delete"):
        raise HTTPException(status_code=400, detail="Ação inválida")

    results: list[dict] = []
    async with StorageManager() as storage:
        for offer_id in body.ids:
            try:
                if body.action == "approve":
                    ok = await _update_scored_offer(
                        storage, offer_id, status="approved"
                    )
                elif body.action in ("reject", "delete"):
                    ok = await storage.discard_offer(
                        offer_id, reason=f"admin_{body.action}"
                    )
                else:
                    ok = False
                results.append({"id": offer_id, "ok": ok})
            except Exception as exc:
                results.append({"id": offer_id, "ok": False, "error": str(exc)})

    return {"results": results}


# ---------------------------------------------------------------------------
# Fila
# ---------------------------------------------------------------------------


@router.get("/queue")
async def get_admin_queue(
    _user: CurrentUser,
):
    """Retorna a fila completa com prioridade e notas do admin."""
    async with StorageManager() as storage:
        if storage._using_supabase:
            offers = await storage._supabase.get_pending_scored_offers(limit=100)
        else:
            offers = await storage._sqlite.get_pending_scored_offers(limit=100)
    return {"queue": offers}


@router.post("/queue/reorder", responses={404: {"description": _NOT_FOUND}})
async def reorder_queue(
    body: QueueReorder,
    _user: CurrentUser,
):
    """Define a prioridade de uma oferta na fila."""
    async with StorageManager() as storage:
        ok = await _update_scored_offer(
            storage, body.offer_id, queue_priority=body.new_priority
        )
        if not ok:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return {"ok": True, "queue_priority": body.new_priority}


@router.post("/queue/{offer_id}/skip", responses={404: {"description": _NOT_FOUND}})
async def skip_offer(
    offer_id: str,
    _user: CurrentUser,
):
    """Move oferta para o final da fila (priority = -1)."""
    async with StorageManager() as storage:
        ok = await _update_scored_offer(storage, offer_id, queue_priority=-1)
        if not ok:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return {"ok": True}


@router.post("/queue/{offer_id}/pin", responses={404: {"description": _NOT_FOUND}})
async def pin_offer(
    offer_id: str,
    _user: CurrentUser,
):
    """Fixa oferta no topo da fila (priority = 999)."""
    async with StorageManager() as storage:
        ok = await _update_scored_offer(storage, offer_id, queue_priority=999)
        if not ok:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Envio e Scraping manual
# ---------------------------------------------------------------------------


@router.post("/send-now", responses={404: {"description": "Fila vazia"}})
async def send_now(
    _user: CurrentUser,
):
    """Envia a próxima oferta da fila imediatamente."""
    from src.distributor.sender import send_next_offer

    async with StorageManager() as storage:
        sent = await send_next_offer(storage)
    if not sent:
        raise HTTPException(status_code=404, detail="Fila vazia")
    return {"ok": True, "message": "Oferta enviada"}


@router.post("/send-now/{offer_id}", responses={500: {"description": "Falha ao enviar"}})
async def send_specific_offer(
    offer_id: str,
    _user: CurrentUser,
):
    """Fixa uma oferta no topo e envia imediatamente."""
    from src.distributor.sender import send_next_offer

    async with StorageManager() as storage:
        await _update_scored_offer(storage, offer_id, queue_priority=9999)
        sent = await send_next_offer(storage)
    if not sent:
        raise HTTPException(status_code=500, detail="Falha ao enviar")
    return {"ok": True, "message": "Oferta enviada"}


@router.post("/scrape-now")
async def scrape_now(
    _user: CurrentUser,
):
    """Dispara um ciclo de scraping imediato."""
    from src.scraper.pipeline import run_pipeline

    async with StorageManager() as storage:
        stats = await run_pipeline(storage)
    return {"ok": True, "stats": stats}


# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------


@router.get("/settings")
async def get_settings(
    _user: CurrentUser,
):
    """Retorna configurações editáveis do sistema."""
    async with StorageManager() as storage:
        overrides = _get_admin_settings(storage)

    return {
        "current": {
            "score_min_discount_pct": settings.score.min_discount_pct,
            "score_min_score": settings.score.min_score,
            "score_min_rating": settings.score.min_rating,
            "score_min_reviews": settings.score.min_reviews,
            "sender_start_hour": settings.sender.start_hour,
            "sender_end_hour": settings.sender.end_hour,
            "sender_min_interval": settings.sender.min_interval,
            "sender_max_interval": settings.sender.max_interval,
        },
        "overrides": overrides,
    }


@router.patch("/settings")
async def update_settings(
    body: SettingsUpdate,
    _user: CurrentUser,
):
    """Atualiza configurações do admin (persistidas em admin_settings)."""
    async with StorageManager() as storage:
        for key, value in body.settings.items():
            _set_admin_setting(storage, key, value)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------


@router.get("/analytics/daily")
async def analytics_daily(
    _user: CurrentUser,
    days: int = 30,
):
    """Métricas diárias para gráficos trend."""
    async with StorageManager() as storage:
        data = _call_rpc(storage,"fn_daily_metrics", {"days_back": days})
    return {"data": data}


@router.get("/analytics/hourly")
async def analytics_hourly(
    _user: CurrentUser,
):
    """Envios por hora de hoje."""
    async with StorageManager() as storage:
        data = _call_rpc(storage,"fn_hourly_sends", {})
    return {"data": data}


@router.get("/analytics/funnel")
async def analytics_funnel(
    _user: CurrentUser,
    hours: int = 24,
):
    """Funil de conversão."""
    async with StorageManager() as storage:
        data = _call_rpc(storage,"fn_conversion_funnel", {"hours_back": hours})
    return {"data": data}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health")
async def admin_health(
    _user: CurrentUser,
):
    """Health check detalhado com status dos backends."""
    async with StorageManager() as storage:
        ping = await storage.ping()
    return {"backends": ping, "healthy": True}


# ---------------------------------------------------------------------------
# Helpers internos (operações no Supabase via service_role)
# ---------------------------------------------------------------------------


async def _update_scored_offer(
    storage: StorageManager,
    scored_offer_id: str,
    **fields: Any,
) -> bool:
    """Atualiza campos arbitrários de um scored_offer."""
    if storage._using_supabase:
        try:
            resp = (
                storage._supabase._client.table("scored_offers")
                .update(fields)
                .eq("id", scored_offer_id)
                .execute()
            )
            return bool(resp.data)
        except Exception as exc:
            logger.warning("update_scored_offer_failed", error=str(exc))
            return False
    else:
        # SQLite fallback
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [scored_offer_id]
        try:
            async with storage._sqlite._get_conn() as conn:
                cursor = await conn.execute(
                    f"UPDATE scored_offers SET {set_clause} WHERE id = ?",
                    values,
                )
                await conn.commit()
                return (cursor.rowcount or 0) > 0
        except Exception as exc:
            logger.warning("update_scored_offer_sqlite_failed", error=str(exc))
            return False


def _get_admin_settings(storage: StorageManager) -> dict[str, Any]:
    """Lê todas as configurações do admin_settings."""
    if storage._using_supabase:
        try:
            resp = storage._supabase._client.table("admin_settings").select("*").execute()
            return {row["key"]: row["value"] for row in (resp.data or [])}
        except Exception:
            return {}
    return {}


def _set_admin_setting(storage: StorageManager, key: str, value: Any) -> bool:
    """Upsert de uma configuração no admin_settings."""
    if storage._using_supabase:
        try:
            storage._supabase._client.table("admin_settings").upsert(
                {"key": key, "value": value},
                on_conflict="key",
            ).execute()
            return True
        except Exception as exc:
            logger.warning("set_admin_setting_failed", error=str(exc))
            return False
    return False


def _call_rpc(
    storage: StorageManager,
    fn_name: str,
    params: dict[str, Any],
) -> list[dict] | dict | None:
    """Chama uma RPC function do Supabase."""
    if storage._using_supabase:
        try:
            resp = storage._supabase._client.rpc(fn_name, params).execute()
            return resp.data
        except Exception as exc:
            logger.warning("rpc_call_failed", fn=fn_name, error=str(exc))
            return None
    return None
