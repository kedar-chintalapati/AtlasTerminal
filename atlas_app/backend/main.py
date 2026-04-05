"""
Atlas Terminal — FastAPI application entry point.

Starts the API server with:
  * CORS configured for the frontend dev server
  * All domain routers mounted under /api/v1
  * WebSocket endpoint for live alert streaming
  * APScheduler background data refresh
  * DuckDB store lifecycle management
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from atlas_app.backend.config import backend_settings
from atlas_app.backend.deps import _get_store_singleton
from atlas_app.backend.routers import energy, events, map_layers, query, research, vessels, weather
from atlas_app.backend.services.scheduler import build_scheduler


# ─── WebSocket connection manager ─────────────────────────────────────────────

class ConnectionManager:
    def __init__(self) -> None:
        self._active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._active.add(ws)
        logger.info(f"WS connect: {len(self._active)} clients")

    def disconnect(self, ws: WebSocket) -> None:
        self._active.discard(ws)

    async def broadcast(self, message: dict) -> None:
        if not self._active:
            return
        data = json.dumps(message, default=str)
        dead: Set[WebSocket] = set()
        for ws in list(self._active):
            try:
                await ws.send_text(data)
            except Exception:
                dead.add(ws)
        self._active -= dead


ws_manager = ConnectionManager()


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start scheduler and store on app startup; clean up on shutdown."""
    store = _get_store_singleton()
    scheduler = build_scheduler(store)
    scheduler.start()
    logger.info("Atlas Terminal API started")

    yield  # ← application runs here

    scheduler.shutdown(wait=False)
    store.close()
    logger.info("Atlas Terminal API shutdown complete")


# ─── FastAPI app ──────────────────────────────────────────────────────────────

app = FastAPI(
    title="Atlas Terminal API",
    description="Physical Commodities Intelligence Terminal — REST + WebSocket API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=backend_settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────

_prefix = backend_settings.api_prefix

app.include_router(energy.router, prefix=_prefix)
app.include_router(weather.router, prefix=_prefix)
app.include_router(events.router, prefix=_prefix)
app.include_router(vessels.router, prefix=_prefix)
app.include_router(map_layers.router, prefix=_prefix)
app.include_router(query.router, prefix=_prefix)
app.include_router(research.router, prefix=_prefix)


# ─── WebSocket — live alert stream ────────────────────────────────────────────

@app.websocket("/ws/alerts")
async def alerts_websocket(ws: WebSocket) -> None:
    """
    WebSocket endpoint that streams atlas alerts to connected clients.

    Clients receive JSON objects with type "alert" or "ping".
    """
    await ws_manager.connect(ws)
    try:
        # Send initial ping so the client knows connection is live
        await ws.send_json({"type": "connected", "message": "Atlas Terminal alert stream connected"})
        while True:
            # Keep connection alive; scheduler broadcasts via ws_manager
            await asyncio.sleep(30)
            await ws.send_json({"type": "ping"})
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception as exc:
        logger.warning(f"WS error: {exc}")
        ws_manager.disconnect(ws)


# ─── Health & status ──────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "atlas-terminal"}


@app.get("/api/v1/status")
async def status() -> dict:
    store = _get_store_singleton()
    tables = store.list_tables()
    row_counts = {}
    for t in tables:
        try:
            row_counts[t] = store.row_count(t)
        except Exception:
            row_counts[t] = -1
    return {
        "status": "ok",
        "tables": row_counts,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "atlas_app.backend.main:app",
        host=backend_settings.host,
        port=backend_settings.port,
        reload=backend_settings.reload,
        log_level=backend_settings.log_level,
    )
