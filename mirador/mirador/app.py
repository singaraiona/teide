"""FastAPI entry point for Mirador."""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Add teide Python bindings to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "py"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from teide import TeideLib

from mirador import __version__
from mirador.api.dashboards import router as dashboards_router
from mirador.api.files import router as files_router
from mirador.api.nodes import router as nodes_router
from mirador.api.pipelines import router as pipelines_router
from mirador.api.projects import router as projects_router
from mirador.api.ws import router as ws_router

_teide: TeideLib | None = None


def get_teide() -> TeideLib:
    """Return the initialized TeideLib instance. Asserts it has been set up."""
    assert _teide is not None, "TeideLib not initialized — lifespan not started"
    return _teide


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _teide
    lib_path = os.environ.get("TEIDE_LIB")
    _teide = TeideLib(lib_path=lib_path)
    _teide.sym_init()
    _teide.arena_init()
    try:
        yield
    finally:
        _teide.pool_destroy()
        _teide.sym_destroy()
        _teide.arena_destroy_all()
        _teide = None


app = FastAPI(title="Mirador", version=__version__, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(dashboards_router)
app.include_router(files_router)
app.include_router(nodes_router)
app.include_router(pipelines_router)
app.include_router(projects_router)
app.include_router(ws_router)


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": __version__, "teide": _teide is not None}


# Serve frontend static files (MUST be after all API routes)
_frontend_dir = Path(__file__).parent / "frontend_dist"
if _frontend_dir.exists():

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve the React SPA — catch-all for non-API routes."""
        file_path = _frontend_dir / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(_frontend_dir / "index.html")


def main():
    import uvicorn

    uvicorn.run("mirador.app:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
