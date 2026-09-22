"""Health endpoint shared by every Python service."""

from fastapi import APIRouter


def health_router(service_name: str) -> APIRouter:
    """Return a router with GET /health for the given service."""
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": service_name}

    return router
