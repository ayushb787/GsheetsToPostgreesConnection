from fastapi import APIRouter

from src.routes.postgres_curd_endpoints import router as postgres_curd


router = APIRouter()

router.include_router(postgres_curd)