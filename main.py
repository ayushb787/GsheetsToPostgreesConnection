from fastapi import FastAPI
from starlette.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware
# from src.all_routes import router
from src.syncfunctions.sync import start_background_sync
from src.routes.all_routes import router
import os

from uvicorn import run


app = FastAPI(
    title="Google Sheets to PostgreSQL Two-way Sync API"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)
app.include_router(router)


@app.on_event("startup")
async def startup_event():
    start_background_sync(app)

if __name__ == "__main__":
    run("main:app", host="0.0.0.0", port=7878, reload=True)