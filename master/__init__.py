from fastapi import FastAPI
from api.routes import router
from utils.config import get_settings
from utils.logger import setup_logger
import uvicorn

app = FastAPI(title="GridMR Master Node", version="1.0.0")
app.include_router(router, prefix="/api/v1")

settings = get_settings()
logger = setup_logger()

@app.on_event("startup")
async def startup_event():
    logger.info("GridMR Master Node starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("GridMR Master Node shutting down...")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )