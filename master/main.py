from fastapi import FastAPI
from contextlib import asynccontextmanager
from api.routes import router
from utils.config import get_settings
from utils.logger import setup_logger
import uvicorn

# -------------------------------------------------------------------
# Load application settings and initialize logger
# -------------------------------------------------------------------
settings = get_settings()
logger = setup_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager for startup and shutdown events.
    
    This function ensures that critical startup and cleanup logic
    is executed at the right points in the application lifecycle.
    
    Args:
        app (FastAPI): The FastAPI application instance.
    """
    # Startup phase
    logger.info("GridMR Master Node starting up...")
    yield
    # Shutdown phase
    logger.info("GridMR Master Node shutting down...")


# -------------------------------------------------------------------
# FastAPI application initialization
# -------------------------------------------------------------------
app = FastAPI(
    title="GridMR Master Node",
    version="1.0.0",
    lifespan=lifespan
)

# Register API routes under versioned prefix
app.include_router(router, prefix="/api/v1")


# -------------------------------------------------------------------
# Application entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug  # reload enabled if debug mode is active
    )