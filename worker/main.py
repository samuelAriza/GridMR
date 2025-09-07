import asyncio
import signal
import sys
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from api.routes import router
from core.worker_engine import WorkerEngine
from core.resource_monitor import ResourceMonitor
from services.heartbeat_service import HeartbeatService
from services.master_client import MasterClient
from utils.config import get_settings
from utils.logger import setup_logger, get_logger
from utils.metrics import MetricsCollector
import uvicorn

class GridMRWorker:
    """Clase principal del worker GridMR"""
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger()
        self.worker_engine: Optional[WorkerEngine] = None
        self.resource_monitor: Optional[ResourceMonitor] = None
    # self.heartbeat_service: Optional[HeartbeatService] = None
        self.master_client: Optional[MasterClient] = None
        self.metrics: Optional[MetricsCollector] = None
        self.shutdown_event = asyncio.Event()
    
    async def startup(self):
        """Inicialización del worker"""
        self.logger.info(f"Starting GridMR Worker {self.settings.worker_id}")
        
        try:
            # Inicializar componentes
            from core.resource_monitor import ResourceMonitor
            
            self.metrics = MetricsCollector()
            self.master_client = MasterClient()
            
            # Inicializar ResourceMonitor
            self.resource_monitor = ResourceMonitor()
            await self.resource_monitor.start()
            
            # Inicializar WorkerEngine con ResourceMonitor
            self.worker_engine = WorkerEngine(self.master_client, self.resource_monitor)
            
            # Asignar objetos globales para las rutas
            import api.routes
            api.routes.worker_engine = self.worker_engine
            api.routes.resource_monitor = self.resource_monitor
            
            # Registrar worker con el master
            # Detectar hostname Docker
            import socket
            hostname = socket.gethostname()
            self.settings.worker_host = hostname
            await self.master_client.register_worker()
            
            # Iniciar servicios
            await self.worker_engine.start()
            
            self.logger.info("GridMR Worker started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start worker: {e}")
            raise
    
    async def shutdown(self):
        """Cierre ordenado del worker"""
        self.logger.info("Shutting down GridMR Worker...")
        
        try:
            # Detener servicios en orden inverso
            # if self.heartbeat_service:
            #     await self.heartbeat_service.stop()
            
            if self.worker_engine:
                await self.worker_engine.stop()
            
            if self.resource_monitor:
                await self.resource_monitor.stop()
            
            if self.master_client:
                await self.master_client.unregister_worker()
                await self.master_client.close()
            
            self.logger.info("GridMR Worker shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def setup_signal_handlers(self):
        """Configura manejadores de señales para cierre ordenado"""
        def signal_handler(sig, frame):
            self.logger.info(f"Received signal {sig}")
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

# Instancia global del worker
worker_instance = GridMRWorker()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Context manager para el ciclo de vida de la aplicación"""
    # Startup
    await worker_instance.startup()
    yield
    # Shutdown
    await worker_instance.shutdown()

# Configuración de FastAPI
app = FastAPI(
    title="GridMR Worker Node",
    version="1.0.0",
    description="GridMR Distributed MapReduce Worker",
    lifespan=lifespan
)

# Middlewares de seguridad
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar orígenes concretos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["*"]  # En producción, especificar hosts concretos
)

# Incluir routes
app.include_router(router, prefix="/api/v1", tags=["worker"])

# Endpoint global de health check
@app.get("/health")
async def health_check():
    """Health check global"""
    return {
        "status": "healthy",
        "worker_id": worker_instance.settings.worker_id,
        "service": "GridMR Worker"
    }

async def main():
    """Función principal"""
    settings = get_settings()
    logger = get_logger(__name__)
    
    # Configurar manejadores de señales
    worker_instance.setup_signal_handlers()
    
    # Configurar servidor
    config = uvicorn.Config(
        "main:app",
        host="0.0.0.0",
        port=settings.worker_port,
        log_level=settings.log_level.lower(),
        access_log=settings.debug,
        reload=False  # Nunca en producción
    )
    
    server = uvicorn.Server(config)
    
    # Ejecutar servidor con manejo de señales
    try:
        await asyncio.gather(
            server.serve(),
            worker_instance.shutdown_event.wait()
        )
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        await server.shutdown()

if __name__ == "__main__":
    asyncio.run(main())