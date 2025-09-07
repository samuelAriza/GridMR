
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class WorkerSettings(BaseSettings):
    worker_heartbeat_interval: float = Field(10.0, description="Intervalo de heartbeat del worker (segundos)")
    worker_task_timeout_seconds: int = Field(600, description="Timeout por tarea (segundos)")
    worker_data_root_path: str = Field("/data", description="Directorio raíz de datos del worker")
    worker_resource_monitor_interval: float = Field(5.0, description="Intervalo de monitoreo de recursos (segundos)")
    worker_enable_gpu_processing: bool = Field(False, description="Habilitar procesamiento GPU")
    worker_id: str = Field("worker-001", description="ID único del worker")
    worker_host: Optional[str] = Field(None, description="Host para el servidor FastAPI (hostname Docker)")
    worker_port: int = Field(8001, description="Puerto para el servidor FastAPI")
    worker_type: str = Field("generic", description="Tipo de worker: map, reduce o generic")

    worker_cpu_cores: int = Field(2, env="GRIDMR_WORKER_CPU_CORES", description="Cantidad de núcleos de CPU asignados al worker")
    worker_memory_mb: int = Field(2048, env="GRIDMR_WORKER_MEMORY_MB", description="Cantidad de memoria RAM (MB) asignada al worker")


    worker_max_concurrent_tasks: int = Field(4, description="Máximo de tareas concurrentes")
    worker_enable_large_memory_tasks: bool = Field(False, description="Permitir tareas de gran memoria")
    master_host: str = Field("master", description="Host del nodo master")
    master_port: int = Field(8000, description="Puerto del nodo master")
    temp_data_path: str = Field("/tmp/worker_data", description="Directorio temporal de datos")
    download_timeout: int = Field(120, description="Timeout para descargas (segundos)")
    request_timeout: int = Field(30, description="Timeout para requests HTTP")
    log_level: str = Field("info", description="Nivel de logging")
    metrics_port: int = Field(9100, description="Puerto de métricas Prometheus")
    security_token: str = Field("changeme", description="Token de autenticación")
    
    # Thresholds de sobrecarga
    cpu_overload_threshold: float = Field(85.0, description="Umbral de CPU para considerar sobrecarga (%)")
    memory_overload_threshold: float = Field(90.0, description="Umbral de memoria para considerar sobrecarga (%)")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_prefix = "GRIDMR_WORKER_"
                
@lru_cache()
def get_settings() -> WorkerSettings:
    return WorkerSettings()
