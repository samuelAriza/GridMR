from typing import List, Dict, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class WorkerResourceState(BaseModel):
	cpu_usage: float = Field(..., description="Porcentaje de uso de CPU")
	memory_usage: float = Field(..., description="Porcentaje de uso de memoria RAM")
	total_memory_mb: int = Field(..., description="Memoria total en MB")
	available_memory_mb: int = Field(..., description="Memoria disponible en MB")
	running_tasks: int = Field(..., description="Número de tareas en ejecución")
	timestamp: datetime = Field(default_factory=datetime.utcnow, description="Marca de tiempo de la métrica")

class WorkerState(BaseModel):
	worker_id: str = Field(..., description="ID único del worker")
	status: str = Field(..., description="Estado actual del worker (available, busy, offline, error)")
	resources: WorkerResourceState
	active_tasks: List[str] = Field(default_factory=list, description="IDs de tareas activas")
	completed_tasks: int = Field(0, description="Tareas completadas")
	failed_tasks: int = Field(0, description="Tareas fallidas")
	last_heartbeat: Optional[datetime] = Field(None, description="Último heartbeat enviado al master")
