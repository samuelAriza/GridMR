from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel
import asyncio

class TaskContext(BaseModel):
    """Contexto completo para la ejecución de una tarea"""
    
    # Identificación
    task_id: str
    job_id: str
    task_type: str  # "map" or "reduce"
    
    # Funciones de usuario
    map_function: Optional[str] = None
    reduce_function: Optional[str] = None
    
    # Datos
    input_splits: List[str]
    output_path: str
    parameters: Dict[str, Any] = {}
    
    # Estado de ejecución
    status: str = "pending"
    assigned_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Resultados
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    # Referencias internas
    execution_task: Optional[asyncio.Task] = None
    
    class Config:
        arbitrary_types_allowed = True
        
    def get_execution_time(self) -> Optional[float]:
        """Calcula el tiempo de ejecución si está disponible"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def is_completed(self) -> bool:
        """Verifica si la tarea está completada"""
        return self.status in ["completed", "failed"]
    
    def is_running(self) -> bool:
        """Verifica si la tarea está ejecutándose"""
        return self.status == "running"