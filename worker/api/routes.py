from fastapi import APIRouter, HTTPException, BackgroundTasks, status, Depends
from fastapi.responses import JSONResponse
from typing import Dict, Any
from datetime import datetime
import asyncio

from api.models import (
    TaskAssignmentRequest, TaskStatusResponse, WorkerStatusResponse,
    TaskExecutionResult, HealthCheckResponse
)
from core.worker_engine import WorkerEngine
from core.resource_monitor import ResourceMonitor
from utils.logger import get_logger
from utils.config import get_settings

router = APIRouter()
logger = get_logger(__name__)
settings = get_settings()

# Dependencias globales - se inicializarán en main.py
worker_engine: WorkerEngine = None
resource_monitor: ResourceMonitor = None

def get_worker_engine() -> WorkerEngine:
    """Dependency para obtener la instancia del WorkerEngine"""
    global worker_engine
    if worker_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Worker engine not initialized"
        )
    return worker_engine

def get_resource_monitor() -> ResourceMonitor:
    """Dependency para obtener la instancia del ResourceMonitor"""
    global resource_monitor
    if resource_monitor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Resource monitor not initialized"
        )
    return resource_monitor

@router.post("/tasks/assign", response_model=dict)
async def assign_task(
    task_request: TaskAssignmentRequest,
    background_tasks: BackgroundTasks,
    engine: WorkerEngine = Depends(get_worker_engine)
):
    """Endpoint para recibir asignación de tarea del master"""
    try:
        logger.info(f"Received task assignment: {task_request.task_id}")

        # Validar que el worker puede aceptar la tarea
        worker_status = await engine.get_worker_status()
        if worker_status["active_tasks"] >= settings.worker_max_concurrent_tasks:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Worker at maximum capacity"
            )

        # Convertir request a formato interno
        task_data = {
            "task_id": task_request.task_id,
            "job_id": task_request.job_id,
            "task_type": task_request.task_type,
            "map_function": task_request.map_function,
            "reduce_function": task_request.reduce_function,
            "input_splits": task_request.input_splits,
            "output_path": task_request.output_path,
            "parameters": task_request.parameters
        }

        # Enviar tarea al worker engine
        task_id = await engine.submit_task(task_data)

        logger.info(f"Task {task_id} accepted and queued")
        return {
            "task_id": task_id,
            "status": "accepted",
            "worker_id": settings.worker_id,
            "message": "Task accepted and queued for processing"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to assign task: {str(e)}"
        )

@router.get("/tasks/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: str,
    engine: WorkerEngine = Depends(get_worker_engine)
):
    """Obtiene el estado de una tarea específica"""
    try:
        # Buscar tarea en tareas activas
        if task_id in engine.active_tasks:
            task_context = engine.active_tasks[task_id]
            
            return TaskStatusResponse(
                task_id=task_context.task_id,
                status=task_context.status,
                worker_id=settings.worker_id,
                assigned_at=task_context.assigned_at,
                started_at=task_context.started_at,
                completed_at=task_context.completed_at,
                execution_time=task_context.get_execution_time(),
                error_message=task_context.error_message,
                result=task_context.result
            )
        
        # Si no está en tareas activas, podría estar completada
        logger.warning(f"Task {task_id} not found in active tasks")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get task status: {str(e)}"
        )

@router.delete("/tasks/{task_id}", response_model=dict)
async def cancel_task(
    task_id: str,
    engine: WorkerEngine = Depends(get_worker_engine)
):
    """Cancela una tarea en ejecución"""
    try:
        if task_id not in engine.active_tasks:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        task_context = engine.active_tasks[task_id]
        
        # Cancelar tarea si tiene execution_task
        if task_context.execution_task and not task_context.execution_task.done():
            task_context.execution_task.cancel()
            task_context.status = "cancelled"
            task_context.completed_at = datetime.utcnow()
            
            logger.info(f"Task {task_id} cancelled")
            return {
                "task_id": task_id,
                "status": "cancelled",
                "message": "Task cancelled successfully"
            }
        else:
            return {
                "task_id": task_id,
                "status": task_context.status,
                "message": "Task cannot be cancelled (already completed or not running)"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel task: {str(e)}"
        )

@router.get("/status", response_model=WorkerStatusResponse)
async def get_worker_status(
    engine: WorkerEngine = Depends(get_worker_engine),
    monitor: ResourceMonitor = Depends(get_resource_monitor)
):
    """Obtiene el estado completo del worker"""
    try:
        # Obtener estado del engine
        worker_status = await engine.get_worker_status()
        
        # Obtener métricas de recursos
        resource_metrics = await monitor.get_current_metrics()
        
        # Obtener tareas activas detalladas
        active_tasks = []
        for task_context in engine.active_tasks.values():
            active_tasks.append({
                "task_id": task_context.task_id,
                "job_id": task_context.job_id,
                "task_type": task_context.task_type,
                "status": task_context.status,
                "started_at": task_context.started_at,
                "execution_time": task_context.get_execution_time()
            })
        
        return WorkerStatusResponse(
            worker_id=settings.worker_id,
            worker_type=settings.worker_type,
            status=worker_status["status"],
            host=settings.worker_host,
            port=settings.worker_port,
            uptime_seconds=worker_status["uptime_seconds"],
            active_tasks=len(active_tasks),
            queued_tasks=worker_status["queued_tasks"],
            completed_tasks=worker_status["tasks_completed"],
            failed_tasks=worker_status["tasks_failed"],
            cpu_usage=resource_metrics["cpu_percent"],
            memory_usage=resource_metrics["memory_percent"],
            disk_usage=resource_metrics["disk_percent"],
            available_memory_mb=resource_metrics["available_memory_mb"],
            is_overloaded=monitor.is_overloaded(),
            active_task_details=active_tasks
        )
        
    except Exception as e:
        logger.error(f"Error getting worker status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get worker status: {str(e)}"
        )

@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Health check detallado del worker"""
    try:
        # Verificar componentes críticos
        health_status = "healthy"
        checks = []
        
        # Check 1: Worker Engine
        if worker_engine and worker_engine.is_running:
            checks.append({"component": "worker_engine", "status": "healthy"})
        else:
            checks.append({"component": "worker_engine", "status": "unhealthy"})
            health_status = "unhealthy"
        
        # Check 2: Resource Monitor
        if resource_monitor and resource_monitor.is_running:
            checks.append({"component": "resource_monitor", "status": "healthy"})
        else:
            checks.append({"component": "resource_monitor", "status": "unhealthy"})
            health_status = "unhealthy"
        
        # Check 3: Disk Space
        if resource_monitor:
            metrics = await resource_monitor.get_current_metrics()
            disk_usage = metrics.get("disk_percent", 0)
            if disk_usage > 95:
                checks.append({"component": "disk_space", "status": "critical", "usage": f"{disk_usage}%"})
                health_status = "unhealthy"
            elif disk_usage > 85:
                checks.append({"component": "disk_space", "status": "warning", "usage": f"{disk_usage}%"})
            else:
                checks.append({"component": "disk_space", "status": "healthy", "usage": f"{disk_usage}%"})
        
        # Check 4: Memory
        if resource_monitor:
            memory_usage = metrics.get("memory_percent", 0)
            if memory_usage > 95:
                checks.append({"component": "memory", "status": "critical", "usage": f"{memory_usage}%"})
                health_status = "unhealthy"
            elif memory_usage > 85:
                checks.append({"component": "memory", "status": "warning", "usage": f"{memory_usage}%"})
            else:
                checks.append({"component": "memory", "status": "healthy", "usage": f"{memory_usage}%"})
        
        return HealthCheckResponse(
            status=health_status,
            worker_id=settings.worker_id,
            timestamp=datetime.utcnow(),
            checks=checks,
            uptime_seconds=(datetime.utcnow() - settings.start_time).total_seconds()
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthCheckResponse(
            status="unhealthy",
            worker_id=settings.worker_id,
            timestamp=datetime.utcnow(),
            checks=[{"component": "health_check", "status": "failed", "error": str(e)}],
            uptime_seconds=0
        )

@router.get("/metrics", response_model=dict)
async def get_metrics(
    minutes: int = 10,
    monitor: ResourceMonitor = Depends(get_resource_monitor)
):
    """Obtiene métricas detalladas del worker"""
    try:
        current_metrics = await monitor.get_current_metrics()
        metrics_history = await monitor.get_metrics_history(minutes)
        
        return {
            "current": current_metrics,
            "history": metrics_history,
            "history_minutes": minutes,
            "total_samples": len(metrics_history)
        }
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get metrics: {str(e)}"
        )

@router.post("/shutdown", response_model=dict)
async def shutdown_worker(background_tasks: BackgroundTasks):
    """Endpoint para shutdown ordenado del worker"""
    try:
        logger.info("Shutdown requested via API")
        
        # Programar shutdown en background
        background_tasks.add_task(graceful_shutdown)
        
        return {
            "status": "shutdown_initiated",
            "worker_id": settings.worker_id,
            "message": "Graceful shutdown initiated"
        }
        
    except Exception as e:
        logger.error(f"Error initiating shutdown: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate shutdown: {str(e)}"
        )

async def graceful_shutdown():
    """Ejecuta shutdown ordenado del worker"""
    try:
        logger.info("Starting graceful shutdown...")
        
        # Esperar un poco para que la respuesta HTTP se envíe
        await asyncio.sleep(1)
        
        # Detener worker engine
        if worker_engine:
            await worker_engine.stop()
        
        # Detener resource monitor
        if resource_monitor:
            await resource_monitor.stop()
        
        logger.info("Graceful shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during graceful shutdown: {e}")
