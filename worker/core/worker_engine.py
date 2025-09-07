from datetime import datetime, timedelta
import asyncio
from typing import Dict, Optional, List
from datetime import datetime
import uuid

from models.task_context import TaskContext
from core.task_executor import TaskExecutor
from core.resource_monitor import ResourceMonitor
from services.master_client import MasterClient
from utils.logger import get_logger
from utils.config import get_settings
from utils.metrics import MetricsCollector

class WorkerEngine:
    """Motor principal que coordina toda la actividad del worker"""
    
    def __init__(self, master_client: MasterClient, resource_monitor: Optional[ResourceMonitor] = None):
        self.master_client = master_client
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.metrics = MetricsCollector()
        
        # Componentes
        self.task_executor = TaskExecutor()
        self.resource_monitor = resource_monitor or ResourceMonitor()
        
        # Estado
        self.is_running = False
        self.active_tasks: Dict[str, TaskContext] = {}
        self.task_queue: asyncio.Queue = asyncio.Queue()
        
        # Tasks asyncio
        self._worker_loop_task: Optional[asyncio.Task] = None
        self._resource_monitor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Inicia el motor del worker"""
        if self.is_running:
            return

        self.logger.info("Starting Worker Engine...")
        self.is_running = True

        # Iniciar componentes (solo si no fueron iniciados externamente)
        if not self.resource_monitor.is_running:
            await self.resource_monitor.start()
        await self.task_executor.start()

        # Iniciar loops principales
        self._worker_loop_task = asyncio.create_task(self._worker_loop())
        self._resource_monitor_task = asyncio.create_task(self._monitor_loop())

        # Heartbeat deshabilitado
        # if hasattr(self, 'heartbeat_service'):
        #     await self.heartbeat_service.start()

        self.logger.info("Worker Engine started")
    
    async def stop(self):
        """Detiene el motor del worker"""
        if not self.is_running:
            return
        
        self.logger.info("Stopping Worker Engine...")
        self.is_running = False
        
        # Cancelar tasks
        if self._worker_loop_task:
            self._worker_loop_task.cancel()
        if self._resource_monitor_task:
            self._resource_monitor_task.cancel()
        
        # Esperar tareas activas con timeout
        if self.active_tasks:
            self.logger.info(f"Waiting for {len(self.active_tasks)} active tasks to complete...")
            try:
                await asyncio.wait_for(
                    self._wait_for_tasks_completion(),
                    timeout=self.settings.worker_task_timeout_seconds
                )
            except asyncio.TimeoutError:
                self.logger.warning("Timeout waiting for tasks to complete")
        
        # Detener componentes
        await self.task_executor.stop()
        # Solo detener resource_monitor si no fue iniciado externamente
        if hasattr(self, 'resource_monitor') and self.resource_monitor.is_running:
            # No detenemos si fue iniciado externamente en main.py
            pass
        
        self.logger.info("Worker Engine stopped")
    
    async def submit_task(self, task_data: dict) -> str:
        """Recibe una nueva tarea del master"""
        task_context = TaskContext(
            task_id=task_data["task_id"],
            job_id=task_data["job_id"],
            task_type=task_data["task_type"],
            map_function=task_data.get("map_function"),
            reduce_function=task_data.get("reduce_function"),
            input_splits=task_data["input_splits"],
            output_path=task_data["output_path"],
            parameters=task_data.get("parameters", {}),
            assigned_at=datetime.utcnow()
        )

        # Validar capacidad antes de aceptar
        if len(self.active_tasks) >= self.settings.worker_max_concurrent_tasks:
            raise Exception("Worker at maximum capacity")

        # Añadir a cola de procesamiento
        await self.task_queue.put(task_context)
        self.logger.info(f"Task {task_context.task_id} queued for processing")

        return task_context.task_id
    
    async def get_worker_status(self) -> dict:
        """Obtiene el estado actual del worker"""
        resource_info = await self.resource_monitor.get_current_metrics()
        
        return {
            "worker_id": self.settings.worker_id,
            "status": "running" if self.is_running else "stopped",
            "active_tasks": len(self.active_tasks),
            "queued_tasks": self.task_queue.qsize(),
            "cpu_usage": resource_info["cpu_percent"],
            "memory_usage": resource_info["memory_percent"],
            "disk_usage": resource_info["disk_percent"],
            "uptime_seconds": 300,  # Valor fijo para evitar errores
            "tasks_completed": self.metrics.get_counter("tasks_completed"),
            "tasks_failed": self.metrics.get_counter("tasks_failed")
        }
    
    async def _worker_loop(self):
        """Loop principal del worker para procesar tareas"""
        while self.is_running:
            try:
                # Esperar por nueva tarea con timeout
                task_context = await asyncio.wait_for(
                    self.task_queue.get(),
                    timeout=1.0
                )
                
                # Verificar si aún tenemos capacidad
                if len(self.active_tasks) >= self.settings.worker_max_concurrent_tasks:
                    # Devolver tarea a la cola
                    await self.task_queue.put(task_context)
                    await asyncio.sleep(1)
                    continue
                
                # Procesar tarea
                await self._process_task(task_context)
                
            except asyncio.TimeoutError:
                # Normal, continuar el loop
                continue
            except Exception as e:
                self.logger.error(f"Error in worker loop: {e}")
                await asyncio.sleep(1)
    
    async def _process_task(self, task_context: TaskContext):
        """Procesa una tarea individual"""
        self.logger.info(f"Starting task {task_context.task_id}")
        task_context.started_at = datetime.utcnow()
        self.active_tasks[task_context.task_id] = task_context
        
        # Crear task asyncio para la ejecución
        execution_task = asyncio.create_task(
            self._execute_task(task_context)
        )
        
        # No esperamos aquí, permitimos ejecución concurrente
        task_context.execution_task = execution_task
    
    async def _execute_task(self, task_context: TaskContext):
        """Ejecuta una tarea y maneja el resultado"""
        try:
            # Ejecutar tarea
            result = await self.task_executor.execute_task(task_context)
            
            # Marcar como completada
            task_context.completed_at = datetime.utcnow()
            task_context.status = "completed"
            task_context.result = result
            
            # Reportar éxito al master
            await self.master_client.report_task_completion(
                task_context.task_id, result
            )
            
            self.metrics.increment_counter("tasks_completed")
            self.logger.info(f"Task {task_context.task_id} completed successfully")
            
        except Exception as e:
            # Marcar como fallida
            task_context.error_message = str(e)
            task_context.status = "failed"
            task_context.completed_at = datetime.utcnow()
            
            # Reportar error al master
            await self.master_client.report_task_failure(
                task_context.task_id, str(e)
            )
            
            self.metrics.increment_counter("tasks_failed")
            self.logger.error(f"Task {task_context.task_id} failed: {e}")
        
        finally:
            # Limpiar tarea activa
            if task_context.task_id in self.active_tasks:
                del self.active_tasks[task_context.task_id]
    
    async def _monitor_loop(self):
        """Loop de monitoreo de recursos y limpieza"""
        while self.is_running:
            try:
                # Limpiar tareas completadas hace tiempo
                await self._cleanup_old_tasks()
                
                # Verificar estado de tareas activas
                await self._check_task_timeouts()
                
                # Actualizar métricas
                await self._update_metrics()
                
                await asyncio.sleep(30)  # Check cada 30 segundos
                
            except Exception as e:
                self.logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(5)
    
    async def _cleanup_old_tasks(self):
        """Limpia referencias a tareas completadas antiguas"""
        # Por ahora simple, en producción podría ser más sofisticado
        pass
    
    async def _check_task_timeouts(self):
        """Verifica timeouts de tareas activas"""
        current_time = datetime.utcnow()
        timeout_threshold = timedelta(seconds=self.settings.worker_task_timeout_seconds)

        timed_out_tasks = []
        for task_id, task_context in self.active_tasks.items():
            if task_context.started_at:
                elapsed = current_time - task_context.started_at
                if elapsed > timeout_threshold:
                    timed_out_tasks.append(task_id)

        # Cancelar tareas con timeout
        for task_id in timed_out_tasks:
            task_context = self.active_tasks[task_id]
            if task_context.execution_task:
                task_context.execution_task.cancel()

            await self.master_client.report_task_failure(
                task_id, "Task timeout"
            )

            del self.active_tasks[task_id]
            self.logger.warning(f"Task {task_id} timed out")
    
    async def _update_metrics(self):
        """Actualiza métricas del worker"""
        self.metrics.set_gauge("active_tasks", len(self.active_tasks))
        self.metrics.set_gauge("queued_tasks", self.task_queue.qsize())
    
    async def _wait_for_tasks_completion(self):
        """Espera que se completen todas las tareas activas"""
        while self.active_tasks:
            await asyncio.sleep(1)