import asyncio
import tempfile
import subprocess
import os
import sys
import json
from typing import Dict, Any, Optional
from datetime import datetime
import traceback

from models.task_context import TaskContext
from core.map_processor import MapProcessor
from core.reduce_processor import ReduceProcessor
from services.execution_sandbox import ExecutionSandbox
from utils.logger import get_logger
from utils.config import get_settings

class TaskExecutor:
    """Ejecutor principal de tareas Map/Reduce con sandbox seguro"""
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.sandbox = ExecutionSandbox()
        self.map_processor = MapProcessor()
        self.reduce_processor = ReduceProcessor()
        self.is_running = False
    
    async def start(self):
        """Inicializa el ejecutor"""
        self.logger.info("Starting Task Executor...")
        self.is_running = True
        await self.sandbox.initialize()
        await self.map_processor.start()
        await self.reduce_processor.start()
        self.logger.info("Task Executor started")
    
    async def stop(self):
        """Detiene el ejecutor"""
        self.logger.info("Stopping Task Executor...")
        self.is_running = False
        await self.reduce_processor.stop()
        await self.map_processor.stop()
        await self.sandbox.cleanup()
        self.logger.info("Task Executor stopped")
    
    async def execute_task(self, task_context: TaskContext) -> Dict[str, Any]:
        """Ejecuta una tarea Map o Reduce"""
        if not self.is_running:
            raise Exception("Task executor is not running")
        
        self.logger.info(f"Executing {task_context.task_type} task {task_context.task_id}")
        
        try:
            # Validar contexto de tarea
            self._validate_task_context(task_context)
            
            # Ejecutar según tipo de tarea
            if task_context.task_type == "map":
                result = await self.map_processor.process(task_context)
            elif task_context.task_type == "reduce":
                result = await self.reduce_processor.process(task_context)
            else:
                raise ValueError(f"Unknown task type: {task_context.task_type}")
            
            self.logger.info(f"Task {task_context.task_id} completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Task {task_context.task_id} failed: {e}")
            self.logger.error(traceback.format_exc())
            raise
    
    def _validate_task_context(self, task_context: TaskContext):
        """Valida el contexto de la tarea antes de ejecutar"""
        if not task_context.task_id:
            raise ValueError("Task ID is required")
        
        if not task_context.job_id:
            raise ValueError("Job ID is required")
        
        if task_context.task_type == "map" and not task_context.map_function:
            raise ValueError("Map function is required for map tasks")
        
        if task_context.task_type == "reduce" and not task_context.reduce_function:
            raise ValueError("Reduce function is required for reduce tasks")
        
        if not task_context.input_splits:
            raise ValueError("Input splits are required")
        
        if not task_context.output_path:
            raise ValueError("Output path is required")