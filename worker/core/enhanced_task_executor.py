"""
Enhanced TaskExecutor with Integrated Performance Monitoring

Executes MapReduce tasks with comprehensive performance tracking and metrics collection.
Integrates with the GridMR performance monitoring system to provide detailed analytics.
"""

import asyncio
import time
import os
import sys
from typing import Dict, Any, Optional

from models.task_context import TaskContext
from core.map_processor import MapProcessor
from core.reduce_processor import ReduceProcessor
from models.task import TaskType
from utils.logger import get_logger

# Import performance monitoring if available
try:
    # Add testing directory to path for performance monitoring
    testing_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'testing')
    if testing_path not in sys.path:
        sys.path.append(testing_path)
    from performance_monitor import PerformanceMonitor
    PERFORMANCE_MONITORING_AVAILABLE = True
except ImportError:
    PERFORMANCE_MONITORING_AVAILABLE = False


class TaskExecutorResult:
    """
    Enhanced task execution result with performance metrics.
    
    Contains execution results along with comprehensive performance
    data for analysis and optimization.
    """
    
    def __init__(self, success: bool, result: Dict[str, Any] = None, 
                 error: str = None, performance_metrics: Dict[str, Any] = None,
                 execution_time: float = None):
        self.success = success
        self.result = result or {}
        self.error = error
        self.performance_metrics = performance_metrics or {}
        self.execution_time = execution_time
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "success": self.success,
            "result": self.result,
            "error": self.error,
            "performance_metrics": self.performance_metrics,
            "execution_time": self.execution_time
        }


class TaskExecutor:
    """
    Enhanced task execution engine with integrated performance monitoring.
    
    Orchestrates the execution of MapReduce tasks while collecting comprehensive
    performance metrics including CPU, memory, I/O, and execution timing data.
    Provides detailed analytics for system optimization and scaling decisions.
    """
    
    def __init__(self, map_processor: MapProcessor, reduce_processor: ReduceProcessor):
        """
        Initialize task executor with processors and performance monitoring.
        
        Args:
            map_processor: Processor for Map phase tasks
            reduce_processor: Processor for Reduce phase tasks
        """
        self.map_processor = map_processor
        self.reduce_processor = reduce_processor
        self.logger = get_logger(__name__)
        
        # Initialize performance monitoring if available
        if PERFORMANCE_MONITORING_AVAILABLE:
            self.performance_monitor = PerformanceMonitor(collection_interval=0.5)
            self.logger.info("Performance monitoring enabled for task execution")
        else:
            self.performance_monitor = None
            self.logger.warning("Performance monitoring not available - install psutil for metrics")
            
        # Metrics storage for this executor
        self.execution_history = []
        
    async def execute_task(self, task_context: TaskContext) -> TaskExecutorResult:
        """
        Execute a MapReduce task with comprehensive performance monitoring.
        
        Args:
            task_context: Context containing task configuration and data
            
        Returns:
            TaskExecutorResult with execution results and performance metrics
        """
        start_time = time.time()
        task_id = task_context.task_id
        task_type = task_context.task_type.value if hasattr(task_context.task_type, 'value') else str(task_context.task_type)
        
        self.logger.info(f"Starting task execution: {task_id} (type: {task_type})")
        
        # Start performance monitoring
        performance_summary = {}
        if self.performance_monitor:
            try:
                self.performance_monitor.start_monitoring(task_id=task_id, task_type=task_type)
                self.logger.debug(f"Performance monitoring started for task {task_id}")
            except Exception as e:
                self.logger.warning(f"Failed to start performance monitoring: {e}")
        
        try:
            # Validate task context
            self._validate_task_context(task_context)
            
            # Execute appropriate processor based on task type
            if task_context.task_type == TaskType.MAP:
                self.logger.info(f"Executing MAP task {task_id}")
                result = await self.map_processor.process(task_context)
                
            elif task_context.task_type == TaskType.REDUCE:
                self.logger.info(f"Executing REDUCE task {task_id}")
                result = await self.reduce_processor.process(task_context)
                
            else:
                raise ValueError(f"Unknown task type: {task_context.task_type}")
                
            execution_time = time.time() - start_time
            
            # Stop monitoring and collect performance summary
            if self.performance_monitor:
                try:
                    performance_summary = self.performance_monitor.stop_monitoring()
                    
                    # Save detailed metrics for analysis
                    if performance_summary:
                        metrics_dir = "/app/logs/performance"
                        os.makedirs(metrics_dir, exist_ok=True)
                        metrics_file = f"{metrics_dir}/task_{task_id}_metrics.json"
                        self.performance_monitor.save_detailed_metrics(metrics_file)
                        
                        self.logger.info(f"Performance metrics saved: {metrics_file}")
                        
                except Exception as e:
                    self.logger.warning(f"Error collecting performance metrics: {e}")
            
            # Store execution history for analysis
            execution_record = {
                "task_id": task_id,
                "task_type": task_type,
                "execution_time": execution_time,
                "success": True,
                "timestamp": time.time(),
                "performance_summary": performance_summary
            }
            self.execution_history.append(execution_record)
            
            # Limit history size to prevent memory issues
            if len(self.execution_history) > 100:
                self.execution_history = self.execution_history[-50:]
            
            self.logger.info(f"Task {task_id} completed successfully in {execution_time:.2f}s")
            
            return TaskExecutorResult(
                success=True,
                result=result,
                performance_metrics=performance_summary,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Task {task_id} failed after {execution_time:.2f}s: {str(e)}"
            
            self.logger.error(error_msg, exc_info=True)
            
            # Stop monitoring on failure
            if self.performance_monitor:
                try:
                    performance_summary = self.performance_monitor.stop_monitoring()
                except Exception as monitor_e:
                    self.logger.warning(f"Error stopping performance monitor: {monitor_e}")
            
            # Record failed execution
            execution_record = {
                "task_id": task_id,
                "task_type": task_type,
                "execution_time": execution_time,
                "success": False,
                "error": str(e),
                "timestamp": time.time(),
                "performance_summary": performance_summary
            }
            self.execution_history.append(execution_record)
            
            return TaskExecutorResult(
                success=False,
                error=error_msg,
                performance_metrics=performance_summary,
                execution_time=execution_time
            )
            
    def _validate_task_context(self, task_context: TaskContext) -> None:
        """
        Validate task context contains all required information.
        
        Args:
            task_context: Task context to validate
            
        Raises:
            ValueError: If task context is invalid
        """
        if not task_context.task_id:
            raise ValueError("Task context missing task_id")
            
        if not task_context.task_type:
            raise ValueError("Task context missing task_type")
            
        if not task_context.input_splits:
            raise ValueError("Task context missing input_splits")
            
        if task_context.task_type == TaskType.MAP:
            if not task_context.map_function:
                raise ValueError("MAP task missing map_function")
        elif task_context.task_type == TaskType.REDUCE:
            if not task_context.reduce_function:
                raise ValueError("REDUCE task missing reduce_function")
                
        self.logger.debug(f"Task context validation passed for {task_context.task_id}")
        
    def get_execution_metrics(self) -> Dict[str, Any]:
        """
        Get execution metrics and statistics for this task executor.
        
        Returns:
            Dictionary containing execution statistics and performance data
        """
        if not self.execution_history:
            return {"status": "no_executions", "message": "No tasks executed yet"}
            
        successful_tasks = [t for t in self.execution_history if t["success"]]
        failed_tasks = [t for t in self.execution_history if not t["success"]]
        
        # Calculate statistics
        total_execution_time = sum(t["execution_time"] for t in self.execution_history)
        avg_execution_time = total_execution_time / len(self.execution_history)
        
        execution_times = [t["execution_time"] for t in successful_tasks]
        
        metrics = {
            "summary": {
                "total_tasks": len(self.execution_history),
                "successful_tasks": len(successful_tasks),
                "failed_tasks": len(failed_tasks),
                "success_rate": len(successful_tasks) / len(self.execution_history) * 100,
                "total_execution_time": round(total_execution_time, 2),
                "average_execution_time": round(avg_execution_time, 2)
            },
            "performance_stats": {
                "min_execution_time": round(min(execution_times), 2) if execution_times else 0,
                "max_execution_time": round(max(execution_times), 2) if execution_times else 0,
                "median_execution_time": round(sorted(execution_times)[len(execution_times)//2], 2) if execution_times else 0
            },
            "task_breakdown": {
                "map_tasks": len([t for t in successful_tasks if t["task_type"] == "MAP"]),
                "reduce_tasks": len([t for t in successful_tasks if t["task_type"] == "REDUCE"])
            },
            "recent_tasks": self.execution_history[-10:] if len(self.execution_history) > 10 else self.execution_history
        }
        
        return metrics
        
    def get_current_performance(self) -> Dict[str, Any]:
        """
        Get current system performance metrics if monitoring is available.
        
        Returns:
            Dictionary with current performance metrics or empty dict
        """
        if self.performance_monitor:
            try:
                return self.performance_monitor.get_current_metrics()
            except Exception as e:
                self.logger.warning(f"Error getting current performance metrics: {e}")
                return {}
        return {}
        
    def reset_metrics(self) -> None:
        """Reset execution history and performance baseline."""
        self.execution_history.clear()
        if self.performance_monitor:
            try:
                self.performance_monitor.reset_baseline()
                self.logger.info("Performance monitoring baseline reset")
            except Exception as e:
                self.logger.warning(f"Error resetting performance baseline: {e}")
                
        self.logger.info("Task executor metrics reset")
        
    def is_performance_monitoring_enabled(self) -> bool:
        """Check if performance monitoring is available and enabled."""
        return PERFORMANCE_MONITORING_AVAILABLE and self.performance_monitor is not None