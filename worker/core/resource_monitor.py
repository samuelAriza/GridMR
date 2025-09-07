import asyncio
import psutil
from typing import Dict, Any
from datetime import datetime, timedelta

from utils.logger import get_logger
from utils.config import get_settings

class ResourceMonitor:
    """Monitor de recursos del sistema para el worker"""
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.is_running = False
        
        # Cache de métricas recientes
        self.current_metrics = {}
        self.metrics_history = []
        self.max_history_size = 100
        
        # Task de monitoreo
        self.monitor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Inicia el monitor de recursos"""
        if self.is_running:
            return
        
        self.logger.info("Starting Resource Monitor...")
        self.is_running = True
        
        # Obtener métricas iniciales
        await self._update_metrics()
        
        # Iniciar loop de monitoreo
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        
        self.logger.info("Resource Monitor started")
    
    async def stop(self):
        """Detiene el monitor de recursos"""
        if not self.is_running:
            return
        
        self.logger.info("Stopping Resource Monitor...")
        self.is_running = False
        
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Resource Monitor stopped")
    
    async def get_current_metrics(self) -> Dict[str, Any]:
        """Obtiene las métricas actuales del sistema"""
        if not self.current_metrics:
            await self._update_metrics()
        
        return self.current_metrics.copy()
    
    async def get_metrics_history(self, minutes: int = 10) -> list:
        """Obtiene el historial de métricas de los últimos N minutos"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        return [
            metric for metric in self.metrics_history 
            if metric["timestamp"] >= cutoff_time
        ]
    
    def is_overloaded(self) -> bool:
        """Verifica si el sistema está sobrecargado"""
        if not self.current_metrics:
            return False
        
        cpu_threshold = self.settings.cpu_overload_threshold
        memory_threshold = self.settings.memory_overload_threshold
        
        return (
            self.current_metrics.get("cpu_percent", 0) > cpu_threshold or
            self.current_metrics.get("memory_percent", 0) > memory_threshold
        )
    
    async def _monitor_loop(self):
        """Loop principal de monitoreo"""
        while self.is_running:
            try:
                await self._update_metrics()
                
                # Agregar al historial
                self.metrics_history.append({
                    **self.current_metrics,
                    "timestamp": datetime.utcnow()
                })
                
                # Mantener tamaño del historial
                if len(self.metrics_history) > self.max_history_size:
                    self.metrics_history.pop(0)
                
                # Log warnings si es necesario
                await self._check_resource_warnings()
                
                await asyncio.sleep(self.settings.worker_resource_monitor_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in resource monitor loop: {e}")
                await asyncio.sleep(5)
    
    async def _update_metrics(self):
        """Actualiza las métricas actuales del sistema"""
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            
            # Memory
            memory = psutil.virtual_memory()
            
            # Disk
            disk = psutil.disk_usage(self.settings.worker_data_root_path)
            
            # Network (opcional)
            network = psutil.net_io_counters()
            
            # Process info
            process = psutil.Process()
            process_info = {
                "pid": process.pid,
                "memory_mb": process.memory_info().rss / 1024 / 1024,
                "cpu_percent": process.cpu_percent(),
                "num_threads": process.num_threads(),
                "open_files": len(process.open_files()),
                "connections": len(process.connections())
            }
            
            self.current_metrics = {
                # Sistema
                "timestamp": datetime.utcnow(),
                "cpu_percent": cpu_percent,
                "cpu_count": cpu_count,
                "memory_percent": memory.percent,
                "memory_total_mb": memory.total / 1024 / 1024,
                "memory_available_mb": memory.available / 1024 / 1024,
                "available_memory_mb": memory.available / 1024 / 1024,
                "disk_percent": disk.percent,
                "disk_total_gb": disk.total / 1024 / 1024 / 1024,
                "disk_free_gb": disk.free / 1024 / 1024 / 1024,
                
                # Red
                "network_bytes_sent": network.bytes_sent,
                "network_bytes_recv": network.bytes_recv,
                
                # Proceso worker
                "process": process_info
            }
            
        except Exception as e:
            self.logger.error(f"Error updating metrics: {e}")
    
    async def _check_resource_warnings(self):
        """Verifica y reporta warnings sobre recursos"""
        metrics = self.current_metrics
        
        # CPU warning
        if metrics.get("cpu_percent", 0) > 80:
            self.logger.warning(f"High CPU usage: {metrics['cpu_percent']:.1f}%")
        
        # Memory warning
        if metrics.get("memory_percent", 0) > 85:
            self.logger.warning(f"High memory usage: {metrics['memory_percent']:.1f}%")
        
        # Disk warning
        if metrics.get("disk_percent", 0) > 90:
            self.logger.warning(f"High disk usage: {metrics['disk_percent']:.1f}%")
        
        # Process warnings
        process_info = metrics.get("process", {})
        if process_info.get("memory_mb", 0) > 1024:  # > 1GB
            self.logger.warning(f"High process memory: {process_info['memory_mb']:.1f} MB")