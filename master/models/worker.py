from enum import Enum
from typing import Optional, Dict, List
from pydantic import BaseModel
from datetime import datetime


class WorkerStatus(str, Enum):
    """
    Enumeration of possible worker states.
    
    A worker can be:
    - AVAILABLE: Ready to accept new tasks
    - BUSY: At maximum capacity, cannot accept more tasks
    - OFFLINE: Not reachable or intentionally disconnected
    - ERROR: In an error state due to failures or misconfiguration
    """
    AVAILABLE = "available"
    BUSY = "busy"
    OFFLINE = "offline"
    ERROR = "error"


class WorkerCapacity(BaseModel):
    """
    Represents the computing resources and concurrency limits of a worker.
    
    Attributes:
        cpu_cores (int): Number of CPU cores allocated.
        memory_mb (int): Amount of memory allocated (in MB).
        max_concurrent_tasks (int): Maximum number of parallel tasks supported.
    """
    cpu_cores: int = 1
    memory_mb: int = 1024
    max_concurrent_tasks: int = 2


class Worker(BaseModel):
    """
    Represents a worker node in the MapReduce cluster.
    
    Attributes:
        worker_id (str): Unique identifier of the worker.
        host (str): Hostname or IP where the worker runs.
        port (int): Network port exposed by the worker service.
        status (WorkerStatus): Current status of the worker.
        capacity (WorkerCapacity): Hardware and concurrency capacity.
        current_tasks (List[str]): List of task IDs currently running on this worker.
        last_heartbeat (Optional[datetime]): Last time the worker sent a heartbeat.
        total_completed_tasks (int): Counter of successfully completed tasks.
        total_failed_tasks (int): Counter of failed tasks.
    """
    worker_id: str
    host: str
    port: int
    status: WorkerStatus = WorkerStatus.AVAILABLE
    capacity: WorkerCapacity = WorkerCapacity()
    current_tasks: List[str] = []  # IDs of tasks currently running
    last_heartbeat: Optional[datetime] = None
    total_completed_tasks: int = 0
    total_failed_tasks: int = 0

    @property
    def endpoint_url(self) -> str:
        """
        Construct the base URL for API communication with this worker.
        
        Returns:
            str: Worker API endpoint URL.
        """
        return f"http://{self.host}:{self.port}"

    @property
    def is_available(self) -> bool:
        """
        Check if the worker is available for new task assignments.
        
        A worker is considered available if:
        - Its status is AVAILABLE.
        - The number of active tasks is below the maximum concurrency.
        
        Returns:
            bool: True if worker can accept new tasks, False otherwise.
        """
        return (
            self.status == WorkerStatus.AVAILABLE
            and len(self.current_tasks) < self.capacity.max_concurrent_tasks
        )