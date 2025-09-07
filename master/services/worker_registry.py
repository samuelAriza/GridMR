from typing import Dict, List, Optional
from models.worker import Worker, WorkerStatus
from utils.logger import get_logger


class WorkerRegistry:
    """
    Centralized registry for all active workers in the system.
    
    This component acts as the authoritative source of worker information
    for the master node. It provides functionality to:
    - Register or update workers
    - Retrieve workers by ID
    - Query available workers
    - Manage worker states
    
    It ensures the master node always has an up-to-date view of cluster resources.
    """

    def __init__(self):
        # Internal storage mapping worker_id -> Worker instance
        self._workers: Dict[str, Worker] = {}
        self.logger = get_logger(__name__)

    def add_worker(self, worker: Worker):
        """
        Register or update a worker in the registry.
        
        Args:
            worker (Worker): Worker instance to add or update.
        """
        self._workers[worker.worker_id] = worker
        self.logger.info(f"Worker {worker.worker_id} registered/updated in WorkerRegistry")

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        """
        Retrieve a specific worker by ID.
        
        Args:
            worker_id (str): Unique worker identifier.
        
        Returns:
            Worker or None if not found.
        """
        return self._workers.get(worker_id)

    def get_available_workers(self) -> List[Worker]:
        """
        Retrieve all workers that are currently available for task assignment.
        
        Returns:
            List[Worker]: All available workers that can accept new tasks.
        """
        return [w for w in self._workers.values() if w.is_available]

    def all_workers(self) -> List[Worker]:
        """
        Retrieve all registered workers regardless of their state.
        
        Returns:
            List[Worker]: All workers in the registry.
        """
        return list(self._workers.values())

    def set_worker_status(self, worker_id: str, status: WorkerStatus):
        """
        Update the status of a specific worker.
        
        Args:
            worker_id (str): Unique identifier of the worker.
            status (WorkerStatus): New status to set.
        """
        worker = self.get_worker(worker_id)
        if worker:
            worker.status = status
            self.logger.info(f"Worker {worker_id} status updated to {status}")