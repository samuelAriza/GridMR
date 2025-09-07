from typing import List, Optional, Dict
from models.task import Task, TaskStatus
from models.worker import Worker, WorkerStatus
from services.worker_registry import WorkerRegistry
from utils.logger import get_logger


class TaskScheduler:
    """
    Responsible for assigning tasks to available workers.
    
    This component ensures that tasks are distributed fairly and efficiently
    by checking worker capacity in real time and selecting the best candidate.
    """

    def __init__(self, worker_registry: WorkerRegistry):
        """
        Initialize the TaskScheduler with a worker registry reference.
        
        Args:
            worker_registry (WorkerRegistry): Central registry of all workers.
        """
        self.worker_registry = worker_registry
        self.logger = get_logger(__name__)

    async def schedule_task(self, task: Task) -> Optional[str]:
        """
        Query workers' real status via HTTP and assign a task only if capacity is available.
        
        Selection strategy:
        - Query each worker's active task count from `/api/v1/status`
        - Filter only workers with free capacity
        - Select the worker with the lowest active task count
        - Assign the task and update worker status
        
        Args:
            task (Task): The task to be assigned.
        
        Returns:
            Optional[str]: The ID of the worker assigned, or None if none available.
        """
        import httpx
        available_workers = []

        # Iterate through registered workers
        for w in self.worker_registry.all_workers():
            if w.status != WorkerStatus.AVAILABLE:
                continue
            try:
                # Query worker status endpoint
                status_url = f"{w.endpoint_url}/api/v1/status"
                async with httpx.AsyncClient(timeout=5) as client:
                    resp = await client.get(status_url)
                    resp.raise_for_status()
                    data = resp.json()

                    # Extract active task count
                    active_tasks = data.get("active_tasks", 0)
                    max_tasks = w.capacity.max_concurrent_tasks

                    # Check available capacity
                    if active_tasks < max_tasks:
                        available_workers.append((w, active_tasks))
            except Exception as e:
                self.logger.warning(f"Could not query status of {w.worker_id}: {e}")

        if not available_workers:
            self.logger.warning("No available workers with real capacity for task assignment")
            return None

        # Select worker with the fewest active tasks
        best_worker, _ = min(available_workers, key=lambda tup: tup[1])

        # Assign task
        task.assigned_worker = best_worker.worker_id
        task.status = TaskStatus.ASSIGNED
        best_worker.current_tasks.append(task.task_id)

        # Update worker status if capacity reached
        if len(best_worker.current_tasks) >= best_worker.capacity.max_concurrent_tasks:
            best_worker.status = WorkerStatus.BUSY

        self.logger.info(f"Task {task.task_id} assigned to worker {best_worker.worker_id}")

        # Send task to the worker for execution
        await self._send_task_to_worker(task, best_worker)
        return best_worker.worker_id

    async def _send_task_to_worker(self, task: Task, worker: Worker):
        """
        Send the assigned task to the worker's `/api/v1/tasks/assign` endpoint.
        
        Args:
            task (Task): Task to be executed.
            worker (Worker): Worker instance to receive the task.
        """
        import httpx
        url = f"{worker.endpoint_url}/api/v1/tasks/assign"

        # Prepare task payload
        payload = {
            "task_id": task.task_id,
            "job_id": task.job_id,
            "task_type": task.task_type,
            "map_function": task.map_function,
            "reduce_function": task.reduce_function,
            "input_splits": task.input_splits,
            "output_path": task.output_path,
            "parameters": task.parameters,
            "priority": 1
        }

        # Perform POST request to worker
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                self.logger.info(f"Task {task.task_id} sent to worker {worker.worker_id} ({url})")
            except Exception as e:
                self.logger.error(f"Failed to send task {task.task_id} to worker {worker.worker_id}: {e}")

    def calculate_worker_score(self, worker: Worker, task: Task) -> float:
        """
        Calculate a score to evaluate which worker is best for a given task.
        
        Factors considered:
        - Current load (ratio of active tasks to max capacity)
        - Historical success rate of the worker
        
        Args:
            worker (Worker): Worker instance being evaluated.
            task (Task): Task to assign (unused but available for extensions).
        
        Returns:
            float: Worker score (higher = better candidate).
        """
        current_load = len(worker.current_tasks) / worker.capacity.max_concurrent_tasks
        success_rate = (
            worker.total_completed_tasks /
            max(1, worker.total_completed_tasks + worker.total_failed_tasks)
        )

        # Weighted combination of load and success rate
        score = (1 - current_load) * 0.7 + success_rate * 0.3
        return score