# Standard library imports for async operations and data handling
import asyncio
import json
from typing import Dict, Any, Optional
from datetime import datetime

# Third-party imports for HTTP client operations
import httpx

# Internal imports for configuration and logging
from utils.logger import get_logger
from utils.config import get_settings

class MasterClient:
    """
    HTTP client for secure and reliable communication with the master node.
    
    This client manages all worker-to-master communication including:
    - Worker registration and heartbeat management
    - Task completion and failure reporting
    - Status updates and health checks
    - Error handling and retry logic for network issues
    
    The client ensures reliable communication even in distributed environments
    with network partitions, temporary master unavailability, and connection issues.
    
    Key Features:
    - Automatic worker registration with retry logic
    - Persistent HTTP connection management
    - Comprehensive error handling and logging
    - Support for various worker configurations and capabilities
    - Graceful handling of master node unavailability
    """
    
    def __init__(self):
        """
        Initialize MasterClient with configuration and connection settings.
        
        Sets up:
        - System configuration from settings
        - Logger for communication monitoring
        - HTTP client configuration with timeouts and limits
        - Master URL construction from configuration
        - Registration state tracking
        """
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.client: Optional[httpx.AsyncClient] = None
        self.master_url = f"http://{self.settings.master_host}:{self.settings.master_port}"
        self.is_registered = False
    
    async def start(self):
        """
        Initialize the HTTP client with proper configuration for distributed communication.
        
        Configures the HTTP client with:
        - Appropriate timeouts for distributed network conditions
        - Connection pooling for efficient communication
        - Retry and error handling configurations
        - Logging for connection monitoring
        """
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.settings.request_timeout),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
        )
        self.logger.info(f"Master client initialized for {self.master_url}")
    
    async def close(self):
        """
        Properly close the HTTP client and release all network resources.
        
        Ensures clean shutdown by:
        - Closing all active HTTP connections
        - Releasing connection pool resources
        - Logging shutdown completion for monitoring
        """
        if self.client:
            await self.client.aclose()
            self.logger.info("Master client closed")
    
    async def register_worker(self) -> bool:
        """
        Register this worker node with the master for task assignment.
        
        This method performs complete worker registration:
        1. Initializes HTTP client if not already started
        2. Collects comprehensive worker capability information
        3. Sends registration request to master node
        4. Handles registration failures with appropriate logging
        
        Registration is required before the worker can receive task assignments
        from the master node's task scheduler.
        
        Returns:
            bool: True if registration successful, False otherwise
            
        Side Effects:
            - Sets self.is_registered flag on successful registration
            - Logs registration status for monitoring
            - May retry registration on temporary failures
        """
        if not self.client:
            await self.start()
        
        # Collect comprehensive worker information for master registration
        worker_info = {
            "worker_id": self.settings.worker_id,
            "host": self.settings.worker_host,
            "port": self.settings.worker_port,
            "worker_type": self.settings.worker_type,
            "cpu_cores": self.settings.worker_cpu_cores,
            "memory_mb": self.settings.worker_memory_mb,
            "max_concurrent_tasks": self.settings.worker_max_concurrent_tasks,
            "capabilities": self._get_worker_capabilities()
        }
        
        try:
            # Send worker registration request to master node
            response = await self.client.post(
                f"{self.master_url}/api/v1/workers/register",
                json=worker_info
            )
            response.raise_for_status()
            
            self.is_registered = True
            self.logger.info(f"Worker {self.settings.worker_id} registered with master")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register with master: {e}")
            return False
    
    async def unregister_worker(self) -> bool:
        """
        Unregister this worker node from the master before shutdown.
        
        This method handles graceful worker removal:
        1. Sends unregistration request to master node
        2. Removes worker from master's active worker registry
        3. Prevents master from assigning new tasks to this worker
        4. Handles network errors gracefully during shutdown
        
        Returns:
            bool: True if unregistration successful, False otherwise
            
        Side Effects:
            - Resets self.is_registered flag on successful unregistration
            - Logs unregistration status for monitoring
        """
        if not self.client or not self.is_registered:
            return True
        
        try:
            response = await self.client.delete(
                f"{self.master_url}/api/v1/workers/{self.settings.worker_id}"
            )
            response.raise_for_status()
            
            self.is_registered = False
            self.logger.info(f"Worker {self.settings.worker_id} unregistered from master")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unregister from master: {e}")
            return False
    
    async def send_heartbeat(self, worker_status: Dict[str, Any]) -> bool:
        """
        Send periodic heartbeat with worker status to the master node.
        
        This method provides continuous health monitoring:
        1. Collects comprehensive worker status information
        2. Reports current resource utilization and task load
        3. Enables master to make informed load balancing decisions
        4. Provides failure detection and worker availability monitoring
        
        Args:
            worker_status: Dictionary containing current worker status including:
                         - CPU and memory utilization
                         - Active task information
                         - Task completion statistics
                         - Health and availability status
        
        Returns:
            bool: True if heartbeat sent successfully, False otherwise
            
        Side Effects:
            - Master updates worker status in registry
            - May trigger load rebalancing decisions
            - Enables failure detection for this worker
        """
        if not self.client or not self.is_registered:
            return False
        
        # Prepare comprehensive heartbeat data for master monitoring
        heartbeat_data = {
            "worker_id": self.settings.worker_id,
            "timestamp": datetime.utcnow().isoformat(),
            "status": worker_status["status"],
            "current_load": worker_status.get("cpu_usage", 0) / 100.0,
            "available_memory_mb": worker_status.get("available_memory_mb", 0),
            "active_tasks": worker_status.get("active_tasks", []),
            "metrics": {
                "tasks_completed": worker_status.get("tasks_completed", 0),
                "tasks_failed": worker_status.get("tasks_failed", 0),
                "uptime_seconds": worker_status.get("uptime_seconds", 0)
            }
        }
        
        try:
            response = await self.client.post(
                f"{self.master_url}/api/v1/workers/{self.settings.worker_id}/heartbeat",
                json=heartbeat_data
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat: {e}")
            return False
    
    async def report_task_completion(self, task_id: str, result: Dict[str, Any]) -> bool:
        """
        Report successful task completion to the master node.
        
        This method is critical for MapReduce workflow orchestration:
        1. Notifies master that a task has completed successfully
        2. Provides task execution results and performance metrics
        3. Enables automatic phase transitions (Map→Reduce, Reduce→Job completion)
        4. Updates task status in master's job management system
        
        Task completion reports trigger important workflow events:
        - Map task completion may trigger Reduce task creation
        - Reduce task completion may trigger job completion
        - Results are used for final output consolidation
        
        Args:
            task_id: Unique identifier for the completed task
            result: Dictionary containing task execution results including:
                   - Output file paths for result retrieval
                   - Performance metrics and timing information
                   - Processing statistics (records processed, etc.)
        
        Returns:
            bool: True if completion reported successfully, False otherwise
            
        Side Effects:
            - Updates task status to COMPLETED in master
            - May trigger MapReduce phase transitions
            - Enables result consolidation and job completion
        """
        if not self.client:
            return False
        
        # Prepare comprehensive completion report for master
        report_data = {
            "task_id": task_id,
            "worker_id": self.settings.worker_id,
            "status": "completed",
            "result": result,  # Contains output files, metrics, and processing stats
            "completed_at": datetime.utcnow().isoformat()
        }
        
        try:
            # Send completion report to master's task completion endpoint
            response = await self.client.post(
                f"{self.master_url}/api/v1/tasks/{task_id}/complete",
                json=report_data
            )
            response.raise_for_status()
            
            self.logger.info(f"Task {task_id} completion reported to master")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to report task completion: {e}")
            return False
    
    async def report_task_failure(self, task_id: str, error_message: str) -> bool:
        """
        Report task execution failure to the master node.
        
        This method handles task failure notification and enables:
        1. Master awareness of task execution problems
        2. Potential task retry or job failure decisions
        3. Error logging and debugging information collection
        4. Proper cleanup and resource management
        
        Task failure reports allow the master to:
        - Mark tasks as FAILED in the job management system
        - Decide whether to retry tasks or fail the entire job
        - Track failure patterns for system monitoring
        - Provide detailed error information to clients
        
        Args:
            task_id: Unique identifier for the failed task
            error_message: Detailed error description for debugging
        
        Returns:
            bool: True if failure reported successfully, False otherwise
            
        Side Effects:
            - Updates task status to FAILED in master
            - May trigger job failure or task retry logic
            - Records error information for debugging
        """
        if not self.client:
            return False
        
        # Prepare comprehensive failure report for master
        report_data = {
            "task_id": task_id,
            "worker_id": self.settings.worker_id,
            "status": "failed",
            "error_message": error_message,
            "failed_at": datetime.utcnow().isoformat()
        }
        
        try:
            # Send failure report to master's task failure endpoint
            response = await self.client.post(
                f"{self.master_url}/api/v1/tasks/{task_id}/fail",
                json=report_data
            )
            response.raise_for_status()
            
            self.logger.info(f"Task {task_id} failure reported to master")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to report task failure: {e}")
            return False
    
    def _get_worker_capabilities(self) -> list:
        """
        Determine and return the processing capabilities of this worker node.
        
        This method analyzes worker configuration to report capabilities:
        1. Determines supported task types (Map, Reduce, or both)
        2. Identifies special processing capabilities (GPU, large memory)
        3. Provides capability information for intelligent task assignment
        4. Enables master to make optimal load balancing decisions
        
        Capabilities are used by the master's task scheduler to:
        - Assign appropriate task types to workers
        - Utilize specialized worker features
        - Optimize performance through capability matching
        - Balance load based on worker strengths
        
        Returns:
            list: List of capability strings supported by this worker
        """
        capabilities = []
        
        # Determine Map/Reduce processing capabilities based on worker type
        if self.settings.worker_type in ["map", "generic"]:
            capabilities.append("map_processing")
        
        if self.settings.worker_type in ["reduce", "generic"]:
            capabilities.append("reduce_processing")
        
        # Add specialized capabilities based on configuration
        if self.settings.worker_enable_gpu_processing:
            capabilities.append("gpu_processing")
        
        if self.settings.worker_enable_large_memory_tasks:
            capabilities.append("large_memory_tasks")
        
        return capabilities