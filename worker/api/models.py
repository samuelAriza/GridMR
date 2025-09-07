# Third-party imports for data validation and type hints
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime

class TaskAssignmentRequest(BaseModel):
    """
    Request model for task assignment from master to worker.
    
    This model represents a complete task assignment containing all information
    needed for a worker to execute a Map or Reduce task:
    - Task identification and type classification
    - User-defined function code (Map and/or Reduce functions)
    - Input data specifications and output destinations
    - Execution parameters and priority settings
    - Optional timeout configurations for task execution
    
    Used by the master node when assigning tasks to available workers
    through the worker's task assignment API endpoint.
    """
    
    # === Task Identification ===
    task_id: str                                        # Unique identifier for this task
    job_id: str                                        # Parent MapReduce job identifier
    task_type: str = Field(..., pattern="^(map|reduce)$")  # Task type: "map" or "reduce"
    
    # === Function Definitions ===
    map_function: Optional[str] = None                 # Python code for Map function (Map tasks only)
    reduce_function: Optional[str] = None              # Python code for Reduce function (Reduce tasks only)
    
    # === Data Configuration ===
    input_splits: List[str]                           # List of input file paths or data splits
    output_path: str                                  # Path where task should write results
    parameters: Dict[str, Any] = {}                   # Additional parameters for function execution
    
    # === Execution Configuration ===
    priority: int = 1                                 # Task priority for scheduling (higher = more priority)
    timeout_seconds: Optional[int] = None             # Maximum execution time before task timeout
    
    class Config:
        """Pydantic configuration with example schema for API documentation."""
        json_schema_extra = {
            "example": {
                "task_id": "task-001",
                "job_id": "job-123",
                "task_type": "map",
                "map_function": "def map_func(key, value): return [(word, 1) for word in value.split()]",
                "input_splits": ["/data/split_001", "/data/split_002"],
                "output_path": "/output/map_result_001",
                "parameters": {"case_sensitive": False},
                "priority": 1
            }
        }

class TaskStatusResponse(BaseModel):
    """
    Response model for task status queries from master or monitoring systems.
    
    This model provides comprehensive information about task execution state:
    - Current execution status and timing information
    - Worker assignment and execution progress
    - Error details for failed tasks
    - Task results and performance metrics
    
    Used by the master node to track task progress and by monitoring
    systems to observe task execution across the distributed system.
    """
    
    # === Task Identification ===
    task_id: str                                      # Unique task identifier
    status: str                                       # Current task status (pending, running, completed, failed)
    worker_id: str                                    # ID of worker executing this task
    
    # === Timing Information ===
    assigned_at: Optional[datetime] = None            # When task was assigned to this worker
    started_at: Optional[datetime] = None             # When task execution began
    completed_at: Optional[datetime] = None           # When task finished (success or failure)
    execution_time: Optional[float] = None            # Total execution time in seconds
    
    # === Execution Results ===
    error_message: Optional[str] = None               # Error details if task failed
    result: Optional[Dict[str, Any]] = None           # Task execution results and metadata
    progress_percentage: float = 0.0                  # Execution progress (0.0 to 100.0)

class WorkerStatusResponse(BaseModel):
    """
    Comprehensive response model for worker node status information.
    
    This model provides complete worker health and performance data:
    - Worker identification and network configuration
    - Task execution statistics and current workload
    - System resource utilization (CPU, memory, disk)
    - Health status and overload detection
    - Active task details for monitoring
    
    Used by the master node for worker health monitoring, load balancing
    decisions, and task assignment optimization.
    """
    
    # === Worker Identification ===
    worker_id: str                                    # Unique worker identifier
    worker_type: str                                  # Worker type classification
    status: str                                       # Worker operational status
    host: str                                        # Worker network host address
    port: int                                        # Worker API port
    
    # === Operational Metrics ===
    uptime_seconds: float                            # Worker uptime since startup
    last_heartbeat: Optional[datetime] = None        # Last successful health check
    
    # === Task Statistics ===
    active_tasks: int                                # Currently executing tasks
    queued_tasks: int                                # Tasks waiting for execution
    completed_tasks: int                             # Total tasks completed successfully
    failed_tasks: int                                # Total tasks that failed
    active_task_details: List[Dict[str, Any]] = []   # Detailed information about active tasks
    
    # === Resource Utilization ===
    cpu_usage: float                                 # Current CPU utilization percentage
    memory_usage: float                              # Current memory utilization percentage
    disk_usage: float                                # Current disk utilization percentage
    available_memory_mb: float                       # Available memory in megabytes
    
    # === Health Assessment ===
    is_overloaded: bool                              # Whether worker is overloaded and should not receive new tasks

class HealthCheckResponse(BaseModel):
    """
    Response model for worker health check endpoints.
    
    This model provides standardized health information for:
    - Load balancer health checks
    - System monitoring and alerting
    - Service discovery and availability tracking
    - Detailed health assessment with individual check results
    
    Health checks verify worker availability, resource capacity,
    and system component functionality.
    """
    
    # === Health Status ===
    status: str                                      # Overall health status: "healthy", "unhealthy", "degraded"
    worker_id: str                                   # Worker identifier for health tracking
    timestamp: datetime                              # When health check was performed
    
    # === System Information ===
    uptime_seconds: float                            # Worker uptime since startup
    version: str = "1.0.0"                          # Worker software version
    
    # === Detailed Health Checks ===
    checks: List[Dict[str, Any]]                     # Individual component health check results

class TaskExecutionResult(BaseModel):
    """
    Model for task execution results reported back to the master node.
    
    This model encapsulates the complete results of task execution:
    - Task completion status and execution timing
    - Output file locations for result retrieval
    - Processing statistics and performance metrics
    - Error details for debugging failed tasks
    
    Used by workers to report task completion to the master node,
    enabling automatic phase transitions and result consolidation.
    """
    
    # === Task Identification ===
    task_id: str                                     # Unique task identifier
    status: str                                      # Final task status (completed, failed)
    
    # === Execution Metrics ===
    execution_time: float                            # Total execution time in seconds
    records_processed: Optional[int] = None          # Number of data records processed
    
    # === Output Information ===
    output_files: Optional[List[str]] = None         # List of output file paths created
    
    # === Error Handling ===
    error_details: Optional[str] = None              # Detailed error information if task failed
    
    # === Performance Metrics ===
    metrics: Dict[str, Any] = {}                     # Additional performance and debugging metrics