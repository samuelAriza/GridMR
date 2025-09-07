# Standard library imports for enumeration, UUID generation, and datetime handling
from enum import Enum
from datetime import datetime
import uuid

# Third-party imports for data validation and type hints
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

class TaskType(str, Enum):
    """
    Enumeration of MapReduce task types.
    
    - MAP: Tasks that process input data and produce intermediate key-value pairs
    - REDUCE: Tasks that aggregate intermediate data by key and produce final results
    """
    MAP = "map"
    REDUCE = "reduce"

class TaskStatus(str, Enum):
    """
    Enumeration of possible task states during execution lifecycle.
    
    Task lifecycle flow:
    PENDING → ASSIGNED → RUNNING → COMPLETED/FAILED
    
    - PENDING: Task created but not yet assigned to a worker
    - ASSIGNED: Task assigned to a worker but not yet started
    - RUNNING: Task actively executing on a worker
    - COMPLETED: Task finished successfully
    - FAILED: Task failed and may be eligible for retry
    """
    PENDING = "pending"
    ASSIGNED = "assigned" 
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class Task(BaseModel):
    """
    Data model representing a single MapReduce task (Map or Reduce).
    
    This model encapsulates all information needed to execute a task:
    - Task identification and type classification
    - Input data specifications and output destinations
    - Function code and execution parameters
    - Worker assignment and execution state tracking
    - Retry logic and error handling information
    - Execution results and timing data
    
    Tasks are created by the JobManager and assigned to workers by the TaskScheduler.
    Workers update task status and results through the master's API endpoints.
    """
    
    # === Task Identification ===
    task_id: str = None                         # Unique identifier, auto-generated if not provided
    job_id: str                                 # ID of the parent MapReduce job
    task_type: TaskType                         # MAP or REDUCE task type
    
    # === Data Configuration ===
    input_splits: List[str]                     # List of input file paths or data splits
    output_path: str                            # Path where task should write results
    
    # === Function Definitions ===
    map_function: Optional[str] = None          # Python code for Map function (Map tasks only)
    reduce_function: Optional[str] = None       # Python code for Reduce function (Reduce tasks only)
    parameters: Dict[str, Any] = {}             # Additional parameters for function execution
    
    # === Worker Assignment ===
    assigned_worker: Optional[str] = None       # ID of worker assigned to execute this task
    
    # === Task State Tracking ===
    status: TaskStatus = TaskStatus.PENDING     # Current execution state
    error_message: Optional[str] = None         # Error details if task failed
    result: Optional[Dict[str, Any]] = None     # Task execution results and metadata
    
    # === Timing Information ===
    created_at: datetime = None                 # When task was created
    assigned_at: Optional[datetime] = None      # When task was assigned to a worker
    completed_at: Optional[datetime] = None     # When task finished (success or failure)
    
    # === Retry Configuration ===
    retry_count: int = 0                        # Number of times this task has been retried
    max_retries: int = 3                        # Maximum retry attempts before permanent failure
    
    def __init__(self, **data):
        """
        Initialize Task with automatic ID and timestamp generation.
        
        Automatically generates:
        - Unique task_id using UUID4 if not provided
        - Creation timestamp if not provided
        
        Args:
            **data: Task configuration parameters
        """
        # Auto-generate unique task ID if not provided
        if data.get('task_id') is None:
            data['task_id'] = str(uuid.uuid4())
            
        # Auto-set creation timestamp if not provided
        if data.get('created_at') is None:
            data['created_at'] = datetime.utcnow()
            
        super().__init__(**data)