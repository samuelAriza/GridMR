# Standard library imports for enumeration, UUID generation, and datetime handling
from enum import Enum
from datetime import datetime
import uuid

# Third-party imports for data validation and type hints
from typing import Dict, List, Optional, Any
from pydantic import BaseModel

class JobStatus(str, Enum):
    """
    Enumeration of possible MapReduce job states throughout the execution lifecycle.
    
    States represent the complete job workflow:
    - PENDING: Job submitted but not yet started
    - RUNNING: Job is actively executing (Map or Reduce phases)
    - COMPLETED: Job finished successfully with all tasks completed
    - FAILED: Job failed due to task failures or system errors
    - CANCELLED: Job was manually cancelled by user or system
    """
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class MapReduceJob(BaseModel):
    """
    Data model representing a complete MapReduce job configuration and state.
    
    This model encapsulates all information needed to execute a MapReduce job:
    - Job identification and metadata
    - User-defined Map and Reduce function code
    - Input/output data paths and processing parameters
    - Execution state tracking and error information
    - Timing information for performance monitoring
    
    The model uses Pydantic for automatic validation and serialization,
    ensuring data integrity across the distributed system.
    """
    
    # === Job Identification ===
    job_id: str = None                          # Unique identifier, auto-generated if not provided
    client_id: str                              # Identifier for the client submitting the job
    job_name: str                               # Human-readable name for the job
    
    # === MapReduce Function Definitions ===
    map_function: str                           # Python code string for the Map function
    reduce_function: str                        # Python code string for the Reduce function
    
    # === Data Configuration ===
    input_data_path: str                        # Path to input data files or directory
    output_data_path: str                       # Path where final results will be written
    split_size: int = 64                        # Size of data splits in MB for Map tasks
    num_reducers: int = 1                       # Number of Reduce tasks to create
    parameters: Dict[str, Any] = {}             # Additional parameters passed to Map/Reduce functions
    
    # === Job State Tracking ===
    status: JobStatus = JobStatus.PENDING       # Current execution state
    error_message: Optional[str] = None         # Error details if job failed
    
    # === Timing Information ===
    created_at: datetime = None                 # When job was submitted
    started_at: Optional[datetime] = None       # When job execution began
    completed_at: Optional[datetime] = None     # When job finished (success or failure)
    
    def __init__(self, **data):
        """
        Initialize MapReduceJob with automatic ID and timestamp generation.
        
        Automatically generates:
        - Unique job_id using UUID4 if not provided
        - Creation timestamp if not provided
        
        Args:
            **data: Job configuration parameters
        """
        # Auto-generate unique job ID if not provided
        if data.get('job_id') is None:
            data['job_id'] = str(uuid.uuid4())
            
        # Auto-set creation timestamp if not provided
        if data.get('created_at') is None:
            data['created_at'] = datetime.utcnow()
            
        super().__init__(**data)