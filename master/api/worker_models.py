from pydantic import BaseModel, Field
from typing import Optional


class WorkerRegistrationRequest(BaseModel):
    """
    Represents the payload sent by a Worker node to the Master node
    during the registration process. It provides metadata about the
    Worker’s resources and capabilities.
    """
    worker_id: str = Field(..., description="Unique identifier of the worker")
    worker_type: str = Field(..., description="Worker type: map, reduce, or generic")
    port: int = Field(..., description="Port where the worker service is running")
    cpu_cores: Optional[int] = Field(None, description="Number of CPU cores assigned to the worker")
    memory_mb: Optional[int] = Field(None, description="Amount of RAM assigned to the worker (in MB)")
    enable_large_memory_tasks: Optional[bool] = Field(
        False, 
        description="Indicates whether the worker can handle large memory tasks"
    )
    host: Optional[str] = Field(None, description="Host address or hostname of the worker")