from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime


class JobSubmissionRequest(BaseModel):
    """
    Represents the request payload sent by a client to submit
    a new MapReduce job to the Master node.
    """
    client_id: str = Field(..., description="ID of the client submitting the job")
    job_name: str = Field(..., description="Name of the MapReduce job")
    map_function: str = Field(..., description="Source code of the map function")
    reduce_function: str = Field(..., description="Source code of the reduce function")
    input_data_path: str = Field(..., description="Path to the input dataset")
    output_data_path: str = Field(..., description="Path where results will be stored")
    split_size: int = Field(64, description="Split size in MB for dividing the input data")
    num_reducers: int = Field(1, description="Number of reducers to be used")
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional additional parameters for the job execution"
    )


class JobStatusResponse(BaseModel):
    """
    Represents the response payload returned by the Master node
    when a client queries the status of a submitted job.
    """
    job_id: str = Field(..., description="Unique identifier of the job")
    status: str = Field(..., description="Current execution status of the job")
    job_name: str = Field(..., description="Name of the job")
    created_at: datetime = Field(..., description="Job creation timestamp")
    total_tasks: int = Field(..., description="Total number of tasks created for the job")
    completed_tasks: int = Field(..., description="Number of tasks completed successfully")
    failed_tasks: int = Field(..., description="Number of tasks that failed during execution")