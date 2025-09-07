# FastAPI imports for routing, HTTP handling, and dependency injection
from fastapi import APIRouter, HTTPException, status, Request

# Internal API models for request/response validation
from api.models import JobSubmissionRequest, JobStatusResponse
from api.worker_models import WorkerRegistrationRequest

# Core business logic components
from core.job_manager import JobManager
from core.task_scheduler import TaskScheduler
from services.worker_registry import WorkerRegistry

# Data models for job and task management
from models.job import MapReduceJob
from models.task import TaskStatus
from models.worker import Worker

# Utility imports
from utils.logger import get_logger

# ===== API Router Configuration =====
router = APIRouter()

# Initialize core system components with proper dependencies
worker_registry = WorkerRegistry()          # Manages available workers
task_scheduler = TaskScheduler(worker_registry)  # Assigns tasks to workers
job_manager = JobManager(task_scheduler)     # Orchestrates complete MapReduce jobs

logger = get_logger(__name__)

# ===== Worker Management Endpoints =====

@router.post("/workers/register")
async def register_worker(worker: WorkerRegistrationRequest, request: Request):
    """
    Professional endpoint for worker registration with the master node.
    
    This endpoint handles worker nodes joining the distributed system:
    1. Validates worker registration request data
    2. Extracts client IP address for network communication
    3. Creates Worker object with capacity and configuration
    4. Registers worker in the worker registry for task assignment
    
    Workers must register before they can receive task assignments.
    The master uses worker capacity information for load balancing.
    
    Args:
        worker: Worker registration data including ID, host, port, and capabilities
        request: FastAPI request object for extracting client information
        
    Returns:
        Dict with registration status and assigned worker ID
        
    Side Effects:
        - Adds worker to worker_registry for task assignment
        - Logs worker registration for monitoring
    """
    # Extract client IP address for network communication
    client_host = request.client.host if request.client else None
    
    # Create comprehensive Worker object with capacity configuration
    worker_obj = Worker(
        worker_id=worker.worker_id,
        host=worker.host or client_host,  # Use provided host or client IP
        port=worker.port,
        capacity={
            "cpu_cores": worker.cpu_cores or 1,
            "memory_mb": worker.memory_mb or 1024,
            # Configure concurrent task capacity based on memory capabilities
            "max_concurrent_tasks": worker.enable_large_memory_tasks and 8 or 4
        },
        status="available"
    )
    
    # Register worker in the distributed system
    worker_registry.add_worker(worker_obj)
    logger.info(f"Worker registered: {worker_obj}")
    
    return {"status": "registered", "worker_id": worker.worker_id}

# ===== Job Management Endpoints =====

@router.post("/jobs", response_model=dict)
async def submit_job(job_request: JobSubmissionRequest):
    """
    Submit a new MapReduce job for distributed processing.
    
    This endpoint serves as the main entry point for MapReduce job submission:
    1. Validates job request data and parameters
    2. Creates MapReduceJob object with complete configuration
    3. Submits job to JobManager for processing
    4. Initiates Map task creation and scheduling
    
    The job submission process automatically begins the MapReduce workflow
    by creating and scheduling Map tasks across available workers.
    
    Args:
        job_request: Complete job specification including functions and data paths
        
    Returns:
        Dict containing assigned job_id and submission status
        
    Raises:
        HTTPException: If job submission fails due to validation or system errors
    """
    try:
        # Create comprehensive MapReduceJob with all configuration
        job = MapReduceJob(
            client_id=job_request.client_id,
            job_name=job_request.job_name,
            map_function=job_request.map_function,
            reduce_function=job_request.reduce_function,
            input_data_path=job_request.input_data_path,
            output_data_path=job_request.output_data_path,
            split_size=job_request.split_size,
            num_reducers=job_request.num_reducers,
            parameters=job_request.parameters
        )
        
        # Submit job for processing and get assigned ID
        job_id = await job_manager.submit_job(job)
        return {"job_id": job_id, "status": "submitted"}
        
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Retrieve comprehensive status information for a MapReduce job.
    
    This endpoint provides detailed job monitoring capabilities:
    1. Looks up job by unique identifier
    2. Gathers all associated task information
    3. Calculates task completion statistics
    4. Returns comprehensive status response
    
    Used by clients for job monitoring and progress tracking.
    
    Args:
        job_id: Unique identifier for the job
        
    Returns:
        JobStatusResponse with complete job and task statistics
        
    Raises:
        HTTPException: If job is not found (404) or system error (500)
    """
    # Look up job in the job manager
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    # Gather all task information for statistics
    tasks = job_manager.get_job_tasks(job_id)
    
    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        job_name=job.job_name,
        created_at=job.created_at,
        total_tasks=len(tasks),
        completed_tasks=len([t for t in tasks if t.status == "completed"]),
        failed_tasks=len([t for t in tasks if t.status == "failed"])
    )

# ===== Task Status Update Endpoints =====

@router.post("/tasks/{task_id}/complete")
async def complete_task(task_id: str, completion_data: dict):
    """
    Endpoint for workers to report successful task completion.
    
    This endpoint handles task completion notifications from workers:
    1. Validates that the task exists in the system
    2. Updates task status to COMPLETED with execution results
    3. Triggers MapReduce phase transition checks (Map→Reduce, Reduce→Job completion)
    4. Records task completion for monitoring and debugging
    
    This is a critical endpoint in the MapReduce workflow as it enables
    automatic phase transitions and job completion detection.
    
    Args:
        task_id: Unique identifier for the completed task
        completion_data: Dict containing task results and execution metadata
        
    Returns:
        Dict with success confirmation
        
    Raises:
        HTTPException: If task not found (404) or system error (500)
        
    Side Effects:
        - Updates task status and results in job_manager
        - May trigger Reduce task creation (if Map task completed)
        - May trigger job completion (if all Reduce tasks completed)
    """
    try:
        # Validate task exists in the system
        task = job_manager.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found"
            )
        
        # Update task status and store execution results
        await job_manager.update_task_status(
            task_id, 
            TaskStatus.COMPLETED, 
            completion_data.get("result")  # Task execution results and metadata
        )
        
        logger.info(f"Task {task_id} marked as completed")
        return {"status": "success", "message": "Task completion recorded"}
        
    except Exception as e:
        logger.error(f"Error completing task {task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/tasks/{task_id}/fail")
async def fail_task(task_id: str, failure_data: dict):
    """
    Endpoint for workers to report task execution failures.
    
    This endpoint handles task failure notifications from workers:
    1. Validates that the task exists in the system
    2. Records error message and failure details
    3. Updates task status to FAILED
    4. May trigger job failure if critical tasks fail
    
    Task failures are logged for debugging and may trigger retry logic
    depending on the task's retry configuration.
    
    Args:
        task_id: Unique identifier for the failed task
        failure_data: Dict containing error message and failure details
        
    Returns:
        Dict with failure recording confirmation
        
    Raises:
        HTTPException: If task not found (404) or system error (500)
        
    Side Effects:
        - Updates task status to FAILED in job_manager
        - Records error message for debugging
        - May trigger job failure for critical task failures
    """
    try:
        # Validate task exists in the system
        task = job_manager.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found"
            )
        
        # Record failure details and update task status
        task.error_message = failure_data.get("error_message", "Unknown error")
        await job_manager.update_task_status(task_id, TaskStatus.FAILED)
        
        logger.info(f"Task {task_id} marked as failed: {task.error_message}")
        return {"status": "success", "message": "Task failure recorded"}
        
    except Exception as e:
        logger.error(f"Error failing task {task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# ===== System Health Endpoints =====

@router.get("/health")
async def health_check():
    """
    System health check endpoint for monitoring and load balancing.
    
    This endpoint provides basic system health information for:
    - Load balancer health checks
    - Monitoring system availability
    - Service discovery and registration
    
    Returns:
        Dict with service health status and identification
    """
    return {"status": "healthy", "service": "GridMR Master"}