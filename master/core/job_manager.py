# Standard library imports for type hints, async operations, and file system operations
from typing import Dict, List, Optional
import asyncio
import os
from pathlib import Path

# Internal imports for MapReduce job management, data processing, and logging
from models.job import MapReduceJob, JobStatus
from models.task import Task, TaskType, TaskStatus
from core.data_splitter import DataSplitter
from utils.logger import get_logger

class JobManager:
    """
    Central orchestrator for complete MapReduce job lifecycle management.
    
    This class serves as the main coordinator for MapReduce operations, handling:
    - Job submission and initialization
    - Map task creation and scheduling
    - Automatic Reduce task creation after Map phase completion
    - Task status monitoring and phase transitions
    - Final result consolidation
    
    The JobManager implements a complete MapReduce workflow with automatic
    phase transitions, ensuring proper task dependencies and error handling.
    """
    
    def __init__(self, task_scheduler=None):
        """
        Initialize the JobManager with all necessary tracking structures.
        
        Args:
            task_scheduler: Optional task scheduler for automatic task assignment
                          If None, tasks must be manually scheduled
        """
        # Core job tracking: Maps job_id to MapReduceJob instances
        self.jobs: Dict[str, MapReduceJob] = {}
        
        # Task registry: Maps task_id to Task instances for all tasks
        self.tasks: Dict[str, Task] = {}
        
        # Job-to-task relationships: Maps job_id to list of all task_ids
        self.job_tasks: Dict[str, List[str]] = {}
        
        # Map phase tracking: Maps job_id to list of map task_ids only
        self.job_map_tasks: Dict[str, List[str]] = {}
        
        # Reduce phase tracking: Maps job_id to list of reduce task_ids only
        self.job_reduce_tasks: Dict[str, List[str]] = {}
        
        # Data processing utilities
        self.data_splitter = DataSplitter()
        self.logger = get_logger(__name__)
        
        # Optional task scheduler for automatic task assignment
        self.task_scheduler = task_scheduler
    
    async def submit_job(self, job: MapReduceJob) -> str:
        """
        Submit a new MapReduce job for processing.
        
        This method initializes a job for execution by:
        1. Registering the job in the job manager
        2. Setting up tracking structures for Map and Reduce tasks
        3. Transitioning job status to RUNNING
        4. Creating and scheduling initial Map tasks
        
        Args:
            job: The MapReduceJob instance containing all job configuration
                including functions, input data, and parameters
        
        Returns:
            str: The unique job_id for tracking and status queries
            
        Raises:
            Exception: If job submission fails, job status is set to FAILED
                      and the exception is re-raised for caller handling
        """
        try:
            # Register job in the job manager
            self.jobs[job.job_id] = job
            
            # Initialize tracking structures for Map and Reduce phases
            self.job_map_tasks[job.job_id] = []
            self.job_reduce_tasks[job.job_id] = []
            
            self.logger.info(f"Job {job.job_id} submitted: {job.job_name}")
            
            # Transition job to running state and create Map tasks
            job.status = JobStatus.RUNNING
            await self._create_map_tasks(job)
            
            return job.job_id
            
        except Exception as e:
            # Handle submission failure by marking job as failed
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            self.logger.error(f"Failed to submit job {job.job_id}: {e}")
            raise
    
    async def _create_map_tasks(self, job: MapReduceJob):
        """
        Create and schedule Map tasks by splitting input data.
        
        This method implements the first phase of MapReduce processing:
        1. Split input data into manageable chunks using the DataSplitter
        2. Create a Map task for each data split
        3. Configure each task with job parameters and functions
        4. Schedule tasks for execution if a task scheduler is available
        
        Map tasks are created with standardized output paths that will be
        used later by Reduce tasks for input data discovery.
        
        Args:
            job: The MapReduceJob containing input data and configuration
            
        Side Effects:
            - Updates self.tasks with new Task instances
            - Updates self.job_tasks and self.job_map_tasks tracking
            - Schedules tasks via task_scheduler if available
        """
        # Split input data into processable chunks
        splits = await self.data_splitter.split_data(
            job.input_data_path, 
            job.split_size
        )
        
        map_tasks = []
        
        # Create a Map task for each data split
        for i, split_path in enumerate(splits):
            # Configure task with standardized output path for Reduce discovery
            task = Task(
                job_id=job.job_id,
                task_type=TaskType.MAP,
                input_splits=[split_path],
                output_path=f"{job.output_data_path}_map_output_{job.job_id}",
                map_function=job.map_function,
                reduce_function=job.reduce_function,
                parameters={**job.parameters, "num_reducers": job.num_reducers}
            )
            
            map_tasks.append(task.task_id)
            self.tasks[task.task_id] = task
            
            # Schedule task immediately upon creation if scheduler available
            if self.task_scheduler:
                await self.task_scheduler.schedule_task(task)
        
        # Update job tracking structures
        self.job_tasks[job.job_id] = map_tasks.copy()
        self.job_map_tasks[job.job_id] = map_tasks
        
        self.logger.info(f"Created and scheduled {len(map_tasks)} map tasks for job {job.job_id}")
    
    async def _create_reduce_tasks(self, job: MapReduceJob):
        """
        Create and schedule Reduce tasks after Map phase completion.
        
        This method implements the second phase of MapReduce processing:
        1. Discover and collect all Map task output files from the file system
        2. Parse partition information from Map output file names
        3. Group Map outputs by partition ID for proper Reduce input aggregation
        4. Create one Reduce task per partition with all relevant input files
        5. Schedule Reduce tasks for execution
        
        The method uses standardized file paths that match the Map processor
        output conventions: /data/map_output_{task_id}/part-{partition_id}.json
        
        Args:
            job: The MapReduceJob containing configuration for Reduce tasks
            
        Side Effects:
            - Updates self.tasks with new Reduce Task instances
            - Updates self.job_tasks and self.job_reduce_tasks tracking
            - Schedules Reduce tasks via task_scheduler if available
            
        Notes:
            - If no Map outputs are found, logs error and searches alternative locations
            - Handles partition ID parsing errors gracefully with warnings
            - Creates one Reduce task per unique partition ID discovered
        """
        # Collect all Map task output files from the file system
        map_output_files = []
        
        # Search for output files in the standardized locations used by workers
        for task_id in self.job_map_tasks[job.job_id]:
            # This path matches the MapProcessor output convention in workers
            task_output_dir = f"/data/map_output_{task_id}"
            
            if os.path.exists(task_output_dir):
                try:
                    # Scan directory for partition files (part-*.json)
                    for file in os.listdir(task_output_dir):
                        if file.startswith("part-") and file.endswith(".json"):
                            full_path = f"{task_output_dir}/{file}"
                            map_output_files.append(full_path)
                            self.logger.debug(f"Found map output file: {full_path}")
                except Exception as e:
                    self.logger.error(f"Error accessing map output directory {task_output_dir}: {e}")
            else:
                self.logger.warning(f"Map output directory not found: {task_output_dir}")
        
        self.logger.info(f"Found {len(map_output_files)} map output files for reduce tasks")
        
        # Handle case where no Map outputs are found (error condition)
        if not map_output_files:
            self.logger.error(f"No map output files found for job {job.job_id}. Cannot create reduce tasks.")
            
            # Debug: Search for Map outputs in alternative locations
            self.logger.info("Searching for map outputs in alternative locations...")
            for root, dirs, files in os.walk("/data"):
                for file in files:
                    if file.startswith("part-") and file.endswith(".json"):
                        self.logger.info(f"Found potential map output: {os.path.join(root, file)}")
            return
        
        # Group Map output files by partition ID for Reduce input aggregation
        partition_groups = {}
        for file_path in map_output_files:
            try:
                filename = os.path.basename(file_path)
                # Extract partition ID from filename format: part-00001.json
                partition_id = int(filename.split('-')[1].split('.')[0])
                
                if partition_id not in partition_groups:
                    partition_groups[partition_id] = []
                partition_groups[partition_id].append(file_path)
            except (IndexError, ValueError) as e:
                self.logger.warning(f"Could not parse partition ID from file {file_path}: {e}")
                continue
        
        self.logger.info(f"Grouped map outputs into {len(partition_groups)} partitions: {list(partition_groups.keys())}")
        
        # Create one Reduce task per partition with all relevant input files
        reduce_tasks = []
        for partition_id, input_files in partition_groups.items():
            # Configure Reduce task with partition-specific input and output
            task = Task(
                job_id=job.job_id,
                task_type=TaskType.REDUCE,
                input_splits=input_files,  # All Map outputs for this partition
                output_path=f"{job.output_data_path}_reduce_output_{partition_id}",
                map_function=job.map_function,
                reduce_function=job.reduce_function,
                parameters={**job.parameters, "partition_id": partition_id}
            )
            
            reduce_tasks.append(task.task_id)
            self.tasks[task.task_id] = task
            
            # Schedule task immediately upon creation if scheduler available
            if self.task_scheduler:
                await self.task_scheduler.schedule_task(task)
        
        # Update job tracking structures with new Reduce tasks
        self.job_reduce_tasks[job.job_id] = reduce_tasks
        all_tasks = self.job_tasks[job.job_id] + reduce_tasks
        self.job_tasks[job.job_id] = all_tasks
        
        self.logger.info(f"Created and scheduled {len(reduce_tasks)} reduce tasks for job {job.job_id}")
    
    async def update_task_status(self, task_id: str, status: TaskStatus, result: Optional[Dict] = None):
        """
        Update task status and trigger phase transition checks.
        
        This method serves as the central point for task status updates from workers.
        It handles:
        1. Updating task status and storing results from task execution
        2. Triggering Map phase completion checks when Map tasks complete
        3. Triggering Reduce phase completion checks when Reduce tasks complete
        4. Automatic phase transitions (Map → Reduce → Job completion)
        
        The method implements the core MapReduce workflow orchestration by
        automatically detecting when phases complete and triggering the next phase.
        
        Args:
            task_id: Unique identifier for the task being updated
            status: New TaskStatus (COMPLETED, FAILED, etc.)
            result: Optional dictionary containing task execution results,
                   including output files and metrics
        
        Side Effects:
            - Updates task status in self.tasks
            - May trigger Map phase completion and Reduce task creation
            - May trigger Reduce phase completion and job completion
        """
        # Validate task exists in our registry
        if task_id not in self.tasks:
            self.logger.warning(f"Task {task_id} not found for status update")
            return
        
        # Update task status and store execution results
        task = self.tasks[task_id]
        old_status = task.status
        task.status = status
        
        if result:
            task.result = result
        
        self.logger.info(f"Task {task_id} status updated: {old_status} -> {status}")
        
        # Trigger phase transition checks based on task type and status
        if task.task_type == TaskType.MAP and status == TaskStatus.COMPLETED:
            # Map task completed - check if all Map tasks are done to start Reduce
            await self._check_map_phase_completion(task.job_id)
        
        if task.task_type == TaskType.REDUCE and status == TaskStatus.COMPLETED:
            # Reduce task completed - check if all Reduce tasks are done to complete job
            await self._check_reduce_phase_completion(task.job_id)
    
    async def _check_map_phase_completion(self, job_id: str):
        """
        Check if all Map tasks are complete and trigger Reduce phase creation.
        
        This method implements the Map → Reduce phase transition logic:
        1. Count completed and failed Map tasks for the job
        2. If any Map tasks failed, mark the entire job as failed
        3. If all Map tasks completed successfully, create Reduce tasks
        
        This ensures that the Reduce phase only begins after all Map tasks
        have successfully completed, maintaining MapReduce correctness.
        
        Args:
            job_id: Unique identifier for the job being checked
            
        Side Effects:
            - May set job status to FAILED if Map tasks failed
            - May trigger Reduce task creation if all Map tasks completed
        """
        map_task_ids = self.job_map_tasks.get(job_id, [])
        
        # Skip check if no Map tasks exist (shouldn't happen in normal flow)
        if not map_task_ids:
            return
        
        # Count completed and failed Map tasks
        completed_map_tasks = [
            task_id for task_id in map_task_ids 
            if self.tasks[task_id].status == TaskStatus.COMPLETED
        ]
        
        failed_map_tasks = [
            task_id for task_id in map_task_ids 
            if self.tasks[task_id].status == TaskStatus.FAILED
        ]
        
        self.logger.info(f"Job {job_id} Map phase progress: {len(completed_map_tasks)}/{len(map_task_ids)} completed, {len(failed_map_tasks)} failed")
        
        # Handle Map phase failure - any failed task fails the entire job
        if failed_map_tasks:
            job = self.jobs[job_id]
            job.status = JobStatus.FAILED
            job.error_message = f"Map phase failed: {len(failed_map_tasks)} tasks failed"
            self.logger.error(f"Job {job_id} failed in Map phase")
            return
        
        # Handle Map phase completion - all tasks completed successfully
        if len(completed_map_tasks) == len(map_task_ids):
            self.logger.info(f"All Map tasks completed for job {job_id}. Creating Reduce tasks...")
            job = self.jobs[job_id]
            await self._create_reduce_tasks(job)
    
    async def _check_reduce_phase_completion(self, job_id: str):
        """
        Check if all Reduce tasks are complete and finalize the job.
        
        This method implements the Reduce phase completion logic:
        1. Count completed and failed Reduce tasks for the job
        2. If any Reduce tasks failed, mark the entire job as failed
        3. If all Reduce tasks completed successfully, mark job as completed
        4. Trigger final result consolidation for successful job completion
        
        This completes the MapReduce workflow by ensuring all Reduce tasks
        finish before marking the job as complete and consolidating results.
        
        Args:
            job_id: Unique identifier for the job being checked
            
        Side Effects:
            - May set job status to FAILED if Reduce tasks failed
            - May set job status to COMPLETED if all Reduce tasks completed
            - May trigger final result consolidation for completed jobs
        """
        reduce_task_ids = self.job_reduce_tasks.get(job_id, [])
        
        # Skip check if no Reduce tasks exist (shouldn't happen in normal flow)
        if not reduce_task_ids:
            return
        
        # Count completed and failed Reduce tasks
        completed_reduce_tasks = [
            task_id for task_id in reduce_task_ids 
            if self.tasks[task_id].status == TaskStatus.COMPLETED
        ]
        
        failed_reduce_tasks = [
            task_id for task_id in reduce_task_ids 
            if self.tasks[task_id].status == TaskStatus.FAILED
        ]
        
        self.logger.info(f"Job {job_id} Reduce phase progress: {len(completed_reduce_tasks)}/{len(reduce_task_ids)} completed, {len(failed_reduce_tasks)} failed")
        
        # Handle Reduce phase failure - any failed task fails the entire job
        if failed_reduce_tasks:
            job = self.jobs[job_id]
            job.status = JobStatus.FAILED
            job.error_message = f"Reduce phase failed: {len(failed_reduce_tasks)} tasks failed"
            self.logger.error(f"Job {job_id} failed in Reduce phase")
            return
        
        # Handle Reduce phase completion - all tasks completed successfully
        if len(completed_reduce_tasks) == len(reduce_task_ids):
            job = self.jobs[job_id]
            job.status = JobStatus.COMPLETED
            self.logger.info(f"Job {job_id} completed successfully! All Map and Reduce tasks finished.")
            
            # Consolidate final results from all Reduce outputs
            await self._consolidate_final_results(job)
    
    async def _consolidate_final_results(self, job: MapReduceJob):
        """
        Consolidate final results from all Reduce tasks into a single output file.
        
        This method implements the final step of MapReduce processing:
        1. Collect output file paths from all completed Reduce tasks
        2. Read and aggregate all Reduce output records
        3. Write consolidated results to the job's final output file
        4. Handle both 'output_file' (single) and 'output_files' (multiple) formats
        
        The consolidation process ensures that distributed Reduce outputs
        are combined into a single, easily accessible result file for the user.
        
        Args:
            job: The completed MapReduceJob whose results need consolidation
            
        Side Effects:
            - Creates the final consolidated output file at job.output_data_path
            - Logs the number of consolidated records
            - Logs errors if consolidation fails
            
        Notes:
            - Handles both task.result['output_file'] and task.result['output_files']
            - Gracefully handles missing or inaccessible output files
            - Creates parent directories for output file if needed
        """
        try:
            final_results = []
            reduce_task_ids = self.job_reduce_tasks.get(job.job_id, [])
            
            # Collect results from all completed Reduce tasks
            for task_id in reduce_task_ids:
                task = self.tasks[task_id]
                
                if task.result:
                    # Handle both output_files (list) and output_file (single file) formats
                    output_files = []
                    
                    # Support multiple output files format
                    if "output_files" in task.result:
                        output_files = task.result["output_files"]
                    # Support single output file format (more common)
                    elif "output_file" in task.result:
                        output_files = [task.result["output_file"]]
                    
                    # Read and aggregate all output files from this Reduce task
                    for output_file in output_files:
                        if os.path.exists(output_file):
                            with open(output_file, 'r') as f:
                                for line in f:
                                    if line.strip():  # Skip empty lines
                                        final_results.append(line.strip())
            
            # Write consolidated results to final output file
            final_output_path = job.output_data_path
            os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
            
            with open(final_output_path, 'w') as f:
                for result_line in final_results:
                    f.write(result_line + '\n')
            
            self.logger.info(f"Final results consolidated to {final_output_path} ({len(final_results)} records)")
            
        except Exception as e:
            self.logger.error(f"Error consolidating final results for job {job.job_id}: {e}")

    def get_job(self, job_id: str) -> Optional[MapReduceJob]:
        """
        Retrieve a MapReduceJob by its unique identifier.
        
        Args:
            job_id: Unique identifier for the job
            
        Returns:
            MapReduceJob instance if found, None otherwise
        """
        return self.jobs.get(job_id)
    
    def get_job_tasks(self, job_id: str) -> List[Task]:
        """
        Retrieve all tasks (Map and Reduce) associated with a job.
        
        Args:
            job_id: Unique identifier for the job
            
        Returns:
            List of Task instances for the job, empty list if job not found
        """
        task_ids = self.job_tasks.get(job_id, [])
        return [self.tasks[task_id] for task_id in task_ids if task_id in self.tasks]
    
    def get_pending_tasks(self) -> List[Task]:
        """
        Retrieve all tasks across all jobs that are pending assignment.
        
        This method is used by task schedulers to find unassigned tasks
        that need to be distributed to available workers.
        
        Returns:
            List of Task instances with PENDING status
        """
        return [task for task in self.tasks.values() 
                if task.status == TaskStatus.PENDING]
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Retrieve a specific task by its unique identifier.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            Task instance if found, None otherwise
        """
        return self.tasks.get(task_id)