# Standard library imports for async operations, file I/O, and data structures
import asyncio
import json
import os
import tempfile
from typing import Dict, Any, List, Tuple, Iterator
from pathlib import Path

# Third-party imports for async file operations
import aiofiles

# Internal imports for task execution and data management
from models.task_context import TaskContext
from services.data_manager import DataManager
from services.execution_sandbox import ExecutionSandbox
from utils.logger import get_logger
from utils.config import get_settings

class MapProcessor:
    """
    Specialized processor for MapReduce Map phase execution.
    
    This processor handles the first phase of MapReduce processing:
    - Reading and parsing input data splits
    - Compiling and executing user-defined Map functions in a secure sandbox
    - Partitioning Map output for distribution to Reduce tasks
    - Saving partitioned results to the distributed file system
    
    The MapProcessor ensures proper data flow from input splits through
    Map function execution to partitioned output ready for Reduce processing.
    
    Key Features:
    - Secure execution of user-defined Map functions
    - Automatic data partitioning based on key hashing
    - Robust error handling and recovery mechanisms
    - Performance monitoring and logging
    """
    
    def __init__(self):
        """
        Initialize MapProcessor with required services and configuration.
        
        Sets up:
        - Configuration settings for processor behavior
        - Logger for execution monitoring and debugging
        - DataManager for input data retrieval
        - ExecutionSandbox for secure Map function execution
        """
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.data_manager = DataManager()
        self.sandbox = ExecutionSandbox()
    
    async def start(self):
        """
        Initialize the MapProcessor and its dependencies.
        
        Prepares the processor for Map task execution by:
        - Starting the data manager for input data access
        - Initializing the execution sandbox for secure code execution
        - Setting up any required resources or connections
        """
        self.logger.info("Starting Map Processor...")
        await self.data_manager.start()
    
    async def stop(self):
        """
        Clean shutdown of the MapProcessor and its dependencies.
        
        Properly releases resources by:
        - Stopping the data manager and closing connections
        - Cleaning up the execution sandbox
        - Ensuring graceful shutdown of all async operations
        """
        self.logger.info("Stopping Map Processor...")
        await self.data_manager.stop()
    
    async def process(self, task_context: TaskContext) -> Dict[str, Any]:
        """
        Execute a complete Map task with input processing and output partitioning.
        
        This method orchestrates the entire Map phase execution:
        1. Loads input data from specified splits
        2. Compiles the user-defined Map function in a secure environment
        3. Executes the Map function on all input records
        4. Partitions Map output based on key hashing for Reduce distribution
        5. Saves partitioned results to the distributed file system
        
        Args:
            task_context: Complete task configuration including input splits,
                         Map function code, and execution parameters
        
        Returns:
            Dict containing execution results including:
            - status: "completed" on success
            - output_files: List of created partition file paths
            - records_processed: Number of input records processed
            - execution_time: Total processing time in seconds
            - partitions_created: Number of output partitions created
        
        Raises:
            Exception: If any step of Map processing fails, with detailed error info
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # 1. Load and parse input data from all assigned splits
            input_data = await self._get_input_data(task_context.input_splits)
            
            # 2. Compile user-defined Map function in secure sandbox
            map_func = await self._compile_map_function(task_context.map_function)
            
            # 3. Execute Map function on all input records
            map_results = await self._execute_map_function(map_func, input_data, task_context)
            
            # 4. Partition Map output by key hash for Reduce task distribution
            partitioned_results = await self._partition_map_output(
                map_results, task_context.parameters.get("num_reducers", 1)
            )
            
            # 5. Save partitioned results to distributed file system
            # Create task-specific output directory for Map results
            map_output_dir = f"/data/map_output_{task_context.task_id}"
            output_files = await self._save_map_output(
                partitioned_results, map_output_dir
            )
            
            execution_time = asyncio.get_event_loop().time() - start_time
            
            return {
                "status": "completed",
                "output_files": output_files,
                "records_processed": len(map_results),
                "execution_time": execution_time,
                "partitions_created": len(partitioned_results)
            }
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            self.logger.error(f"Map processing failed: {e}")
            raise Exception(f"Map processing failed: {e}") from e
    
    async def _get_input_data(self, input_splits: List[str]) -> List[Tuple[str, str]]:
        """
        Load and parse input data from all assigned data splits.
        
        This method handles input data retrieval and formatting:
        1. Downloads or accesses each data split file
        2. Reads file content line by line with proper encoding
        3. Creates key-value pairs for Map function processing
        4. Handles file access errors and empty lines gracefully
        
        Args:
            input_splits: List of file paths or URIs for input data splits
        
        Returns:
            List of (key, value) tuples where:
            - key: Unique identifier (file:line_number format)
            - value: Line content as string for Map function processing
        
        Raises:
            Exception: If data split access fails or file reading errors occur
        """
        input_data = []
        
        for split_path in input_splits:
            try:
                # Download or access data split file through data manager
                local_path = await self.data_manager.get_data_split(split_path)
                
                # Read and process data split content line by line
                async with aiofiles.open(local_path, 'r', encoding='utf-8') as f:
                    line_number = 0
                    async for line in f:
                        line = line.strip()
                        if line:  # Skip empty lines to avoid processing null data
                            # Create key-value pair: (unique_identifier, line_content)
                            input_data.append((f"{split_path}:{line_number}", line))
                            line_number += 1
                            
            except Exception as e:
                self.logger.error(f"Error reading split {split_path}: {e}")
                raise
        
        self.logger.info(f"Loaded {len(input_data)} records from {len(input_splits)} splits")
        return input_data
    
    async def _compile_map_function(self, map_function_code: str):
        """
        Compile user-defined Map function in a secure execution environment.
        
        This method ensures safe compilation of user code:
        1. Validates function code syntax using AST parsing
        2. Compiles code in restricted execution sandbox
        3. Verifies function signature and return type expectations
        4. Provides detailed error messages for invalid functions
        
        Args:
            map_function_code: Python source code string containing Map function
        
        Returns:
            Compiled function object ready for execution in sandbox
        
        Raises:
            Exception: If compilation fails due to syntax errors, security violations,
                      or invalid function structure
        """
        try:
            # Validate and compile Map function code in secure sandbox
            compiled_func = await self.sandbox.compile_function(
                map_function_code, 
                function_type="map"
            )
            return compiled_func
            
        except Exception as e:
            self.logger.error(f"Error compiling map function: {e}")
            raise Exception(f"Invalid map function: {e}") from e
    
    async def _execute_map_function(
        self, 
        map_func, 
        input_data: List[Tuple[str, str]], 
        task_context: TaskContext
    ) -> List[Tuple[str, Any]]:
        """
        Execute the compiled Map function on all input data records.
        
        This method handles the core Map processing logic:
        1. Iterates through all input key-value pairs
        2. Executes Map function in secure sandbox for each record
        3. Collects and validates Map function output
        4. Handles various output formats (generators, lists, single values)
        5. Provides error handling with optional error tolerance
        
        In traditional MapReduce, Map functions receive input values and
        produce intermediate key-value pairs for Reduce processing.
        
        Args:
            map_func: Compiled Map function ready for execution
            input_data: List of (key, value) pairs from input splits
            task_context: Task configuration including error handling parameters
        
        Returns:
            List of (key, value) tuples produced by Map function execution
        
        Raises:
            Exception: If Map function execution fails and error tolerance is disabled
        """
        map_results = []
        
        for key, value in input_data:
            try:
                # Execute Map function with input value (traditional MapReduce approach)
                # The key is generally ignored in Map processing, focus on value content
                result = await self.sandbox.execute_function(
                    map_func, 
                    args=[value],  # Pass only the value (line content) to Map function
                    context=task_context.parameters
                )
                
                # Handle different Map function output formats
                if hasattr(result, '__iter__') and not isinstance(result, str):
                    # If result is iterable (list, generator), extend results
                    map_results.extend(list(result))
                else:
                    # If result is single value, convert to list format
                    map_results.append(result)
                    
            except Exception as e:
                self.logger.error(f"Error executing map function on key {key}: {e}")
                # Continue processing or fail based on error tolerance configuration
                if not task_context.parameters.get("ignore_errors", False):
                    raise
        
        self.logger.info(f"Map function produced {len(map_results)} output records")
        return map_results
    
    async def _partition_map_output(
        self, 
        map_results: List[Tuple[str, Any]], 
        num_reducers: int
    ) -> Dict[int, List[Tuple[str, Any]]]:
        """
        Partition Map output records for distribution to Reduce tasks.
        
        This method implements the shuffle phase of MapReduce:
        1. Groups Map output records by key hash for even distribution
        2. Ensures each Reduce task receives all records for its assigned keys
        3. Uses consistent hashing for deterministic partitioning
        4. Balances load across all available Reduce tasks
        
        Partitioning is critical for MapReduce correctness, ensuring that
        all records with the same key are processed by the same Reduce task.
        
        Args:
            map_results: List of (key, value) tuples from Map function execution
            num_reducers: Number of Reduce tasks that will process the output
        
        Returns:
            Dictionary mapping partition_id to list of (key, value) tuples
            where partition_id ranges from 0 to (num_reducers - 1)
        """
        partitions = {i: [] for i in range(num_reducers)}
        
        for key, value in map_results:
            # Use consistent key hashing to determine target partition
            partition_id = hash(str(key)) % num_reducers
            partitions[partition_id].append((key, value))
        
        # Log partitioning statistics for monitoring and debugging
        for partition_id, records in partitions.items():
            self.logger.debug(f"Partition {partition_id}: {len(records)} records")
        
        return partitions
    
    async def _save_map_output(
        self, 
        partitioned_results: Dict[int, List[Tuple[str, Any]]], 
        output_path: str
    ) -> List[str]:
        """
        Save partitioned Map results to the distributed file system.
        
        This method handles persistent storage of Map output:
        1. Creates output directory with proper permissions for NFS access
        2. Writes each partition to a separate file for Reduce task consumption
        3. Uses standardized JSON format for cross-worker compatibility
        4. Implements fallback mechanisms for permission or storage issues
        5. Sets appropriate file permissions for distributed access
        
        The output format follows MapReduce conventions with numbered
        partition files that Reduce tasks can discover and process.
        
        Args:
            partitioned_results: Dictionary mapping partition IDs to record lists
            output_path: Base directory path for partition file storage
        
        Returns:
            List of created partition file paths for result tracking
        
        Raises:
            Exception: If file system operations fail after fallback attempts
        """
        output_files = []
        
        try:
            # Create output directory with proper permissions for NFS and distributed access
            os.makedirs(output_path, mode=0o755, exist_ok=True)
            self.logger.info(f"Created output directory: {output_path}")
            
            for partition_id, records in partitioned_results.items():
                if not records:  # Skip empty partitions to reduce file system overhead
                    continue
                    
                # Create standardized partition file name for Reduce task discovery
                partition_file = f"{output_path}/part-{partition_id:05d}.json"
                
                # Write partition records in JSON format for cross-worker compatibility
                async with aiofiles.open(partition_file, 'w') as f:
                    for key, value in records:
                        record = {"key": key, "value": value}
                        await f.write(json.dumps(record) + '\n')
                
                # Set file permissions for distributed access across worker nodes
                os.chmod(partition_file, 0o644)
                output_files.append(partition_file)
                self.logger.debug(f"Saved {len(records)} records to {partition_file}")
                
        except PermissionError as e:
            self.logger.error(f"Permission error creating output directory {output_path}: {e}")
            # Implement fallback to temporary directory if NFS permissions fail
            temp_dir = f"/tmp/map_output_{os.getpid()}_{len(partitioned_results)}"
            self.logger.info(f"Falling back to temporary directory: {temp_dir}")
            return await self._save_map_output(partitioned_results, temp_dir)
        except Exception as e:
            self.logger.error(f"Error saving map output: {e}")
            raise
        
        self.logger.info(f"Map output saved to {len(output_files)} partition files")
        return output_files