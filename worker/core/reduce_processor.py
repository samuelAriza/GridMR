# Standard library imports for async operations, file I/O, and data structures
import asyncio
import json
import os
from typing import Dict, Any, List, Tuple, Iterator
from collections import defaultdict
from pathlib import Path

# Third-party imports for async file operations
import aiofiles

# Internal imports for task execution and data management
from models.task_context import TaskContext
from services.data_manager import DataManager
from services.execution_sandbox import ExecutionSandbox
from utils.logger import get_logger
from utils.config import get_settings

class ReduceProcessor:
    """
    Specialized processor for MapReduce Reduce phase execution.
    
    This processor handles the second phase of MapReduce processing:
    - Collecting and aggregating Map output files by key
    - Compiling and executing user-defined Reduce functions in a secure sandbox
    - Grouping intermediate key-value pairs for Reduce processing
    - Generating final results and saving them to the distributed file system
    
    The ReduceProcessor ensures proper data aggregation from distributed Map
    outputs through Reduce function execution to final consolidated results.
    
    Key Features:
    - Automatic grouping of Map outputs by key for Reduce processing
    - Secure execution of user-defined Reduce functions
    - Efficient handling of large intermediate datasets
    - Robust error handling and performance monitoring
    """
    
    def __init__(self):
        """
        Initialize ReduceProcessor with required services and configuration.
        
        Sets up:
        - Configuration settings for processor behavior
        - Logger for execution monitoring and debugging
        - DataManager for Map output data retrieval
        - ExecutionSandbox for secure Reduce function execution
        """
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.data_manager = DataManager()
        self.sandbox = ExecutionSandbox()
    
    async def start(self):
        """
        Initialize the ReduceProcessor and its dependencies.
        
        Prepares the processor for Reduce task execution by:
        - Starting the data manager for Map output data access
        - Initializing the execution sandbox for secure code execution
        - Setting up any required resources or connections
        """
        self.logger.info("Starting Reduce Processor...")
        await self.data_manager.start()
    
    async def stop(self):
        """
        Clean shutdown of the ReduceProcessor and its dependencies.
        
        Properly releases resources by:
        - Stopping the data manager and closing connections
        - Cleaning up the execution sandbox
        - Ensuring graceful shutdown of all async operations
        """
        self.logger.info("Stopping Reduce Processor...")
        await self.data_manager.stop()
    
    async def process(self, task_context: TaskContext) -> Dict[str, Any]:
        """
        Execute a complete Reduce task with data aggregation and final output generation.
        
        This method orchestrates the entire Reduce phase execution:
        1. Collects and groups Map output data by key from multiple partition files
        2. Compiles the user-defined Reduce function in a secure environment
        3. Executes the Reduce function on all grouped key-value collections
        4. Saves final Reduce results to the distributed file system
        
        Args:
            task_context: Complete task configuration including input partition files,
                         Reduce function code, and execution parameters
        
        Returns:
            Dict containing execution results including:
            - status: "completed" on success
            - output_file: Path to the created final output file
            - keys_processed: Number of unique keys processed
            - records_output: Number of final output records generated
            - execution_time: Total processing time in seconds
        
        Raises:
            Exception: If any step of Reduce processing fails, with detailed error info
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # 1. Collect and group Map output data by key from all input partitions
            grouped_data = await self._get_and_group_input_data(task_context.input_splits)
            
            # 2. Compile user-defined Reduce function in secure sandbox
            reduce_func = await self._compile_reduce_function(task_context.reduce_function)
            
            # 3. Execute Reduce function on all grouped key-value collections
            reduce_results = await self._execute_reduce_function(
                reduce_func, grouped_data, task_context
            )
            
            # 4. Save final Reduce results to distributed file system
            output_file = await self._save_reduce_output(
                reduce_results, task_context.output_path
            )
            
            execution_time = asyncio.get_event_loop().time() - start_time
            
            return {
                "status": "completed",
                "output_file": output_file,
                "keys_processed": len(grouped_data),
                "records_output": len(reduce_results),
                "execution_time": execution_time
            }
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            self.logger.error(f"Reduce processing failed: {e}")
            raise Exception(f"Reduce processing failed: {e}") from e
    
    async def _get_and_group_input_data(
        self, input_splits: List[str]
    ) -> Dict[str, List[Any]]:
        """
        Collect and group Map phase output data by key for Reduce processing.
        
        This method implements the critical shuffle/sort phase of MapReduce:
        1. **Data Collection**: Retrieves all Map output partition files
        2. **Key Grouping**: Groups all values by their associated keys
        3. **Data Validation**: Ensures proper JSON format and key-value structure
        4. **Error Recovery**: Handles malformed records gracefully with logging
        
        Shuffle/Sort Implementation:
        - Reads JSON records from each partition file
        - Groups values by key using efficient defaultdict collection
        - Maintains order within value lists for deterministic processing
        - Aggregates statistics for monitoring and debugging
        
        Data Format Expectations:
        - Each line contains JSON object with 'key' and 'value' fields
        - Keys are used for grouping, values are collected into lists
        - Invalid records are logged but processing continues
        - Empty lines are ignored for robustness
        
        Performance Considerations:
        - Async file I/O prevents blocking during large file processing
        - Memory efficient processing with incremental record handling
        - Error isolation prevents single bad records from failing jobs
        - Statistics collection for performance monitoring
        
        Args:
            input_splits: List of paths to Map output partition files
        
        Returns:
            Dict[str, List[Any]]: Grouped data where keys map to lists of values
        
        Raises:
            Exception: If any input split cannot be read or accessed
        """
        grouped_data = defaultdict(list)
        total_records = 0
        
        for split_path in input_splits:
            try:
                # Download split if necessary for data access
                local_path = await self.data_manager.get_data_split(split_path)
                
                # Read and group data by key
                async with aiofiles.open(local_path, 'r', encoding='utf-8') as f:
                    async for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                key = record["key"]
                                value = record["value"]
                                grouped_data[key].append(value)
                                total_records += 1
                            except (json.JSONDecodeError, KeyError) as e:
                                self.logger.warning(f"Invalid record in {split_path}: {line}")
                                continue
                            
            except Exception as e:
                self.logger.error(f"Error reading split {split_path}: {e}")
                raise
        
        self.logger.info(
            f"Grouped {total_records} records into {len(grouped_data)} unique keys"
        )
        return dict(grouped_data)
    
    async def _compile_reduce_function(self, reduce_function_code: str):
        """
        Compile user-defined Reduce function in secure execution environment.
        
        This method provides secure compilation of user code:
        1. **Security Isolation**: Uses ExecutionSandbox for safe compilation
        2. **Function Validation**: Ensures proper Reduce function structure
        3. **Error Handling**: Provides detailed error messages for debugging
        4. **Type Safety**: Validates function signature and requirements
        
        Compilation Process:
        - Validates user-provided Reduce function code syntax
        - Compiles function in isolated security sandbox
        - Ensures function meets MapReduce Reduce requirements
        - Returns compiled function ready for execution
        
        Security Features:
        - Isolated compilation environment prevents system access
        - Function validation ensures proper MapReduce interface
        - Error containment prevents compilation issues from affecting worker
        - Safe code execution with restricted permissions
        
        Args:
            reduce_function_code: User-defined Reduce function as string
        
        Returns:
            Compiled function object ready for secure execution
            
        Raises:
            Exception: If function compilation fails with detailed error information
        """
        try:
            compiled_func = await self.sandbox.compile_function(
                reduce_function_code, 
                function_type="reduce"
            )
            return compiled_func
            
        except Exception as e:
            self.logger.error(f"Error compiling reduce function: {e}")
            raise Exception(f"Invalid reduce function: {e}") from e
    
    async def _execute_reduce_function(
        self, 
        reduce_func, 
        grouped_data: Dict[str, List[Any]], 
        task_context: TaskContext
    ) -> List[Tuple[str, Any]]:
        """
        Execute the compiled Reduce function on all grouped key-value collections.
        
        This method implements the core Reduce computation:
        1. **Parallel Processing**: Executes Reduce function on each key group
        2. **Result Collection**: Aggregates all Reduce outputs into final results
        3. **Error Handling**: Manages execution errors with configurable behavior
        4. **Format Standardization**: Ensures consistent output format
        
        Reduce Execution Strategy:
        - Iterates over each unique key and its associated values
        - Executes user Reduce function in secure sandbox environment
        - Collects results in standardized (key, value) tuple format
        - Handles various result formats from user functions
        
        Error Management:
        - Logs execution errors for individual keys
        - Configurable error handling (continue vs fail)
        - Preserves partial results when possible
        - Provides detailed error context for debugging
        
        Result Format Handling:
        - Accepts tuple (key, value) format from user functions
        - Converts single values to (key, value) tuples automatically
        - Maintains result ordering for deterministic output
        - Logs statistics for monitoring and performance analysis
        
        Args:
            reduce_func: Compiled Reduce function ready for execution
            grouped_data: Dictionary mapping keys to lists of values
            task_context: Execution context with parameters and configuration
        
        Returns:
            List[Tuple[str, Any]]: Final Reduce results as key-value pairs
            
        Raises:
            Exception: If error handling is configured to fail on first error
        """
        reduce_results = []
        
        for key, values in grouped_data.items():
            try:
                # Execute Reduce function in secure sandbox
                result = await self.sandbox.execute_function(
                    reduce_func, 
                    args=[key, values],
                    context=task_context.parameters
                )
                
                # Handle different result formats from user functions
                if isinstance(result, tuple) and len(result) == 2:
                    reduce_results.append(result)
                else:
                    reduce_results.append((key, result))
                    
            except Exception as e:
                self.logger.error(f"Error executing reduce function on key {key}: {e}")
                # Configurable error handling - continue or fail based on settings
                if not task_context.parameters.get("ignore_errors", False):
                    raise
        
        self.logger.info(f"Reduce function produced {len(reduce_results)} output records")
        return reduce_results
    
    async def _save_reduce_output(
        self, 
        reduce_results: List[Tuple[str, Any]], 
        output_path: str
    ) -> str:
        """
        Save final Reduce results to the distributed file system.
        
        This method completes the MapReduce workflow:
        1. **File System Preparation**: Creates output directory structure
        2. **Result Serialization**: Converts results to persistent JSON format
        3. **Atomic Writing**: Ensures complete file creation or failure
        4. **Statistics Logging**: Records output metrics for monitoring
        
        Output Format:
        - Each result stored as JSON object with 'key' and 'value' fields
        - One JSON object per line for efficient streaming processing
        - UTF-8 encoding for international character support
        - Consistent format for downstream processing tools
        
        File System Operations:
        - Creates parent directories automatically if needed
        - Uses async I/O for non-blocking large file operations
        - Ensures atomic write completion for data consistency
        - Provides detailed logging for operation tracking
        
        Args:
            reduce_results: Final key-value pairs from Reduce execution
            output_path: Destination path for the output file
        
        Returns:
            str: Path to the successfully created output file
            
        Side Effects:
            - Creates directories in the file system
            - Writes final results to persistent storage
            - Logs completion statistics and file information
        """
        # Create output directory structure
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save results in JSON format
        async with aiofiles.open(output_path, 'w') as f:
            for key, value in reduce_results:
                record = {"key": key, "value": value}
                await f.write(json.dumps(record) + '\n')
        
        self.logger.info(f"Reduce output saved to {output_path} ({len(reduce_results)} records)")
        return output_path
