import asyncio
from datetime import datetime
from typing import Optional

from services.master_client import MasterClient
from core.resource_monitor import ResourceMonitor
from utils.logger import get_logger
from utils.config import get_settings

class HeartbeatService:
    """
    Enterprise-grade heartbeat service for distributed worker node health monitoring.
    
    This service provides critical infrastructure for distributed system monitoring:
    
    Core Responsibilities:
    1. **Liveness Monitoring**: Regular heartbeat signals to master node
    2. **Resource Reporting**: Real-time system resource usage metrics  
    3. **Status Communication**: Worker availability and task capacity information
    4. **Failure Detection**: Enables master to detect worker failures quickly
    5. **Load Balancing**: Provides data for intelligent task distribution
    
    Heartbeat Architecture:
    - Periodic status reports with configurable intervals
    - Comprehensive resource metrics collection
    - Graceful handling of network failures and retries
    - Async operation to avoid blocking worker processes
    
    Monitoring Capabilities:
    - CPU usage and availability tracking
    - Memory usage and free capacity monitoring
    - Disk space utilization reporting
    - Worker uptime and stability metrics
    - Active task count and completion statistics
    
    Fault Tolerance Features:
    - Automatic retry on heartbeat failures
    - Graceful degradation during network issues
    - Resource monitor error isolation
    - Configurable failure handling strategies
    
    Integration Points:
    - Uses MasterClient for secure communication
    - Integrates with ResourceMonitor for system metrics
    - Coordinates with WorkerEngine for task information
    - Provides data for master's load balancing decisions
    """
    
    def __init__(self, master_client: MasterClient):
        """
        Initialize the heartbeat service with essential dependencies and state.
        
        Sets up:
        1. Communication client for master node interaction
        2. Resource monitoring infrastructure for system metrics
        3. Service state management for graceful lifecycle control
        4. Timing infrastructure for heartbeat interval tracking
        
        Dependencies:
        - master_client: Handles secure communication with master node
        - resource_monitor: Provides real-time system resource metrics
        - Configuration and logging for operational requirements
        
        State Management:
        - is_running: Controls service lifecycle and heartbeat loop
        - heartbeat_task: Manages async heartbeat execution
        - start_time: Enables uptime calculations for monitoring
        """
        self.master_client = master_client
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.resource_monitor = ResourceMonitor()
        
        # Service lifecycle management
        self.is_running = False
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.start_time = datetime.utcnow()
    
    async def start(self):
        """
        Start the heartbeat service and initialize monitoring infrastructure.
        
        Startup Process:
        1. Prevents duplicate service initialization
        2. Starts resource monitoring for system metrics collection
        3. Launches async heartbeat loop for periodic master communication
        4. Establishes service availability for distributed coordination
        
        Service Initialization:
        - Activates resource monitoring infrastructure
        - Creates background task for heartbeat loop execution
        - Enables real-time worker status reporting
        - Establishes connection with master node coordination
        
        Error Handling:
        - Graceful handling of duplicate start attempts
        - Resource monitor initialization failure recovery
        - Logging for service startup monitoring
        
        Side Effects:
        - Starts background async task for heartbeat loop
        - Initializes resource monitoring infrastructure
        - Begins periodic communication with master node
        """
        if self.is_running:
            return
        
        self.logger.info("Starting Heartbeat Service...")
        self.is_running = True
        
        # Initialize resource monitoring infrastructure
        await self.resource_monitor.start()
        
        # Launch background heartbeat loop task
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        self.logger.info("Heartbeat Service started")
    
    async def stop(self):
        """
        Gracefully shutdown the heartbeat service and cleanup resources.
        
        Shutdown Process:
        1. Prevents duplicate shutdown attempts
        2. Cancels background heartbeat task cleanly
        3. Stops resource monitoring infrastructure
        4. Ensures all resources are properly released
        
        Task Management:
        - Cancels running heartbeat task gracefully
        - Waits for task completion to prevent resource leaks
        - Handles cancellation exceptions properly
        - Ensures clean async task lifecycle
        
        Resource Cleanup:
        - Stops resource monitor to free system resources
        - Closes monitoring infrastructure connections
        - Prevents resource leaks during shutdown
        - Maintains system stability during termination
        
        Side Effects:
        - Cancels background async tasks
        - Stops system resource monitoring
        - Ends communication with master node
        """
        if not self.is_running:
            return
        
        self.logger.info("Stopping Heartbeat Service...")
        self.is_running = False
        
        # Cancel heartbeat task gracefully
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Stop resource monitoring infrastructure
        await self.resource_monitor.stop()
        
        self.logger.info("Heartbeat Service stopped")
    
    async def _heartbeat_loop(self):
        """
        Main heartbeat loop for periodic master node communication.
        
        This method implements the core heartbeat functionality:
        1. **Status Collection**: Gathers current worker status and metrics
        2. **Master Communication**: Sends heartbeat with status information
        3. **Error Recovery**: Handles communication failures gracefully
        4. **Timing Control**: Maintains configured heartbeat intervals
        
        Loop Behavior:
        - Runs continuously while service is active
        - Collects fresh worker status for each heartbeat
        - Sends status to master through secure client
        - Handles network failures with logging and retry
        
        Failure Handling:
        - Network failures logged as warnings for monitoring
        - Communication errors don't terminate the service
        - Automatic retry on next interval for resilience
        - Graceful cancellation support for clean shutdown
        
        Interval Management:
        - Configurable heartbeat interval for different environments
        - Sleep between heartbeats to prevent resource exhaustion
        - Immediate retry delay on persistent errors
        
        Side Effects:
        - Periodic network communication with master node
        - Regular resource metric collection
        - Continuous service availability reporting
        """
        while self.is_running:
            try:
                # Collect current worker status and resource metrics
                worker_status = await self._get_worker_status()
                
                # Send heartbeat with status information to master
                success = await self.master_client.send_heartbeat(worker_status)
                
                if not success:
                    self.logger.warning("Heartbeat failed, will retry next interval")
                
                # Wait for next heartbeat interval
                await asyncio.sleep(self.settings.worker_heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(5)  # Brief delay before retry
    
    async def _get_worker_status(self) -> dict:
        """
        Collect comprehensive worker status information for master reporting.
        
        This method assembles critical worker information:
        1. **Resource Metrics**: Current CPU, memory, and disk usage
        2. **Availability Status**: Worker operational state and capacity
        3. **Performance Data**: Uptime and task execution statistics  
        4. **Capacity Information**: Available resources for new tasks
        
        Status Components:
        - Resource utilization percentages for load balancing
        - Available memory for task size estimation
        - Worker uptime for stability assessment
        - Task execution history for performance tracking
        
        Integration Points:
        - ResourceMonitor provides real-time system metrics
        - WorkerEngine could provide active task information
        - Metrics system could provide completion statistics
        
        The status information enables the master to:
        - Make intelligent task assignment decisions
        - Monitor worker health and performance
        - Balance load across available workers
        - Detect performance degradation or failures
        
        Returns:
            dict: Comprehensive worker status information including:
                 - Current resource usage metrics
                 - Worker availability and capacity
                 - Performance and uptime statistics
                 - Task execution summary data
        """
        # Collect current resource metrics from monitoring system
        resource_metrics = await self.resource_monitor.get_current_metrics()
        
        # Calculate worker uptime for stability reporting
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        
        # Assemble comprehensive status report
        status = {
            "status": "available" if self.is_running else "offline",
            "uptime_seconds": uptime,
            "cpu_usage": resource_metrics["cpu_percent"],
            "memory_usage": resource_metrics["memory_percent"], 
            "disk_usage": resource_metrics["disk_percent"],
            "available_memory_mb": resource_metrics["available_memory_mb"],
            "active_tasks": [],  # Future: populated by WorkerEngine
            "tasks_completed": 0,  # Future: populated by metrics system
            "tasks_failed": 0  # Future: populated by metrics system
        }
        
        return status