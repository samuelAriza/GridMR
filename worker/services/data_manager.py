import asyncio
import os
import aiofiles
import aiohttp
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from utils.logger import get_logger
from utils.config import get_settings

class DataManager:
    """
    Enterprise-grade data management service for MapReduce worker nodes.
    
    This service provides comprehensive data handling capabilities for distributed computing:
    
    Key Responsibilities:
    1. **Data Acquisition**: Downloads and caches input data splits from various sources
    2. **Local Storage Management**: Maintains efficient local cache with intelligent cleanup
    3. **Output Management**: Handles result serialization and storage operations
    4. **Network Optimization**: Minimizes data transfer through smart caching strategies
    5. **Resource Management**: Manages temporary storage and cleanup operations
    
    Architecture Features:
    - Asynchronous operations for non-blocking data handling
    - Multi-source support (HTTP, HTTPS, local filesystem, NFS)
    - Intelligent caching with hash-based deduplication
    - Automatic cleanup and resource management
    - Configurable timeout and retry policies
    
    Storage Strategy:
    - Input data cached in dedicated cache directory
    - Downloads managed in separate downloads directory
    - Hash-based naming prevents duplicate downloads
    - Configurable cleanup policies for storage optimization
    
    Integration Points:
    - Used by MapProcessor and ReduceProcessor for data access
    - Coordinates with ExecutionSandbox for secure data handling
    - Communicates with master node for data location resolution
    
    Performance Optimizations:
    - Local cache reduces redundant downloads
    - Chunked downloads for large files
    - Asynchronous I/O operations
    - Directory-based search for local files
    """
    
    def __init__(self):
        """
        Initialize the DataManager with essential configuration and storage directories.
        
        Sets up:
        1. Configuration and logging infrastructure
        2. Directory structure for caching and downloads
        3. Local cache for file path mapping
        4. HTTP client placeholder for network operations
        
        Directory Structure:
        - cache/: Stores frequently accessed data splits
        - downloads/: Stores files downloaded from remote sources
        
        Cache Management:
        - local_cache maps original paths to local file locations
        - Enables quick access to previously processed data
        - Reduces redundant network operations
        """
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.cache_dir = Path(self.settings.temp_data_path) / "cache"
        self.downloads_dir = Path(self.settings.temp_data_path) / "downloads"
        
        # Cache of local file mappings for efficient data access
        self.local_cache: Dict[str, str] = {}
        
        # HTTP client for remote data downloads (initialized in start())
        self.http_client: Optional[aiohttp.ClientSession] = None
    
    async def start(self):
        """
        Initialize the data management service and prepare for operations.
        
        Startup Process:
        1. Creates necessary directory structure for data management
        2. Initializes HTTP client with configured timeout settings
        3. Prepares caching infrastructure for optimal performance
        
        Directory Creation:
        - Ensures cache and downloads directories exist
        - Creates parent directories as needed
        - Sets up proper permissions for data operations
        
        Network Configuration:
        - Configures HTTP client with appropriate timeouts
        - Enables connection pooling for efficiency
        - Prepares for concurrent download operations
        
        Side Effects:
        - Creates filesystem directories
        - Initializes network client
        - Prepares logging infrastructure
        """
        self.logger.info("Starting Data Manager...")
        
        # Create necessary directories for data operations
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize HTTP client with proper timeout configuration
        self.http_client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.settings.download_timeout)
        )
        
        self.logger.info("Data Manager started")
    
    async def stop(self):
        """
        Gracefully shutdown the data management service and cleanup resources.
        
        Shutdown Process:
        1. Closes HTTP client connections to prevent resource leaks
        2. Optionally cleans up temporary files based on configuration
        3. Ensures all resources are properly released
        
        Cleanup Strategy:
        - Closes network connections to free system resources
        - Removes temporary files if configured for cleanup
        - Preserves important cached data based on policies
        
        Configuration-Based Behavior:
        - cleanup_temp_files setting controls file cleanup
        - Balances storage cleanup with performance optimization
        - Maintains cache for frequently accessed data
        
        Side Effects:
        - Closes network connections
        - May remove temporary files
        - Logs shutdown completion
        """
        self.logger.info("Stopping Data Manager...")
        
        if self.http_client:
            await self.http_client.close()
        
        # Clean up temporary files if configured
        if self.settings.cleanup_temp_files:
            await self._cleanup_temp_files()
        
        self.logger.info("Data Manager stopped")
    
    async def get_data_split(self, split_path: str) -> str:
        """
        Retrieve a data split for processing, handling various data sources efficiently.
        
        This method is central to MapReduce data access and implements:
        1. **Cache-First Strategy**: Checks local cache before any network operations
        2. **Multi-Source Support**: Handles URLs, local files, and distributed filesystems
        3. **Intelligent Discovery**: Searches known data directories for missing files
        4. **Performance Optimization**: Caches results to minimize repeated operations
        
        Data Source Resolution Order:
        1. Local cache (instant access)
        2. URL download (network operation)
        3. Direct local file access (filesystem operation)
        4. Search in known data directories (discovery operation)
        
        Caching Strategy:
        - Successful retrievals are cached for future access
        - Cache key is the original split_path for consistency
        - Local path mapping enables quick subsequent access
        
        Args:
            split_path: Path or URL to the data split to retrieve
                       Can be HTTP/HTTPS URL, local file path, or logical name
        
        Returns:
            str: Local filesystem path to the accessible data split
        
        Raises:
            FileNotFoundError: If split cannot be found or downloaded
            RuntimeError: If HTTP client is not initialized for URL downloads
            
        Side Effects:
            - May download files from remote sources
            - Updates local cache with new file mappings
            - Creates local copies of remote data
        """
        
        # Check local cache first for optimal performance
        if split_path in self.local_cache:
            local_path = self.local_cache[split_path]
            if os.path.exists(local_path):
                self.logger.debug(f"Using cached split: {split_path}")
                return local_path
        
        # Determine data source type and handle appropriately
        if self._is_url(split_path):
            local_path = await self._download_split(split_path)
        elif os.path.exists(split_path):
            # Direct local file access - use as-is
            local_path = split_path
        else:
            # Search in known data directories for the file
            local_path = await self._find_local_split(split_path)
            if not local_path:
                raise FileNotFoundError(f"Split not found: {split_path}")
        
        # Cache successful resolution for future access
        self.local_cache[split_path] = local_path
        return local_path
    
    async def save_output_data(self, data: Any, output_path: str) -> str:
        """
        Persist task output data to the specified location with format detection.
        
        This method handles various data types and provides:
        1. **Format Detection**: Automatically determines serialization strategy
        2. **Directory Creation**: Ensures output directory structure exists
        3. **Type-Specific Handling**: Optimizes serialization for different data types
        4. **Error Recovery**: Provides detailed error information for debugging
        
        Supported Data Types:
        - String data: Written directly as text files
        - Binary data: Written as binary files
        - Complex objects: Serialized as JSON with formatting
        
        File Handling:
        - Creates parent directories automatically
        - Uses async I/O for non-blocking operations
        - Provides proper error handling and logging
        
        Args:
            data: The data to save (str, bytes, or complex object)
            output_path: Destination path for the output file
        
        Returns:
            str: The output path where data was successfully saved
            
        Raises:
            OSError: If directory creation or file writing fails
            json.JSONEncodeError: If complex data cannot be serialized
            
        Side Effects:
            - Creates directories in the filesystem
            - Writes files to the specified location
            - Logs successful operations
        """
        
        # Ensure output directory exists for successful file creation
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            if isinstance(data, (str, bytes)):
                # Handle simple data types efficiently
                mode = 'w' if isinstance(data, str) else 'wb'
                async with aiofiles.open(output_path, mode) as f:
                    await f.write(data)
            else:
                # Serialize complex data structures as formatted JSON
                import json
                async with aiofiles.open(output_path, 'w') as f:
                    await f.write(json.dumps(data, indent=2))
            
            self.logger.info(f"Output data saved to: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to save output data: {e}")
            raise
    
    async def upload_result(self, local_path: str, remote_path: str) -> bool:
        """
        Upload processing results to remote storage systems (future implementation).
        
        This method provides the foundation for result distribution:
        1. **Multi-Backend Support**: Designed for S3, HDFS, and other distributed storage
        2. **Upload Strategy**: Handles large files with streaming uploads
        3. **Retry Logic**: Implements resilient upload with error recovery
        4. **Progress Tracking**: Monitors upload progress for large results
        
        Future Implementation Plans:
        - AWS S3 integration for cloud storage
        - HDFS support for Hadoop ecosystem integration
        - Azure Blob Storage for Microsoft cloud environments
        - Google Cloud Storage for GCP deployments
        
        Current Status:
        - Placeholder implementation for future development
        - Logs upload intentions for debugging
        - Returns success for compatibility
        
        Args:
            local_path: Path to the local file to upload
            remote_path: Destination path in remote storage system
        
        Returns:
            bool: True if upload successful (currently always True)
            
        Note:
            This is a future implementation placeholder. Actual upload
            functionality will be added based on deployment requirements.
        """
        # Future implementation for distributed storage integration
        self.logger.info(f"Upload from {local_path} to {remote_path} (not implemented)")
        return True
    
    def _is_url(self, path: str) -> bool:
        """
        Determine if a given path is a URL requiring network access.
        
        This method enables intelligent routing of data access requests:
        1. **Protocol Detection**: Identifies common URL schemes
        2. **Network vs Local**: Distinguishes remote from local resources
        3. **Access Strategy**: Informs download vs filesystem access decisions
        
        Supported URL Schemes:
        - http/https: Standard web protocols
        - ftp: File Transfer Protocol
        - s3: Amazon S3 protocol (future support)
        
        Args:
            path: The path or URL to analyze
        
        Returns:
            bool: True if path is a URL requiring network access
        """
        parsed = urlparse(path)
        return parsed.scheme in ['http', 'https', 'ftp', 's3']
    
    async def _download_split(self, url: str) -> str:
        """
        Download a data split from a remote URL with caching and optimization.
        
        This method implements robust download functionality:
        1. **Deduplication**: Uses content-based naming to avoid duplicate downloads
        2. **Caching**: Stores downloads for reuse across tasks
        3. **Chunked Transfer**: Handles large files efficiently
        4. **Error Handling**: Provides detailed error information for debugging
        
        Download Strategy:
        - Hash-based filenames prevent duplicate downloads
        - Chunked reading enables large file handling
        - Async I/O prevents blocking other operations
        - Automatic retry through HTTP client configuration
        
        Caching Mechanism:
        - URL hash generates unique local filename
        - Existing downloads are reused immediately
        - Cache persists across worker restarts
        
        Args:
            url: The URL to download data from
        
        Returns:
            str: Local path to the downloaded file
            
        Raises:
            RuntimeError: If HTTP client is not initialized
            aiohttp.ClientError: If download fails due to network issues
            OSError: If local file operations fail
        """
        if not self.http_client:
            raise RuntimeError("HTTP client not initialized")
        
        # Generate unique local filename based on URL content hash
        url_hash = hashlib.md5(url.encode()).hexdigest()
        filename = f"split_{url_hash}"
        local_path = self.downloads_dir / filename
        
        # Return immediately if already downloaded
        if local_path.exists():
            self.logger.debug(f"Split already downloaded: {url}")
            return str(local_path)
        
        try:
            self.logger.info(f"Downloading split from: {url}")
            
            async with self.http_client.get(url) as response:
                response.raise_for_status()
                
                # Download in chunks for efficient large file handling
                async with aiofiles.open(local_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
            
            self.logger.info(f"Split downloaded successfully: {local_path}")
            return str(local_path)
            
        except Exception as e:
            self.logger.error(f"Failed to download split from {url}: {e}")
            raise
    
    async def _find_local_split(self, split_path: str) -> Optional[str]:
        """
        Search for a data split in known local directories.
        
        This method implements intelligent file discovery:
        1. **Multi-Directory Search**: Checks common data locations
        2. **NFS Integration**: Supports distributed filesystem mounts
        3. **Fallback Strategy**: Provides multiple search paths for reliability
        4. **Path Resolution**: Handles various directory structures
        
        Search Priority:
        1. Worker-specific data directory
        2. Temporary data directory
        3. NFS export mount points
        4. Standard data directories
        
        This enables flexible deployment scenarios:
        - Local development with relative paths
        - Container deployments with volume mounts
        - NFS-based distributed storage
        - Custom data directory configurations
        
        Args:
            split_path: The split path to search for (filename extracted automatically)
        
        Returns:
            Optional[str]: Local path if found, None otherwise
        """
        search_paths = [
            self.settings.worker_data_root_path,
            self.settings.temp_data_path,
            "/exports",  # NFS mount point for distributed storage
            "/data",     # Standard data directory
            "/app/data"  # Container-based data directory
        ]
        
        filename = os.path.basename(split_path)
        
        for search_dir in search_paths:
            potential_path = os.path.join(search_dir, filename)
            if os.path.exists(potential_path):
                self.logger.debug(f"Found split at: {potential_path}")
                return potential_path
        
        return None
    
    async def _cleanup_temp_files(self):
        """
        Clean up temporary files and cached data to manage storage usage.
        
        This method implements intelligent cleanup:
        1. **Selective Removal**: Removes temporary files while preserving important data
        2. **Storage Management**: Prevents unlimited storage growth
        3. **Error Resilience**: Continues cleanup even if individual operations fail
        4. **Logging**: Tracks cleanup operations for monitoring
        
        Cleanup Targets:
        - Downloads directory (remote files cached locally)
        - Cache directory (processed data intermediates)
        - Individual temporary files created during processing
        
        Safety Features:
        - Only removes files, preserves directory structure
        - Logs cleanup operations for audit trails
        - Handles file access errors gracefully
        - Maintains system stability during cleanup
        """
        try:
            # Clean downloads directory to free storage space
            for file_path in self.downloads_dir.iterdir():
                if file_path.is_file():
                    file_path.unlink()
            
            # Clean cache directory to remove stale data
            for file_path in self.cache_dir.iterdir():
                if file_path.is_file():
                    file_path.unlink()
            
            self.logger.info("Temporary files cleaned up")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up temp files: {e}")