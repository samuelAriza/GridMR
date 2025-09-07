# GridMR - Distributed MapReduce Framework

## Overview

GridMR is an enterprise-grade distributed MapReduce framework implemented in Python, designed for scalable big data processing across multiple worker nodes. The framework provides a complete implementation of the MapReduce paradigm with automatic job orchestration, fault tolerance, security isolation, and professional distributed computing capabilities.

### Key Features

- 🚀 **Complete MapReduce Workflow**: Automatic Map→Reduce phase transitions with intelligent task orchestration
- 🔒 **Secure Code Execution**: Sandboxed environment for safe execution of user-defined functions
- 🌐 **Distributed Processing**: Horizontal scaling across multiple worker nodes with load balancing
- 📁 **Shared Storage**: NFS-based distributed file system for seamless data access
- 🐳 **Containerized Deployment**: Docker Compose orchestration with proper networking
- 📊 **Monitoring & Logging**: Comprehensive system monitoring with detailed execution metrics
- 🔄 **Fault Tolerance**: Automatic retry mechanisms and graceful error handling
- 🎯 **Production Ready**: Enterprise-level code quality with professional documentation

## Architecture

### System Components

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client API    │    │   Master Node    │    │  Worker Nodes   │
│                 │    │                  │    │                 │
│ Job Submission  │───▶│  Job Manager     │───▶│  Task Executor  │
│ Status Monitor  │    │  Task Scheduler  │    │  Map Processor  │
│ Result Access   │    │  Worker Registry │    │  Reduce Process │
└─────────────────┘    │  Resource Mgmt   │    │  Sandbox Engine │
                       └──────────────────┘    └─────────────────┘
                                │                        │
                                └────────────────────────┘
                                    Distributed NFS Storage
```

### Master Node Components

- **JobManager**: Central orchestrator managing complete MapReduce job lifecycles
- **TaskScheduler**: Intelligent task distribution with load balancing algorithms
- **WorkerRegistry**: Dynamic worker node management and health monitoring
- **API Layer**: RESTful endpoints for job submission, monitoring, and control
- **DataSplitter**: Input data partitioning for optimal parallel processing

### Worker Node Components

- **ExecutionSandbox**: Secure Python execution environment for user functions
- **MapProcessor**: Specialized processor for Map phase operations with data partitioning
- **ReduceProcessor**: Specialized processor for Reduce phase with key grouping
- **MasterClient**: Secure communication interface with master coordination
- **ResourceMonitor**: Real-time system resource monitoring and reporting

## Project Structure

```
gridMR/
├── master/                          # Master node implementation
│   ├── api/                         # REST API layer
│   │   ├── models.py               # API data models and schemas
│   │   ├── routes.py               # API endpoints and handlers
│   │   └── worker_models.py        # Worker communication models
│   ├── core/                       # Core master services
│   │   ├── job_manager.py          # Central job orchestration engine
│   │   ├── task_scheduler.py       # Task distribution and load balancing
│   │   └── data_splitter.py        # Input data partitioning logic
│   ├── models/                     # Data models and schemas
│   │   ├── job.py                  # Job definition and state management
│   │   ├── task.py                 # Task models and execution tracking
│   │   └── worker.py               # Worker node registration models
│   ├── services/                   # Master support services
│   │   ├── worker_registry.py      # Worker lifecycle management
│   │   └── metrics_collector.py    # Performance metrics aggregation
│   ├── utils/                      # Master utilities
│   │   ├── config.py               # Configuration management
│   │   └── logger.py               # Logging infrastructure
│   └── main.py                     # Master node entry point
│
├── worker/                          # Worker node implementation
│   ├── api/                        # Worker API interface
│   │   ├── models.py               # Worker API data models
│   │   └── routes.py               # Worker endpoints for task assignment
│   ├── core/                       # Core worker processing
│   │   ├── worker_engine.py        # Main worker orchestration engine
│   │   ├── task_executor.py        # Task execution coordination
│   │   ├── map_processor.py        # Map phase processing engine
│   │   ├── reduce_processor.py     # Reduce phase processing engine
│   │   └── resource_monitor.py     # System resource monitoring
│   ├── services/                   # Worker support services
│   │   ├── execution_sandbox.py    # Secure code execution environment
│   │   ├── master_client.py        # Master communication client
│   │   ├── data_manager.py         # Data acquisition and storage
│   │   └── heartbeat_service.py    # Health monitoring service
│   ├── utils/                      # Worker utilities
│   │   ├── config.py               # Worker configuration
│   │   └── metrics.py              # Performance metrics collection
│   └── main.py                     # Worker node entry point
│
├── common/                          # Shared components
│   ├── models/                     # Common data models
│   │   ├── base.py                 # Base model definitions
│   │   └── task_context.py         # Task execution context
│   └── utils/                      # Shared utilities
│       ├── logger.py               # Common logging utilities
│       └── config.py               # Shared configuration management
│
├── client/                          # Client interface (future)
│   └── mapreduce_client.py         # Python client for job submission
│
├── data/                           # Data directory
│   ├── input.txt                   # Sample input data
│   └── output/                     # Processing results
│
├── nfs-server/                     # NFS server configuration
├── nfs-client/                     # NFS client setup
├── docker-compose.yml              # Container orchestration
├── payload.json                    # Sample job payload
└── README.md                       # This documentation
```

## Quick Start

### Prerequisites

- **Docker** (version 20.0+)
- **Docker Compose** (version 2.0+)
- **Python** (version 3.8+) for local development
- **4GB RAM** minimum for cluster operation

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd gridMR

# Set proper permissions for data directory
sudo chown -R $USER:$USER data/
chmod 755 data/
```

### 2. Start the Distributed System

```bash
# Start all services (NFS, Master, Workers)
docker-compose up -d

# Verify all services are running
docker-compose ps

# Expected output:
# NAME              SERVICE     STATUS      PORTS
# gridmr-master-1   master      running     0.0.0.0:8000->8000/tcp
# gridmr-worker1-1  worker1     running     0.0.0.0:8001->8001/tcp
# gridmr-worker2-1  worker2     running     0.0.0.0:8002->8002/tcp
# gridmr-nfs-1      nfs         running     2049/tcp, 111/tcp
```

### 3. Submit Your First Job

```bash
# Submit a word count job
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d @payload.json

# Expected response:
# {
#   "job_id": "job_abc123",
#   "status": "submitted",
#   "message": "Job submitted successfully"
# }
```

### 4. Monitor Job Progress

```bash
# Check job status
curl http://localhost:8000/api/v1/jobs/{job_id}/status

# Monitor system health
curl http://localhost:8000/api/v1/health

# View worker status
curl http://localhost:8000/api/v1/workers
```

### 5. Retrieve Results

```bash
# View final consolidated output
cat ./data/output_wordcount.txt

# Example output:
# {"key": "mapreduce", "value": 6}
# {"key": "data", "value": 4}
# {"key": "processing", "value": 3}
# {"key": "distributed", "value": 2}
```

## Usage Examples

### Basic Word Count Job

```json
{
  "client_id": "test-client",
  "job_name": "word_count",
  "map_function": "def map_fn(line):\n    for word in line.strip().split():\n        yield (word.lower(), 1)",
  "reduce_function": "def reduce_fn(key, values):\n    return (key, sum(values))",
  "input_data_path": "/data/input.txt",
  "output_data_path": "/data/output_wordcount.txt",
  "split_size": 1,
  "num_reducers": 1,
  "parameters": {}
}
```

## System Monitoring and Debugging

### Health Checks

```bash
# Master node health
curl http://localhost:8000/api/v1/health

# Worker node health
curl http://localhost:8001/api/v1/health
curl http://localhost:8002/api/v1/health

# System overview
curl http://localhost:8000/api/v1/status
```

### Log Analysis

```bash
# View master logs
docker-compose logs master

# View worker logs
docker-compose logs worker1
docker-compose logs worker2

# View NFS logs
docker-compose logs nfs

# Follow live logs
docker-compose logs -f master

# Filter for specific events
docker-compose logs master | grep "ERROR\|WARN"
```

### Performance Monitoring

```bash
# Job execution metrics
curl http://localhost:8000/api/v1/jobs/{job_id}/metrics

# Worker resource utilization
curl http://localhost:8000/api/v1/workers/{worker_id}/metrics

# System performance overview
curl http://localhost:8000/api/v1/metrics/system
```

### Error Diagnosis

```bash
# Check for failed tasks
curl http://localhost:8000/api/v1/jobs/{job_id}/tasks?status=failed

# View detailed error information
curl http://localhost:8000/api/v1/tasks/{task_id}/errors

# System error summary
curl http://localhost:8000/api/v1/errors/summary
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Input File Not Found
```bash
# Problem: "Input file not found: /data/input.txt"
# Solution: Ensure input file exists and has proper permissions

# Check file existence
ls -la ./data/input.txt

# Fix permissions if needed
sudo chown -R $USER:$USER data/
```

#### 2. Worker Connection Issues
```bash
# Problem: Workers not connecting to master
# Solution: Check network connectivity and docker networking

# Verify containers are running
docker-compose ps

# Check network connectivity
docker-compose exec worker1 ping master
```

#### 3. NFS Mount Issues
```bash
# Problem: NFS mount failures
# Solution: Restart NFS services and check exports

# Restart NFS server
docker-compose restart nfs

# Verify NFS exports
docker-compose exec nfs showmount -e
```

#### 4. Task Execution Failures
```bash
# Problem: Tasks failing with sandbox errors
# Solution: Validate user function code and check logs

# Check task-specific logs
docker-compose logs worker1 | grep "task_"

# Validate function syntax
python -c "compile(your_function_code, '<string>', 'exec')"
```

### Performance Tuning

#### Scaling Workers
```bash
# Add more workers by modifying docker-compose.yml
# Then restart the system:
docker-compose down
docker-compose up -d --scale worker1=3 --scale worker2=2
```

#### Memory Optimization
```yaml
# In docker-compose.yml, adjust memory limits:
services:
  worker1:
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 1G
```

#### Storage Performance
```bash
# For better I/O performance, use SSD storage for data directory
# Mount data directory to high-performance storage:
# /high-speed-storage/gridmr-data:/exports
```

## API Reference

### Job Management Endpoints

- `POST /api/v1/jobs` - Submit new MapReduce job
- `GET /api/v1/jobs` - List all jobs with status
- `GET /api/v1/jobs/{job_id}` - Get specific job details
- `GET /api/v1/jobs/{job_id}/status` - Get job execution status
- `GET /api/v1/jobs/{job_id}/metrics` - Get job performance metrics
- `DELETE /api/v1/jobs/{job_id}` - Cancel running job

### Worker Management Endpoints

- `GET /api/v1/workers` - List all registered workers
- `GET /api/v1/workers/{worker_id}` - Get worker details
- `GET /api/v1/workers/{worker_id}/metrics` - Get worker metrics
- `POST /api/v1/workers/{worker_id}/restart` - Restart worker

### System Monitoring Endpoints

- `GET /api/v1/health` - System health check
- `GET /api/v1/status` - System status overview
- `GET /api/v1/metrics/system` - System-wide metrics
- `GET /api/v1/errors/summary` - Error summary report

## Development

### Local Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r master/requirements.txt
pip install -r worker/requirements.txt

# Run master locally
cd master && python main.py

# Run worker locally (in another terminal)
cd worker && python main.py
```

### Running Tests

```bash
# Run unit tests
python -m pytest tests/

# Run integration tests
python -m pytest tests/integration/

# Run with coverage
python -m pytest --cov=master --cov=worker tests/
```

### Code Quality

```bash
# Format code
black master/ worker/ common/

# Lint code
flake8 master/ worker/ common/

# Type checking
mypy master/ worker/ common/
```

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

### Coding Standards

- Follow PEP 8 style guidelines
- Add comprehensive docstrings to all functions
- Include type hints for all function parameters
- Write unit tests for new functionality
- Update documentation for API changes

---

**GridMR** - Empowering distributed data processing with professional-grade MapReduce capabilities.
