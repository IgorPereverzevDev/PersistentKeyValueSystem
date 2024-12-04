# Distributed Key-Value Store

A network-available persistent Key/Value system with replication support, implemented as part of the Moniepoint assessment. The system implements a distributed key-value store with support for multiple nodes, automatic failover, and monitoring capabilities.

## Features

- Basic Key/Value operations (Put, Get, Delete)
- Batch operations support
- Range queries
- Data replication across multiple nodes
- Automatic failover
- Prometheus metrics integration
- Grafana dashboards
- Crash recovery with journal files
- Support for datasets larger than RAM

## Architecture

The system is built using:
- Python FastAPI for the HTTP API
- PySyncObj for replication and consensus
- Prometheus for metrics collection
- Grafana for visualization
- Docker and Docker Compose for containerization

## Requirements

- Python 3.9+
- Docker and Docker Compose
- 16GB RAM (recommended)
- 10GB free disk space

## Installation & Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd PersistentKeyValueSystem
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
pip install -r requirements.txt
```

3. Create Prometheus configuration directory:
```bash
mkdir prometheus
```

## Running Locally

### Using Docker Compose (Recommended)

1. Start the entire cluster:
```bash
docker-compose up --build
```

This will start:
- 4 KV store nodes (ports 8000-8003)
- Prometheus (port 9090)
- Grafana (port 3000)

2. Access the services:
- Node 1: http://localhost:8000
- Node 2: http://localhost:8001
- Node 3: http://localhost:8002
- Node 4: http://localhost:8003
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (login: admin/admin)

### Running Individual Nodes (Development)

Run each node in a separate terminal:

```bash
# Terminal 1 - Node 1
python start_node.py --port 4321 --host localhost

# Terminal 2 - Node 2
python start_node.py --port 4322 --host localhost

# Terminal 3 - Node 3
python start_node.py --port 4323 --host localhost

# Terminal 4 - Node 4
python start_node.py --port 4324 --host localhost
```

## API Endpoints

- `PUT /kv/{key}` - Store a value
- `GET /kv/{key}` - Retrieve a value
- `DELETE /kv/{key}` - Delete a value
- `PUT /kv/batch` - Batch store operation
- `POST /kv/range` - Range query
- `GET /health` - Health check
- `GET /debug/replication` - Replication status

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_kvstore.py

```

## Performance Benchmarks

Benchmarks were run on a MacBook Pro (2.3 GHz 8-core Intel, 16 GB memory):

### Single Operations
- PUT operations: 6.42 ops/second
- GET operations: 542.34 ops/second

### Batch Operations
| Batch Size | Operations/Second |
|------------|------------------|
| 10         | 63.88           |
| 50         | 308.81          |
| 100        | 796.44          |
| 500        | 1,767.32        |

### Range Queries
| Range Size | Queries/Second |
|------------|---------------|
| 10         | 327.95       |
| 50         | 269.29       |
| 100        | 219.02       |
| 500        | 82.10        |

### Concurrent Client Performance
| Clients | Operations/Second |
|---------|------------------|
| 5       | 5.98            |
| 10      | 5.96            |
| 25      | 5.96            |
| 50      | 6.00            |
| 100     | 6.35            |

Full benchmark results are available in the `benchmark_results` directory.

## Monitoring

1. Access Grafana at http://localhost:3000
2. Use default credentials (admin/admin)
3. Navigate to the "Key-Value Store" dashboard

Key metrics available:
- Operation throughput
- Operation latency
- Replication lag
- Cluster size
- Node status

## Troubleshooting

### Common Issues

1. Node not connecting to cluster:
   - Check if all required ports are available
   - Verify network connectivity between nodes
   - Check logs for connection errors

2. Prometheus not collecting metrics:
   - Verify prometheus.yml configuration
   - Check if nodes are exposing metrics endpoint
   - Check Prometheus targets page

3. Data persistence issues:
   - Check disk permissions
   - Verify journal files in data directory
   - Check available disk space

## Implementation Details

The system uses:
- Raft consensus algorithm for leader election and replication
- Journal-based persistence for crash recovery
- In-memory storage with disk backup
- Prometheus for real-time metrics
- Docker for containerization and deployment

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
