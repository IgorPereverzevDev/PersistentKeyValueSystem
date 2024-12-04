from pathlib import Path
from typing import List, Optional, Dict, Tuple, Any, Set

from pysyncobj import SyncObj, replicated, SyncObjConf
import asyncio
import logging
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager
import prometheus_client as prom

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class StoreMetrics:
    operations: prom.Counter
    operation_latency: prom.Histogram
    replication_lag: prom.Gauge
    cluster_size: prom.Gauge
    last_commit_time: prom.Gauge


class KeyValueStoreError(Exception):
    """Base exception for KeyValueStore errors"""
    pass


class ConnectionKeyValueError(KeyValueStoreError):
    """Raised when cluster connection fails"""
    pass


class ReplicationError(KeyValueStoreError):
    """Raised when replication fails"""
    pass


def _setup_metrics() -> StoreMetrics:
    return StoreMetrics(
        operations=prom.Counter(
            'kvstore_operations_total',
            'Total number of operations',
            ['operation', 'status']
        ),
        operation_latency=prom.Histogram(
            'kvstore_operation_latency_seconds',
            'Operation latency in seconds',
            ['operation']
        ),
        replication_lag=prom.Gauge(
            'kvstore_replication_lag_seconds',
            'Replication lag in seconds'
        ),
        cluster_size=prom.Gauge(
            'kvstore_cluster_size',
            'Number of nodes in cluster'
        ),
        last_commit_time=prom.Gauge(
            'kvstore_last_commit_time',
            'Last commit timestamp'
        )
    )


class KeyValueStore(SyncObj):
    def __init__(
            self,
            self_node: str,
            other_nodes: List[str]
    ):
        data_dir = Path("data")
        node_dir = data_dir / self_node.replace(':', '_')

        node_dir.mkdir(parents=True, exist_ok=True)

        # Create paths for dump and journal files
        dump_file = str(node_dir / "dump.bin")
        journal_file = str(node_dir / "journal")

        sync_config = SyncObjConf(
            appendEntriesPeriod=0.01,
            raftMinTimeout=0.2,
            raftMaxTimeout=0.5,
            commandsWaitLeader=True,
            commandsWaitReadyToRead=True,
            commandsWaitReadyToWrite=True,
            appendEntriesBatchSizeBytes=128 * 1024,
            journalFileMaxSize=32 * 1024 * 1024,
            logCompactionMinEntries=5000,
            logCompactionMinTime=300,
            fullDumpStoreMinTime=300,
            dynamicMembershipChange=True,
            onReady=lambda: logger.info(f"Node {self_node} ready callback triggered"),
            retryFailedConnections=True,
            quorum=1,
            fullDumpFile=dump_file,
            journalFile=journal_file
        )

        super().__init__(
            selfNode=self_node,
            otherNodes=other_nodes,
            conf=sync_config
        )

        self._data: Dict[str, bytes] = {}
        self._metrics = _setup_metrics()
        self._last_commit_time = time.time()

        self._tasks = [
            asyncio.create_task(self._update_metrics())
        ]

    def _serialize(self):
        """Return a serializable state snapshot"""
        return {
            'data': self._data,
            'last_commit_time': self._last_commit_time
        }

    def _deserialize(self, data):
        """Restore state from a snapshot"""
        if data:
            self._data = data.get('data', {})
            self._last_commit_time = data.get('last_commit_time', time.time())

    @replicated
    def _put(self, key: str, value: bytes) -> bool:
        """Replicated put operation"""
        try:
            if not self._isReady():
                logger.warning("Node not ready for put operation")
                return False

            self._data[key] = value
            self._last_commit_time = time.time()
            logger.debug(f"Put successful: {key}={value}")
            return True
        except Exception as e:
            logger.error(f"Error in replicated put: {e}")
            return False

    async def put(self, key: str, value: bytes) -> bool:
        """Put a value into the store"""
        async with self.operation("put"):
            try:
                result = self._put(key, value, sync=True)
                if result is None:
                    return False
                return True
            except Exception as e:
                logger.error(f"Put operation failed for key {key}: {str(e)}")
                raise ReplicationError(f"Put operation failed: {str(e)}")

    @replicated
    def _batch_put(self, items: List[Tuple[str, bytes]]) -> bool:
        """Replicated batch put operation"""
        try:
            for key, value in items:
                self._data[key] = value
            self._last_commit_time = time.time()
            return True
        except Exception as e:
            logger.error(f"Error in replicated batch put: {e}")
            return False

    async def batch_put(self, items: List[Tuple[str, bytes]]) -> bool:
        """Batch put values into the store"""
        async with self.operation("batch_put"):
            try:
                success = self._batch_put(items)
                return success
            except Exception as e:
                logger.error(f"Batch put operation failed: {str(e)}")
                raise ReplicationError(f"Batch put operation failed: {str(e)}")

    @replicated
    def _delete(self, key: str) -> bool:
        """Replicated delete operation"""
        try:
            if key in self._data:
                del self._data[key]
                self._last_commit_time = time.time()
                return True
            return False
        except Exception as e:
            logger.error(f"Error in replicated delete: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete a key from the store"""
        async with self.operation("delete"):
            try:
                success = self._delete(key)
                return success
            except Exception as e:
                logger.error(f"Delete operation failed for key {key}: {e}")
                raise ReplicationError(f"Failed to replicate delete: {e}")

    async def get(self, key: str) -> Optional[bytes]:
        """Get a value from the store"""
        async with self.operation("get"):
            return self._data.get(key)

    async def read_key_range(
            self,
            start_key: str,
            end_key: str
    ) -> List[Tuple[str, bytes]]:
        """Read a range of keys from the store"""
        async with self.operation("read_range"):
            return [(k, v) for k, v in self._data.items()
                    if start_key <= k <= end_key]

    async def get_health(self) -> Dict[str, Any]:
        """Get health status with JSON-serializable values"""
        connected_nodes = self._get_connected_nodes()
        total_nodes = len(self.otherNodes) + 1
        is_leader = self._isLeader()
        leader = self._getLeader()

        return {
            "status": "healthy" if self._isReady() else "degraded",
            "cluster_size": total_nodes,
            "connected_nodes": len(connected_nodes),
            "connected_node_addresses": [str(node) for node in connected_nodes],
            "is_leader": is_leader,
            "leader_node": str(leader) if leader else None,
            "last_commit_time": self._last_commit_time,
            "replication_lag": time.time() - self._last_commit_time,
            "ready": self._isReady()
        }

    def _get_connected_nodes(self) -> Set[str]:
        """Get the set of currently connected nodes in the cluster"""
        connected = set()
        if self.selfNode:
            connected.add(self.selfNode)

        if hasattr(self, '_SyncObj__connectedNodes'):
            connected.update(self._SyncObj__connectedNodes)

        return connected

    async def _update_metrics(self):
        """Update Prometheus metrics"""
        while True:
            try:
                self._metrics.cluster_size.set(len(self.otherNodes) + 1)
                self._metrics.last_commit_time.set(self._last_commit_time)
                self._metrics.replication_lag.set(time.time() - self._last_commit_time)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error updating metrics: {e}")
                await asyncio.sleep(1)

    @asynccontextmanager
    async def operation(self, op_name: str):
        """Context manager for recording operation metrics"""
        start_time = time.time()
        try:
            yield
            self._metrics.operations.labels(operation=op_name, status="success").inc()
        except Exception as e:
            self._metrics.operations.labels(operation=op_name, status="failure").inc()
            logger.error(f"Operation {op_name} failed: {e}")
            raise
        finally:
            self._metrics.operation_latency.labels(operation=op_name).observe(
                time.time() - start_time
            )

    def _get_last_commit_time(self) -> float:
        return self._last_commit_time

    async def close(self):
        """Shut down the KeyValueStore"""
        logger.info("Shutting down KeyValueStore")
        for task in self._tasks:
            task.cancel()
        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        finally:
            # Wait for any pending writes
            await asyncio.sleep(0.5)
            self.destroy()
            await asyncio.sleep(0.1)
