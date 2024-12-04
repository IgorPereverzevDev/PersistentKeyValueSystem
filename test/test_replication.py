import asyncio
import time
from typing import List, Optional
import pytest
from unittest.mock import patch
from storage.kvstore import KeyValueStore


class TestReplication:
    @staticmethod
    async def setup_nodes(count: int = 3, base_port: int = 4321) -> List[KeyValueStore]:
        with patch('prometheus_client.Counter'), \
                patch('prometheus_client.Histogram'), \
                patch('prometheus_client.Gauge'):
            nodes = []
            for i in range(count):
                other_nodes = [f"127.0.0.1:{base_port + j}" for j in range(count) if j != i]
                node = KeyValueStore(
                    f"127.0.0.1:{base_port + i}",
                    other_nodes
                )
                nodes.append(node)

            # Wait for initial setup
            await asyncio.sleep(2)

            # Wait for at least one node to become ready
            timeout = 10
            start_time = time.time()
            while time.time() - start_time < timeout:
                for node in nodes:
                    if node._isReady():
                        break
                else:
                    await asyncio.sleep(0.1)
                    continue
                break
            else:
                raise Exception("No node became ready within timeout")

            return nodes

    @staticmethod
    async def wait_for_leader(nodes: List[KeyValueStore], timeout: float = 5.0) -> Optional[KeyValueStore]:
        """Wait for a leader to be elected"""
        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            for node in nodes:
                if node._isReady() and node._isLeader():
                    return node
            await asyncio.sleep(0.1)
        return None

    @staticmethod
    async def verify_replication(nodes: List[KeyValueStore], key: str, expected_value: bytes,
                                 timeout: float = 5.0) -> bool:
        """Verify that a value has been replicated to all nodes"""
        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_matched = True
            for node in nodes:
                if node._isReady():
                    value = await node.get(key)
                    if value != expected_value:
                        all_matched = False
                        break
            if all_matched:
                return True
            await asyncio.sleep(0.1)
        return False

    @pytest.mark.asyncio
    async def test_data_replication(self):
        nodes = await self.setup_nodes()
        try:
            # Wait for leader election with longer timeout
            leader = await self.wait_for_leader(nodes, timeout=10.0)
            assert leader is not None, "No leader was elected"

            # Wait for cluster to stabilize and verify connections
            await asyncio.sleep(1)

            for node in nodes:
                print(f"Node {node.selfNode} state - Ready: {node._isReady()}, Leader: {node._isLeader()}")
                print(f"Connected nodes for {node.selfNode}: {node._get_connected_nodes()}")

            test_key, test_value = "test_key", b"test_value"

            # Try put operation
            success = await leader.put(test_key, test_value)
            assert success is True, "Failed to put value"

            # Wait for replication with a longer timeout
            max_retries = 10
            retry_delay = 0.5
            success = False

            for _ in range(max_retries):
                all_replicated = True
                for node in nodes:
                    if not node._isReady():
                        continue
                    value = await node.get(test_key)
                    if value != test_value:
                        all_replicated = False
                        print(f"Node {node.selfNode} has value: {value}")
                        break

                if all_replicated:
                    success = True
                    break

                await asyncio.sleep(retry_delay)

            assert success, "Data was not replicated to all nodes within timeout"

        finally:
            await asyncio.gather(*[node.close() for node in nodes])

    @pytest.mark.asyncio
    async def test_replication_consistency(self):
        nodes = await self.setup_nodes()
        try:
            # Wait for leader election with longer timeout
            leader = await self.wait_for_leader(nodes, timeout=10.0)
            assert leader is not None, "No leader was elected"

            # Wait for cluster to stabilize
            await asyncio.sleep(1)

            test_key, test_value = "test_key", b"test_value"
            success = await leader.put(test_key, test_value)
            assert success is True, "Failed to put initial value"

            await asyncio.sleep(1)
            for node in nodes:
                if node._isReady():
                    value = await node.get(test_key)
                    assert value == test_value, "Initial value not replicated"

            # Find and close a follower node
            follower = next(node for node in nodes if not node._isLeader())
            await follower.close()

            # Wait for cluster to recognize node failure
            await asyncio.sleep(1)

            new_value = b"updated_value"
            success = await leader.put(test_key, new_value)
            assert success is True, "Failed to put value with node down"

            # Wait for replication to remaining nodes
            await asyncio.sleep(2)

            active_nodes = [node for node in nodes if node != follower]
            for node in active_nodes:
                if node._isReady():
                    value = await node.get(test_key)
                    assert value == new_value, f"Updated value not replicated to node {node.selfNode}"

        finally:
            await asyncio.gather(*[node.close() for node in nodes])