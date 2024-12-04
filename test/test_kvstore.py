import asyncio

import pytest
import pytest_asyncio
from unittest.mock import patch, MagicMock
from db.kvstore import KeyValueStore, ReplicationError


@pytest_asyncio.fixture
async def store():
    """Fixture that provides a KeyValueStore instance with mocked Prometheus metrics"""
    with patch('prometheus_client.Counter'), \
            patch('prometheus_client.Histogram'), \
            patch('prometheus_client.Gauge'):
        store = KeyValueStore(
            "127.0.0.1:8000",
            ["127.0.0.1:8001"]
        )
        yield store
        await store.close()


@pytest.fixture
def mock_ready():
    """Fixture that mocks the _isReady method to return True"""
    with patch.object(KeyValueStore, '_isReady', return_value=True):
        yield


@pytest.mark.asyncio
async def test_put_success(store, mock_ready):
    """Test successful put operation"""
    with patch.object(store, '_put', return_value=True):
        key, value = "test_key", b"test_value"
        assert await store.put(key, value)
        assert store._put.called_with(key, value)


@pytest.mark.asyncio
async def test_put_failure(store, mock_ready):
    """Test put operation failure"""
    with patch.object(store, '_put', side_effect=Exception("Replication failed")):
        with pytest.raises(ReplicationError, match="Put operation failed"):
            await store.put("key", b"value")


@pytest.mark.asyncio
async def test_batch_put_success(store, mock_ready):
    """Test successful batch put operation"""
    items = [("key1", b"value1"), ("key2", b"value2")]
    with patch.object(store, '_batch_put', return_value=True):
        assert await store.batch_put(items)
        store._batch_put.assert_called_once_with(items)


@pytest.mark.asyncio
async def test_batch_put_failure(store, mock_ready):
    """Test batch put operation failure"""
    items = [("key1", b"value1"), ("key2", b"value2")]
    with patch.object(store, '_batch_put', side_effect=Exception("Batch operation failed")):
        with pytest.raises(ReplicationError, match="Batch put operation failed"):
            await store.batch_put(items)


@pytest.mark.asyncio
async def test_delete_success(store, mock_ready):
    """Test successful delete operation"""
    key = "delete_key"
    with patch.object(store, '_delete', return_value=True):
        assert await store.delete(key)
        store._delete.assert_called_once_with(key)


@pytest.mark.asyncio
async def test_delete_failure(store, mock_ready):
    """Test delete operation failure"""
    with patch.object(store, '_delete', side_effect=Exception("Delete failed")):
        with pytest.raises(ReplicationError, match="Failed to replicate delete"):
            await store.delete("key")


@pytest.mark.asyncio
async def test_get(store, mock_ready):
    """Test get operation"""
    key, value = "test_key", b"test_value"
    store._data[key] = value
    result = await store.get(key)
    assert result == value

    # Test non-existent key
    assert await store.get("nonexistent") is None


@pytest.mark.asyncio
async def test_read_key_range(store, mock_ready):
    """Test reading a range of keys"""
    test_data = [
        ("a1", b"val1"),
        ("b2", b"val2"),
        ("c3", b"val3"),
        ("d4", b"val4")
    ]
    for k, v in test_data:
        store._data[k] = v

    # Test inclusive range
    result = await store.read_key_range("a1", "c3")
    assert len(result) == 3
    assert all(item in result for item in test_data[:3])

    # Test empty range
    result = await store.read_key_range("x1", "z1")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_health(store, mock_ready):
    """Test health check functionality"""
    with patch.object(store, '_isLeader', return_value=True), \
            patch.object(store, '_getLeader', return_value="127.0.0.1:8000"), \
            patch.object(store, '_get_connected_nodes', return_value={"127.0.0.1:8000", "127.0.0.1:8001"}):
        health = await store.get_health()

        assert health["status"] == "healthy"
        assert health["is_leader"] is True
        assert health["cluster_size"] == 2
        assert health["connected_nodes"] == 2
        assert health["connected_node_addresses"] == ["127.0.0.1:8000", "127.0.0.1:8001"] or health[
            "connected_node_addresses"] == ["127.0.0.1:8001", "127.0.0.1:8000"]
        assert health["leader_node"] == "127.0.0.1:8000"
        assert isinstance(health["last_commit_time"], float)
        assert isinstance(health["replication_lag"], float)
        assert health["ready"] is True


@pytest.mark.asyncio
async def test_metrics_update(store):
    """Test metrics update task"""
    mock_metrics = MagicMock()
    store._metrics = mock_metrics

    # Create a task that runs for a short time
    task = asyncio.create_task(store._update_metrics())

    # Let it run briefly
    await asyncio.sleep(0.1)

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert mock_metrics.cluster_size.set.called
    assert mock_metrics.last_commit_time.set.called
    assert mock_metrics.replication_lag.set.called
