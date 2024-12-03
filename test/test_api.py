import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
import msgpack

from app import app, ConnectionKeyValueError


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_kv_store():
    with patch('app.KeyValueStore') as mock:
        mock_instance = AsyncMock()
        mock.return_value = mock_instance
        mock_instance.wait_ready.return_value = True
        app.state.kv_store = mock_instance
        yield mock_instance


@pytest.mark.asyncio
def test_put_success(client, mock_kv_store):
    mock_kv_store.put.return_value = True
    response = client.put("/kv/test_key", json="test")
    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    mock_kv_store.put.assert_called_once_with("test_key", msgpack.packb("test"))


@pytest.mark.asyncio
async def test_put_value_connection_error(client, mock_kv_store):
    mock_kv_store.put.side_effect = ConnectionKeyValueError("Failed")
    response = client.put("/kv/test_key", json={"value": "test"})
    assert response.status_code == 503
    assert "Failed" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_value_success(client, mock_kv_store):
    test_value = msgpack.packb({"value": "test"})
    mock_kv_store.get.return_value = test_value
    response = client.get("/kv/test_key")
    assert response.status_code == 200
    assert response.json() == {"value": {"value": "test"}}


@pytest.mark.asyncio
async def test_get_value_not_found(client, mock_kv_store):
    mock_kv_store.get.return_value = None
    response = client.get("/kv/key")
    assert response.status_code == 404
    assert response.json()["detail"] == "Key not found"


@pytest.mark.asyncio
async def test_batch_put_success(client, mock_kv_store):
    mock_kv_store.batch_put.return_value = True
    items = [{"key": "key1", "value": "val1"}, {"key": "key2", "value": "val2"}]
    response = client.put("/kv/batch", json={"items": items})
    assert response.status_code == 200
    assert response.json() == {"status": "success"}


@pytest.mark.asyncio
async def test_delete_value_success(client, mock_kv_store):
    mock_kv_store.delete.return_value = True
    response = client.delete("/kv/test_key")
    assert response.status_code == 200
    assert response.json() == {"status": "success"}


@pytest.mark.asyncio
async def test_read_key_range_success(client, mock_kv_store):
    test_data = [("key1", msgpack.packb("val1")), ("key2", msgpack.packb("val2"))]
    mock_kv_store.read_key_range.return_value = test_data
    response = client.post("/kv/range", json={
        "start_key": "key1",
        "end_key": "key2"
    })
    assert response.status_code == 200
    assert len(response.json()["results"]) == 2


@pytest.mark.asyncio
async def test_health_check(client, mock_kv_store):
    mock_kv_store.get_health.return_value = {
        "status": "healthy",
        "cluster_size": 3,
        "is_leader": True
    }
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
