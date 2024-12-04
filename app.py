import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import msgpack
import uvicorn
from fastapi import FastAPI, HTTPException, Body
from pysyncobj.node import TCPNode

from cluster import wait_for_cluster_connection
from db.kvstore import KeyValueStore, ReplicationError, ConnectionKeyValueError
from model.model import KeyRange, BatchPutRequest
from settings import settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class State:
    def __init__(self):
        self.kv_store: Optional[KeyValueStore] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info(
            f"Starting node on port {settings.PORT} - "
            f"Cluster ports: {settings.cluster_ports}"
        )

        # Initialize store with increased timeouts
        app.state.kv_store = KeyValueStore(
            settings.self_addr,
            settings.other_nodes
        )

        # Wait for cluster formation
        if not await wait_for_cluster_connection(app.state.kv_store):
            raise ConnectionKeyValueError(
                f"Failed to establish cluster connection for node {settings.PORT}: "
                "Could not connect to sufficient nodes or sync with leader"
            )

        logger.info(f"Node {settings.PORT} successfully initialized and ready")
        yield

    except Exception as e:
        logger.error(f"Node {settings.PORT} failed to initialize: {e}")
        if app.state.kv_store:
            try:
                health = await app.state.kv_store.get_health()
                logger.error(f"Final cluster state for node {settings.PORT}: {health}")
            except Exception as health_error:
                logger.error(f"Could not get final health status: {health_error}")
        raise ConnectionKeyValueError(f"Failed to initialize store: {e}")

    finally:
        if app.state.kv_store:
            logger.info(f"Shutting down node {settings.PORT}")
            await app.state.kv_store.close()


app = FastAPI(lifespan=lifespan)
app.state = State()


@app.put("/kv/{key}")
async def put(key: str, value: Any = Body(...)) -> Dict[str, str]:
    try:
        if not app.state.kv_store:
            raise ConnectionKeyValueError("Store not initialized")
        value_bytes = msgpack.packb(value)
        success = await app.state.kv_store.put(key, value_bytes)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store value")
        return {"status": "success"}
    except ConnectionKeyValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ReplicationError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/kv/{key}")
async def get(key: str) -> Dict[str, Any]:
    try:
        if not app.state.kv_store:
            raise ConnectionKeyValueError("Store not initialized")
        value_bytes = await app.state.kv_store.get(key)
        if value_bytes is None:
            raise HTTPException(status_code=404, detail="Key not found")
        return {"value": msgpack.unpackb(value_bytes)}
    except ConnectionKeyValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/kv/batch")
async def batch_put(request: BatchPutRequest) -> Dict[str, str]:
    try:
        if not app.state.kv_store:
            raise ConnectionKeyValueError("Store not initialized")
        items = [(item.key, msgpack.packb(item.value)) for item in request.items]
        success = await app.state.kv_store.batch_put(items)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store values")
        return {"status": "success"}
    except ConnectionKeyValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ReplicationError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/kv/{key}")
async def delete(key: str) -> Dict[str, str]:
    try:
        if not app.state.kv_store:
            raise ConnectionKeyValueError("Store not initialized")
        success = await app.state.kv_store.delete(key)
        if not success:
            raise HTTPException(status_code=404, detail="Key not found")
        return {"status": "success"}
    except ConnectionKeyValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ReplicationError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/kv/range")
async def read_key_range(range_request: KeyRange) -> Dict[str, list]:
    try:
        if not app.state.kv_store:
            raise ConnectionKeyValueError("Store not initialized")
        results = await app.state.kv_store.read_key_range(
            range_request.start_key,
            range_request.end_key
        )
        return {
            "results": [
                {"key": key, "value": msgpack.unpackb(value)}
                for key, value in results
            ]
        }
    except ConnectionKeyValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    try:
        if not app.state.kv_store:
            logger.error("Health check failed: Store not initialized")
            raise ConnectionKeyValueError("Store not initialized")

        health = await app.state.kv_store.get_health()

        if isinstance(health.get('leader_node'), TCPNode):
            health['leader_node'] = str(health['leader_node'])

        health.update({
            "node_port": settings.PORT,
            "api_port": settings.API_PORT,
            "cluster_ports": settings.cluster_ports,
            "other_nodes": settings.other_nodes
        })

        if 'connected_node_addresses' in health:
            health['connected_node_addresses'] = [
                str(node) if isinstance(node, TCPNode) else node
                for node in health['connected_node_addresses']
            ]

        logger.debug(f"Health check response: {health}")
        return health

    except ConnectionKeyValueError as e:
        logger.error(f"Health check failed with connection error: {e}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": str(e),
                "status": "connection_error",
                "node_port": settings.PORT
            }
        )
    except Exception as e:
        logger.error(f"Health check failed with unexpected error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "status": "error",
                "node_port": settings.PORT
            }
        )


@app.get("/debug/replication")
async def debug_replication() -> Dict[str, Any]:
    """Get detailed replication status for debugging"""
    try:
        if not app.state.kv_store:
            raise ConnectionKeyValueError("Store not initialized")

        store = app.state.kv_store
        leader = store._getLeader()
        connected_nodes = store._get_connected_nodes()

        status = {
            "node_address": settings.self_addr,
            "is_leader": store._isLeader(),
            "leader": str(leader) if leader else None,
            "is_ready": store.isReady(),
            "connected_nodes": {
                "count": len(connected_nodes),
                "addresses": [str(node) for node in connected_nodes]
            },
            "total_nodes": len(store.otherNodes) + 1,
            "last_commit_time": store._get_last_commit_time(),
            "replication_lag": time.time() - store._get_last_commit_time(),
            "cluster_time": time.time(),
            "data_size": len(store._data)
        }

        return status

    except Exception as e:
        logger.error(f"Debug replication check failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "status": "error",
                "node": settings.self_addr
            }
        )


if __name__ == "__main__":
    logger.info(
        f"Starting server - "
        f"API: {settings.API_HOST}:{settings.API_PORT}, "
        f"Node: {settings.self_addr}, "
        f"Cluster: {settings.cluster_ports}"
    )

    uvicorn.run(
        app,
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="debug"
    )
