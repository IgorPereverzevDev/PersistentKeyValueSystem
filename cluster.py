import asyncio
import logging

from db.kvstore import KeyValueStore
from settings import settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def wait_for_leader_sync(store: KeyValueStore, timeout: float = 30.0) -> bool:
    """Wait for the node to sync with the leader"""
    start_time = asyncio.get_event_loop().time()

    while (asyncio.get_event_loop().time() - start_time) < timeout:
        try:
            health = await store.get_health()

            if health['is_leader']:
                logger.info(f"Node {settings.PORT} is the leader")
                return True

            if health['leader_node'] and health['status'] == 'healthy':
                logger.info(
                    f"Node {settings.PORT} synced with leader "
                    f"{health['leader_node']}"
                )
                return True

            logger.debug(
                f"Waiting for leader sync - "
                f"Leader: {health['leader_node']}, "
                f"Status: {health['status']}, "
                f"Lag: {health['replication_lag']:.2f}s"
            )

            await asyncio.sleep(1)

        except Exception as e:
            logger.warning(f"Error checking leader sync: {e}")
            await asyncio.sleep(1)

    return False


async def wait_for_cluster_connection(store: KeyValueStore, max_attempts: int = 10) -> bool:
    """Wait for the cluster to establish connections with exponential backoff"""
    total_nodes = len(settings.cluster_ports)
    required_nodes = (total_nodes // 2) + 1

    for attempt in range(max_attempts):
        try:
            health = await store.get_health()
            connected = health['connected_nodes']

            logger.info(
                f"Cluster connection attempt {attempt + 1}/{max_attempts}: "
                f"Connected {connected}/{total_nodes} nodes "
                f"(need {required_nodes} for consensus) - "
                f"Port {settings.PORT}"
            )

            if connected >= required_nodes:
                logger.info(
                    f"Node {settings.PORT} ready: "
                    f"{connected}/{total_nodes} nodes connected"
                )
                # Wait for leader sync after connecting
                if await wait_for_leader_sync(store):
                    return True
                logger.warning("Failed to sync with leader, retrying connection")

            if attempt < max_attempts - 1:
                wait_time = min(1 * (2 ** attempt), 10)
                logger.debug(f"Waiting {wait_time}s before next attempt...")
                await asyncio.sleep(wait_time)

        except Exception as e:
            logger.warning(f"Error checking cluster health (attempt {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                wait_time = min(1 * (2 ** attempt), 10)
                await asyncio.sleep(wait_time)

    return False



