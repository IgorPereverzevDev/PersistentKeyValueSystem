import argparse
import asyncio
import logging
import os
import json

from storage.kvstore import KeyValueStore

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def run_node(host: str, port: int):
    node_addr = os.getenv('SELF_ADDR', f"{host}:{port}")

    if os.getenv('OTHER_NODES'):
        other_nodes = json.loads(os.getenv('OTHER_NODES'))
    else:
        cluster_ports = [4321, 4322, 4323, 4324]
        other_ports = [p for p in cluster_ports if p != port]
        other_nodes = [f"{host}:{p}" for p in other_ports]

    store = KeyValueStore(node_addr, other_nodes)

    while True:
        is_ready = store.isReady()
        is_leader = store._isLeader()
        logger.info(f"Node {node_addr} status - Ready: {is_ready}, Leader: {is_leader}")
        if is_ready:
            logger.info(f"Node {node_addr} is fully connected to cluster")
        await asyncio.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="localhost")
    args = parser.parse_args()
    asyncio.run(run_node(args.host, args.port))