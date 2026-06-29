import asyncio
import json
import logging
import time
import uuid

from peer_table import *
import project_config as cfg
from p2p_client import disconnect_peer
from message_router import _timestamp_now, _peer_id as _peer_id_from_peer_table

logger = logging.getLogger("KeepAlive")


def build_ping(msg_id=None, timestamp=None, ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "PING",
        "msg_id": str(msg_id or uuid.uuid4()),
        "timestamp": timestamp or _timestamp_now(),
        "ttl": ttl//ttl,
    }


def build_pong(msg_id, timestamp, ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "PONG",
        "msg_id": str(msg_id),
        "timestamp": timestamp,
        "ttl": ttl//ttl,
    }


async def ping(peer:Peer, wait_time=cfg.getConfig().get('timeout'), msg_size=cfg.getConfig().get('msg_size',32768),attempts=cfg.getConfig().get('max_reconnect_attempts')):

    peer_id = _peer_id_from_peer_table(peer)
    config_dict = cfg.getConfig()
    ttl = config_dict.get("keep_alive_ttl", 1)
    reader, writer = peer.get('conn')
    tries = 1

    try:
        async with peer.get('lock'):
            ping_dict = build_ping(ttl=ttl)
            request_string = json.dumps(ping_dict) + '\n'

            while tries < attempts:
                logger.debug(f"Sending PING: {json.dumps(ping_dict, ensure_ascii=False)} (Attempt {tries}).")

                started_at = time.perf_counter()
                writer.write(request_string.encode())
                await writer.drain()
                
                try:
                    response_string = await asyncio.wait_for(reader.read(msg_size), wait_time)
                
                    response_string = response_string.decode().strip('\n')
                    elapsed_ms = (time.perf_counter() - started_at) * 1000
                    peer.set('rtt',elapsed_ms)

                    response_dict = json.loads(response_string)
                    match response_dict.get('type'):
                        case 'PONG':
                            logger.debug(f"PING sent to {peer_id}, PONG received.")
                            pass
                        case _:
                            logger.debug(f"The peer sent an invalid response to PING. Disconnecting from {peer_id}.")
                            break

                    return True

                except asyncio.TimeoutError:
                    await asyncio.sleep(1)
                    tries += 1

        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        if disconnected:
            return False


    except ConnectionRefusedError:
        logger.debug("The peer refused the keep_alive connection.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None
    except TimeoutError:
        logger.debug("Could not connect to peer due to timed out attempt.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None
    except OSError:
        logger.debug("There was a network issue and it was not possible to connect to the peer.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None
    except (InterruptedError, KeyboardInterrupt):
        logger.debug("The keep_alive loop was interrupted by a signal.")
        return None
    except json.JSONDecodeError:
        logger.debug("The peer returned an invalid JSON response for keep_alive.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None

async def ping_loop(peer:Peer,interval=1):
    interval = cfg.getConfig().get("keep_alive_interval",30)
    try:
        while True:
            logger.debug(f"The code entred in the ping_loop with {interval} seconds")
            await ping(peer)
            logger.debug(f"The code tried to ping to {peer}.")
            await asyncio.sleep(interval)
    except TypeError:
        logger.debug("Probably, the 'keep_alive_interval' parameter in config file is configured wrong.")

async def start_PingPong():

    async with peer_table_lock:
        peers_connected = peer_table.get('conn')

    tasks = [ping_loop(peer) for peer in peers_connected if (peer.get('direction') != 'STALE')]
    results = await asyncio.gather(*tasks, return_exceptions=True)
        
    if results:
        return True
