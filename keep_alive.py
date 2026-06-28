import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from peer_table import *
import project_config as cfg

logger = logging.getLogger("KeepAlive")

async def disconnect_peer(peer:Peer, writer:asyncio.StreamWriter, reader:asyncio.StreamReader):

    my_id = _local_peer_id()
    peer_id = _peer_id_from_peer_table(peer)
    msg_size = cfg.getConfig().get('msg_size',32768)
    lock = peer.get('lock')
    try:
        bye_request = json.dumps(build_bye(my_id, peer_id)).encode()
        
        async with lock:
            writer.write(bye_request)
            await writer.drain()

            try:
                response = await asyncio.wait_for(reader.read(msg_size), timeout=cfg.getConfig().get('timeout',5))
                response_dict = json.loads(response.decode())
                if response_dict.get('type') == 'BYE_OK':
                    pass
            except asyncio.TimeoutError, json.JSONDecodeError, AttributeError:
                pass

            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            peer.set('rtt',-1)
            peer.set('direction','STALE')
            peer.set('conn', None)
            peer_table.degrade(peer)
        return True
    except:
        return False

def _timestamp_now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

def _local_peer_id():
    config_dict = cfg.getConfig()
    return f"{config_dict.get('name')}@{config_dict.get('namespace')}"

def _peer_id_from_peer_table(peer:Peer):
    if not isinstance(peer, Peer):
        return None

    name = peer.get("name") if isinstance(peer, Peer) else None
    namespace = peer.get("namespace") if isinstance(peer, Peer) else None
    if name and namespace:
        return f"{name}@{namespace}"

    return None

def build_bye(src_id=None, dst_id=None,msg_id=None,reason="Closing session...",ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "BYE",
        "msg_id": (msg_id or uuid.uuid4()),
        "src": src_id,
        "dst": dst_id,
        "reason": reason,
        "ttl": ttl//ttl
}


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
    try:
        async with peer_table_lock:
            peers_connected = peer_table.get('conn')

        tasks = [ping_loop(peer) for peer in peers_connected if (peer.get('direction') != 'STALE')]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
    except asyncio.CancelledError:
        logger.debug(f"Task 'start_PingPong' closed.")
