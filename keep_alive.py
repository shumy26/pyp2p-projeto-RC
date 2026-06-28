import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from peer_table import *
import project_config as cfg

logger = logging.getLogger("KeepAlive")

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


def build_ping(msg_id=None, timestamp=None, ttl=1):
    return {
        "type": "PING",
        "msg_id": str(msg_id or uuid.uuid4()),
        "timestamp": timestamp or _timestamp_now(),
        "ttl": ttl,
    }


def build_pong(msg_id, timestamp, ttl=1):
    return {
        "type": "PONG",
        "msg_id": str(msg_id),
        "timestamp": timestamp,
        "ttl": ttl,
    }


async def handle_keep_alive_message(payload, writer, peer_id=None):
    if not isinstance(payload, dict):
        return False

    if payload.get("type") != "PING":
        return False

    pong = build_pong(
        msg_id=payload.get("msg_id"),
        timestamp=payload.get("timestamp"),
        ttl=payload.get("ttl", 1),
    )
    writer.write((json.dumps(pong) + "\n").encode())
    await writer.drain()

    if peer_id:
        logger.debug("PING received from %s; PONG sent.", peer_id)
    else:
        logger.debug("PING received; PONG sent.")

    return True


async def keep_alive_receiver(reader, writer, peer_id=None):
    try:
        while not reader.at_eof():
            message = await reader.readline()
            if not message:
                break

            try:
                payload = json.loads(message.decode("utf-8"))
                handled = await handle_keep_alive_message(payload, writer, peer_id=peer_id)
                if not handled:
                    logger.info("Mensagem nao relacionada a keep_alive: %s", payload)
            except json.JSONDecodeError:
                logger.warning("Mensagem invalida recebida no keep_alive.")
    except asyncio.CancelledError:
        return None
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    return None

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

async def ping_loop(peer:Peer,interval=cfg.getConfig().get('keep_alive_interval')):
    try:
        while True:
            await ping(peer)
            await asyncio.sleep(interval)
    except TypeError:
        logger.debug("Probably, the 'keep_alive_interval' parameter in config file is configured wrong.")

async def start_PingPong():
    try:
        async with peer_table_lock:
            peers_not_connected = peer_table.get('nconn')
    
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(ping_loop(peer)) for peer in peers_not_connected if (peer.get('direction') != 'STALE')]
        
    except asyncio.CancelledError:
        logger.debug(f"Task 'start_PingPong' closed.")
