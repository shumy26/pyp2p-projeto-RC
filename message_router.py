import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
import traceback

import project_config as cfg
import peer_table as pt

logger = logging.getLogger("Router")

def _local_peer_id() -> str:
    # Build the current node identity in the form name@namespace.
    name = cfg.getConfig().get('name', '')
    namespace = cfg.getConfig().get('namespace', '')
    return f"{name}@{namespace}" if name and namespace else (name or namespace or "unknown")

def _get_ttl() -> int:
    if cfg.getConfig().get("ttl") is None:
        return 1
    return cfg.getConfig().get("ttl")

def _peer_id(peer: pt.Peer | None) -> str | None:
    # Convert a Peer object to its network identifier for use in message headers.
    if not isinstance(peer, pt.Peer):
        return None
    peer_name = peer.get("name")
    peer_namespace = peer.get("namespace")
    if peer_name and peer_namespace:
        return f"{peer_name}@{peer_namespace}"
    return peer_name or peer_namespace


def _timestamp_now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

def build_hello(peer_id, version="1.0", features=None, ttl=cfg.getConfig().get('ttl',1)):
    if features is None:
        features = []
    return {
        "type": "HELLO",
        "peer_id": str(peer_id),
        "version": str(version),
        "features": features,
        "ttl": ttl//ttl,
    }

def build_hello_ok(peer_id, version="1.0", features=None, ttl=cfg.getConfig().get('ttl',1)):
    if features is None:
        features = []
    return {
        "type": "HELLO_OK",
        "peer_id": str(peer_id),
        "version": str(version),
        "features": features,
        "ttl": ttl//ttl,
    }

def build_ack(msg_id=None, timestamp=None, ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "ACK",
        "msg_id": str(msg_id or uuid.uuid4()),
        "timestamp": timestamp or _timestamp_now(),
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


def build_bye(src_id=_local_peer_id(), dst_id=None,msg_id=None,reason="Closing session...",ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "BYE",
        "msg_id": str(msg_id or uuid.uuid4()),
        "src": src_id,
        "dst": dst_id,
        "reason": reason,
        "ttl": ttl//ttl
}


def build_bye_ok(src_id=_local_peer_id(), dst_id=None,msg_id=None,ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "BYE_OK",
        "msg_id": str(msg_id or uuid.uuid4()),
        "src": src_id,
        "dst": dst_id,
        "ttl": ttl//ttl
            }

async def _send_pong(peer:pt.Peer, msg_id, timestamp):
    try:
        pong_message = (json.dumps(build_pong(msg_id=msg_id, timestamp=timestamp)) + '\n').encode()
        lock = peer.get('lock')
        _, writer = _get_peer_streams(peer)
        async with lock:
            writer.write(pong_message)
            await writer.drain()
        return True
    except:
        return False


def _get_peer_streams(peer: pt.Peer | None):
    # Resolve the active reader/writer pair from the connected peer instance.
    if not isinstance(peer, pt.Peer):
        logger.error("A valid Peer instance is required to route a message.")
        return None, None

    conn = peer.get("conn")
    if isinstance(conn, tuple) and len(conn) == 2:
        return conn

    logger.warning("Peer %s is not connected.", _peer_id(peer) or peer)
    return None, None


async def _send_data(writer: asyncio.StreamWriter, data):
    # Serialize the payload and send it over the peer stream as a newline-delimited JSON message.
    json_data = json.dumps(data).encode('utf-8') + b'\n'
    if cfg.getConfig().get("msg_size") is None or cfg.getConfig().get("msg_size",32768) > 32768:
        max_msg_size = 32768
    else:
        max_msg_size = cfg.getConfig().get("msg_size")
        
    if len(json_data) > max_msg_size:
        logger.error(f"message size exceeds the limit of {max_msg_size} bytes.")
        return False

    async with pt.peer_table_lock:
        for peer in pt.peer_table.get('conn'):
            _, peer_writer = peer.get('conn')
            if peer_writer == writer:
                current_peer = peer
                break
    try:
        lock = current_peer.get('lock')    
        async with lock:
            writer.write(json_data)
            await writer.drain()
            return True
    except NameError:
        logger.debug(f"No Peers with IP: {peer.get("ip")} and PORT: {peer.get("port")}.")


async def _read_message(reader: asyncio.StreamReader):
    # Read one message from the peer stream and decode it into a Python dictionary.
    try:
        data = await reader.readuntil(b'\n')

        if not data:
            return None
        
        return json.loads(data.decode('utf-8').strip('\n'))

    except asyncio.IncompleteReadError as exc:
        raise ConnectionError("Connection closed while reading message.") from exc
    except Exception as e:
        logger.debug(f"Error reading message: {e}")
        raise


async def _send_to_peer(peer: pt.Peer, data: dict):
    # Send a prepared message payload to the peer using the stored connection.
    try:
        _, writer = _get_peer_streams(peer)
        if writer is None:
            return False
        return await _send_data(writer, data)
    except OSError:
        pt.peer_table.get('conn').degrade(peer)
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        return False

async def send_message(peer: pt.Peer, src: str, message: str):
    # Send a point-to-point message and expect an ACK from the peer.
    if not isinstance(peer, pt.Peer):
        logger.debug("A valid Peer instance is required to send a message.")
        return False

    ttl = _get_ttl()//_get_ttl()
    message_id = str(uuid.uuid4())
    data = {
        "type": "SEND",
        "msg_id": message_id,
        "src": src or _local_peer_id(),
        "dest": _peer_id(peer),
        "payload": message,
        "require_ack": True,
        "ttl": ttl,
    }
    
    _, writer = peer.get('conn')
    lock = peer.get('lock')
    timeout = cfg.getConfig().get("timeout",5)
    try:
        future = asyncio.get_running_loop().create_future()
        peer.set('ack', future)

        async with lock:
            writer.write((json.dumps(data) + '\n').encode())
            await writer.drain()

        await asyncio.wait_for(future, timeout=timeout)
        logger.debug(f"ACK received from {_peer_id(peer)} with success!")
        return True

    except (asyncio.TimeoutError, TimeoutError):
        logger.debug(f"[Timeout] Failed to receive acknowledgment from {_peer_id(peer)} for the sent message.")
        return False

    finally:
        peer.set('ack', None)

async def send_ack(peer: pt.Peer, message_id: str = ''):
    # Reply to a message with an ACK carrying the original message identifier.
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ack_data = build_ack(message_id)
    return await _send_to_peer(peer, ack_data)


async def _get_publish_targets(namespace: str):
    # Select all connected peers in the requested namespace, or every connected peer for wildcard broadcasts.
    async with pt.peer_table_lock:
        connected_peers = list(pt.peer_table.get("conn") or set())
        if namespace == "*":
            return connected_peers
        return [peer for peer in connected_peers if peer.get("namespace") == namespace]


async def publish_message(namespace: str, src: str, message: str):
    # Publish a message to every connected peer in the requested namespace, or broadcast to all connected peers when namespace is *.
    targets = await _get_publish_targets(namespace or "")
    if not targets:
        logger.debug("No connected peers found for publish namespace %s.", namespace or "*")
        return False

    message_id = str(uuid.uuid4())
    data = {
        "type": "PUB",
        "msg_id": message_id,
        "src": src or _local_peer_id(),
        "dst": namespace or "*",
        "payload": message,
        "require_ack": False,
        "ttl": _get_ttl(),
    }

    for peer in targets:
        try:
            await _send_to_peer(peer, data)
        except:
            logger.debug(f"Something went wrong when trying to publish to {peer}.")

    return True


async def send_bye(peer: pt.Peer, reason: str = "Closing session..."):
    # Notify the peer that the local side is closing the connection.
    if not isinstance(peer, pt.Peer):
        logger.debug("A valid Peer instance is required to send a BYE message.")
        return False

    bye_dict = build_bye(dst_id=_peer_id(peer),reason=reason)
    bye_message = (json.dumps(bye_dict) + '\n').encode()

    lock = peer.get("lock")
    _, writer = peer.get("conn")
    timeout = (peer.get('rtt')/1000 + 1) if (peer.get('rtt') != -1) else cfg.getConfig().get("timeout",5)

    try:
        future = asyncio.get_event_loop().create_future()
        peer.set('bye',future)
        async with lock:
            writer.write(bye_message)
            await writer.drain()
        
        await asyncio.wait_for(future, timeout=timeout)
        return True

    except (asyncio.TimeoutError, TimeoutError):
        logger.debug(f"[Timeout] Failed to receive BYE_OK from {_peer_id(peer)}. It will be disconnected either way.")
        raise TimeoutError
    

async def send_bye_ok(peer: pt.Peer, src: str, msg_id_to_ack: str):
    # Acknowledge a BYE request from the peer.
    if not isinstance(peer, pt.Peer):
        logger.error("A valid Peer instance is required to acknowledge a BYE.")
        return False

    data = build_bye_ok(dst_id=_peer_id(peer))

    return await _send_to_peer(peer, data)


async def handle_incoming_message(peer: pt.Peer):
    # Process incoming messages from the peer until the stream closes.
    reader, writer = _get_peer_streams(peer)
    peer_id = _peer_id(peer)
    if reader is None or writer is None:
        return

    try:
        while True:
            try:
                message = await _read_message(reader)

                if not message:
                    break

                msg_type = message.get("type")

                if msg_type == "SEND":
                    logger.info(f"[SEND] {message.get('src')}: {message.get('payload')}")
                    if message.get("require_ack"):
                        await send_ack(peer, message.get("msg_id"))

                elif msg_type == "ACK":
                    if peer.get('ack') and not peer.get('ack').done():
                        peer.get('ack').set_result(True)
                        logger.debug(f"An ACK was received from {peer_id}.")

                elif msg_type == "PING":
                    logger.debug(f"PING received from {peer_id}. Sending PONG...")
                    pong_sent = await _send_pong(peer,message.get('msg_id'),message.get('timestamp'))
                    if pong_sent:
                        logger.debug(f"PONG sent to {peer_id}.")
                    else:
                        logger.debug(f"Something went wrong when sending the PONG to {peer_id}.")

                elif msg_type == "PONG":
                    future = peer.get('pong')
                    if not future.done():
                        future.set_result(message)

                    logger.debug(f"PING sent to {peer_id}, PONG received.")
        
                elif msg_type == "PUB":
                    dst = message.get("dst")
                    target = '' if dst == '*' else f"@{dst}"
                    logger.info(f"[PUB{target}] {message.get('src')}: {message.get('payload')}")

                elif msg_type == "BYE":
                    logger.debug(f"Received BYE message from {message.get('src')}. Reason: {message.get('reason')}")
                    await send_bye_ok(peer, _local_peer_id(), message.get("msg_id"))
                    logger.info(f"Peer {peer_id} sent a BYE and disconnected.")
                    break

                elif msg_type == "BYE_OK":
                    future = peer.get('bye')
                    if not future.done():
                        future.set_result(message)

                    logger.debug(f"Received BYE_OK message from {message.get('src')} acknowledging BYE with msg_id: {message.get('msg_id')}")
                    logger.debug(f"Peer {peer_id} received the BYE and it is being disconnected...")
                    break

                else:
                    logger.debug(f"Received unknown message type: {msg_type}")

            except asyncio.CancelledError:
                logger.debug(f"Sending BYE to {peer_id}...")
                await send_bye(peer)
                continue

    except (asyncio.TimeoutError, ConnectionError, TimeoutError) as e:
        logger.debug(f"Connection to {peer_id} closed while handling incoming message: {e}")
        logger.debug(f"{traceback.format_exc()}")

    except asyncio.IncompleteReadError as e:
        logger.debug(f"Connection dropped before total read process. Partial data retrieved: {e.partial}")

    except Exception as e:
        logger.debug(f"Error handling incoming message of {peer_id}: {e}")

    finally:
        logger.debug("Exiting receive loop, closing stream writer.")
        logger.info(f"Closing connection to {peer_id}...")
        
        if writer is not None and not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        async with pt.peer_table_lock:
            pt.peer_table.degrade(peer)
            logger.debug(f"{pt.peer_table.get("conn")}")
