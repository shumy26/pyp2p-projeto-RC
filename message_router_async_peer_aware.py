import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import project_config as cfg
import peer_table as pt


def _local_peer_id() -> str:
    # Build the current node identity in the form name@namespace.
    name = cfg.getConfig().get('name', '')
    namespace = cfg.getConfig().get('namespace', '')
    return f"{name}@{namespace}" if name and namespace else (name or namespace or "unknown")

def _get_ttl() -> str:
    if cfg.getConfig().get("ttl") is None:
        return ttl = 1
    return ttl = cfg.getConfig().get("ttl")

def _peer_id(peer: pt.Peer | None) -> str | None:
    # Convert a Peer object to its network identifier for use in message headers.
    if not isinstance(peer, pt.Peer):
        return None
    peer_name = peer.get("name")
    peer_namespace = peer.get("namespace")
    if peer_name and peer_namespace:
        return f"{peer_name}@{peer_namespace}"
    return peer_name or peer_namespace


def _get_peer_streams(peer: pt.Peer | None):
    # Resolve the active reader/writer pair from the connected peer instance.
    if not isinstance(peer, pt.Peer):
        logging.error("A valid Peer instance is required to route a message.")
        return None, None

    conn = peer.get("conn")
    if isinstance(conn, tuple) and len(conn) == 2:
        return conn

    logging.warning("Peer %s is not connected.", _peer_id(peer) or peer)
    return None, None


async def _send_data(writer: asyncio.StreamWriter, data):
    # Serialize the payload and send it over the peer stream as a newline-delimited JSON message.
    json_data = json.dumps(data).encode('utf-8') + b'\n'
    if cfg.getConfig().get("msg_size") is None or cfg.getConfig().get("msg_size") > 32768:
    max_msg_size = 32768
    else:
    max_msg_size = cfg.getConfig().get("msg_size")
    if len(json_data) > max_msg_size:
        logging.error(f"message size exceeds the limit of {max_msg_size} bytes.")
        return False
    writer.write(json_data)
    await writer.drain()
    return True


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
        logging.error(f"Error reading message: {e}")
        raise


async def _send_to_peer(peer: pt.Peer, data: dict):
    # Send a prepared message payload to the peer using the stored connection.

    try:
        _, writer = _get_peer_streams(peer)
        if writer is None:
            return False
        return await _send_data(writer, data)
    except OSError:
        pt.peer_table.get('conn').discard(peer)
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        return False

async def _wait_for_ack(peer: pt.Peer, message_id: str):
    # Wait for the peer to acknowledge a previously sent message.
    reader, _ = _get_peer_streams(peer)
    if reader is None:
        return False
    try:
        async with peer.get('lock'):
            if cfg.getConfig().get("timeout") is None:
                timeout = 5
            else:
            timeout = cfg.getConfig().get("timeout")
            ack_message = asyncio.wait_for(_read_message(reader), timeout)
            if ack_message.get("type") == "ACK" and ack_message.get("msg_id") == message_id:
                return True
            logging.error(f"Received invalid acknowledgment: {ack_message}")
            return False
    except (asyncio.TimeoutError, ConnectionError) as e:
        logging.error(f"Error waiting for acknowledgment: {e}")
        return False
    except Exception as e:
        logging.error(f"Error waiting for acknowledgment: {e}")
        return False


async def send_message(peer: pt.Peer, src: str, message: str):
    # Send a point-to-point message and expect an ACK from the peer.
    if not isinstance(peer, pt.Peer):
        logging.error("A valid Peer instance is required to send a message.")
        return False

    message_id = str(uuid.uuid4())
    data = {
        "type": "SEND",
        "msg_id": message_id,
        "src": src or _local_peer_id(),
        "dest": _peer_id(peer),
        "payload": message,
        "require_ack": True,
        "ttl": _get_ttl(),
    }
    if not await _send_to_peer(peer, data):
        return False
    if not await _wait_for_ack(peer, message_id):
        logging.error("Failed to receive acknowledgment for the sent message.")
        return False
    return True


async def send_ack(peer: pt.Peer, message_id: str):
    # Reply to a message with an ACK carrying the original message identifier.
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ack_data = {
        "type": "ACK",
        "msg_id": message_id,
        "timestamp": timestamp,
        "ttl": _get_ttl(),
    }
    return await _send_to_peer(peer, ack_data)


def _get_publish_targets(namespace: str):
    # Select all connected peers in the requested namespace, or every connected peer for wildcard broadcasts.
    connected_peers = list(pt.peer_table.get("conn") or set())
    if namespace == "*":
        return connected_peers
    return [peer for peer in connected_peers if peer.get("namespace") == namespace]


async def publish_message(namespace: str, src: str, message: str):
    # Publish a message to every connected peer in the requested namespace, or broadcast to all connected peers when namespace is *.
    targets = _get_publish_targets(namespace or "")
    if not targets:
        logging.warning("No connected peers found for publish namespace %s.", namespace or "*")
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
        await _send_to_peer(peer, data)

    return True


async def send_bye(peer: pt.Peer, src: str, reason: str):
    # Notify the peer that the local side is closing the connection.
    if not isinstance(peer, pt.Peer):
        logging.error("A valid Peer instance is required to send a BYE message.")
        return False

    message_id = str(uuid.uuid4())
    data = {
        "type": "BYE",
        "msg_id": message_id,
        "src": src or _local_peer_id(),
        "dest": _peer_id(peer),
        "reason": reason,
        "ttl": _get_ttl(),
    }
    return await _send_to_peer(peer, data)


async def send_bye_ok(peer: pt.Peer, src: str, msg_id_to_ack: str):
    # Acknowledge a BYE request from the peer.
    if not isinstance(peer, pt.Peer):
        logging.error("A valid Peer instance is required to acknowledge a BYE.")
        return False

    data = {
        "type": "BYE_OK",
        "msg_id_to_ack": msg_id_to_ack,
        "src": src or _local_peer_id(),
        "dest": _peer_id(peer),
        "ttl": _get_ttl(),
    }
    return await _send_to_peer(peer, data)


async def handle_incoming_message(peer: pt.Peer):
    # Process incoming messages from the peer until the stream closes.
    reader, writer = _get_peer_streams(peer)
    if reader is None or writer is None:
        return

    try:
        while True:
            message = await _read_message(reader)
            msg_type = message.get("type")
            if msg_type == "SEND":
                logging.info(f"Received SEND message from {message.get('src')}: {message.get('payload')}")
                if message.get("require_ack"):
                    await send_ack(peer, message.get("msg_id"))
            elif msg_type == "PUB":
                logging.info(f"Received PUB message from {message.get('src')}: {message.get('payload')}")
            elif msg_type == "BYE":
                logging.info(f"Received BYE message from {message.get('src')}: {message.get('reason')}")
                await send_bye_ok(peer, _local_peer_id(), message.get("msg_id"))
            elif msg_type == "BYE_OK":
                logging.info(f"Received BYE_OK message from {message.get('src')} acknowledging BYE with msg_id: {message.get('msg_id_to_ack')}")
            else:
                logging.warning(f"Received unknown message type: {msg_type}")
    except (asyncio.TimeoutError, ConnectionError) as e:
        logging.info(f"Connection closed while handling incoming message: {e}")
    except Exception as e:
        logging.error(f"Error handling incoming message: {e}")
    finally:
        logging.info("Exiting receive loop, closing stream writer.")
        if writer is not None and not writer.is_closing():
            writer.close()
            await writer.wait_closed()


def start_incoming_message_task(peer: pt.Peer):
    # Start the inbound message loop in the background for a given peer.
    return asyncio.create_task(handle_incoming_message(peer))
