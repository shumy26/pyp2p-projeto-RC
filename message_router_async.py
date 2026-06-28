import asyncio
import json
import uuid
import logging
from datetime import datetime, timezone
import project_config as cfg


name = cfg.getConfig().get('name')
namespace = cfg.getConfig().get('namespace')
if cfg.getConfig().get("msg_size") is None or cfg.getConfig().get("msg_size") > 32768:
    max_msg_size = 32768
else:
    max_msg_size = cfg.getConfig().get("msg_size")
if cfg.getConfig().get("ttl") is None:
    ttl = 1
else:
    ttl = cfg.getConfig().get("ttl")
if cfg.getConfig().get("timeout") is None:
    timeout = 5
else:
    timeout = cfg.getConfig().get("timeout")


async def _send_data(writer: asyncio.StreamWriter, data):
    try:
        json_data = json.dumps(data).encode('utf-8') + b'\n'
        if len(json_data) > max_msg_size:
            logging.error(f"message size exceeds the limit of {max_msg_size} bytes.")
            return
        writer.write(json_data)
        await writer.drain()
    except Exception as e:
        if not writer.is_closing():
            writer.close()
        logging.error(f"Error sending data: {e}")


async def _read_message(reader: asyncio.StreamReader):
    try:
        data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout)
        return json.loads(data.decode('utf-8').strip())
    except asyncio.TimeoutError:
        logging.error("Timeout waiting for acknowledgment.")
        raise
    except asyncio.IncompleteReadError:
        raise ConnectionError("Connection closed while reading message.")
    except Exception as e:
        logging.error(f"Error reading message: {e}")
        raise


async def _wait_for_ack(reader: asyncio.StreamReader, message_id: str):
    try:
        ack_message = await _read_message(reader)
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


async def send_message(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, src: str, dest: str, message: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "SEND",
        "msg_id": message_id,
        "src": src,
        "dest": dest,
        "payload": message,
        "require_ack": True,
        "ttl": ttl
    }
    await _send_data(writer, data)
    if not await _wait_for_ack(reader, message_id):
        logging.error("Failed to receive acknowledgment for the sent message.")


async def send_ack(writer: asyncio.StreamWriter, message_id: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ack_data = {
        "type": "ACK",
        "msg_id": message_id,
        "timestamp": timestamp,
        "ttl": ttl
    }
    await _send_data(writer, ack_data)


async def publish_message(writer: asyncio.StreamWriter, src: str, dst: str, message: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "PUB",
        "msg_id": message_id,
        "src": src,
        "dst": dst,
        "payload": message,
        "require_ack": False,
        "ttl": ttl
    }
    await _send_data(writer, data)


async def send_bye(writer: asyncio.StreamWriter, src: str, dest: str, reason: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "BYE",
        "msg_id": message_id,
        "src": src,
        "dest": dest,
        "reason": reason,
        "ttl": ttl
    }
    await _send_data(writer, data)


async def send_bye_ok(writer: asyncio.StreamWriter, src: str, dest: str, msg_id_to_ack: str):
    data = {
        "type": "BYE_OK",
        "msg_id_to_ack": msg_id_to_ack,
        "src": src,
        "dest": dest,
        "ttl": ttl
    }
    await _send_data(writer, data)


async def handle_incoming_message(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, name: str):
    try:
        while True:
            message = await _read_message(reader)
            msg_type = message.get("type")
            if msg_type == "SEND":
                logging.info(f"Received SEND message from {message.get('src')}: {message.get('payload')}")
                if message.get("require_ack"):
                    await send_ack(writer, message.get("msg_id"))
            elif msg_type == "PUB":
                logging.info(f"Received PUB message from {message.get('src')}: {message.get('payload')}")
            elif msg_type == "BYE":
                logging.info(f"Received BYE message from {message.get('src')}: {message.get('reason')}")
                await send_bye_ok(writer, name + namespace, message.get("src"), message.get("msg_id"))
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
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()


def start_incoming_message_task(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, name: str):
    return asyncio.create_task(handle_incoming_message(reader, writer, name))
