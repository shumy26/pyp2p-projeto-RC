import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone

import project_config as cfg


logger = logging.getLogger(__name__)


def _timestamp_now():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


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
        logger.info("PING recebido de %s; PONG enviado.", peer_id)
    else:
        logger.info("PING recebido; PONG enviado.")

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
                    logger.info("Mensagem nao relacionada a keep-alive: %s", payload)
            except json.JSONDecodeError:
                logger.warning("Mensagem invalida recebida no keep-alive.")
    except asyncio.CancelledError:
        return None
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    return None


async def ping_once():
    config_dict = cfg.getConfig()
    conn_ip = config_dict.get("rdv_name")
    conn_port = config_dict.get("rdv_port")
    ttl = config_dict.get("keep_alive_ttl", 1)

    if not isinstance(conn_ip, str) or not isinstance(conn_port, int):
        logger.error("Invalid rendezvous connection settings for keep-alive.")
        return None

    try:
        reader, writer = await asyncio.open_connection(conn_ip, conn_port)
        ping_dict = build_ping(ttl=ttl)
        request_string = json.dumps(ping_dict) + '\n'

        print(f"Enviando PING: {json.dumps(ping_dict, ensure_ascii=False)}")

        started_at = time.perf_counter()
        writer.write(request_string.encode())
        await writer.drain()

        response_string = await reader.read(32768)
        elapsed_ms = (time.perf_counter() - started_at) * 1000

        if not response_string:
            logger.warning("PING sent but no PONG was received.")
            writer.close()
            await writer.wait_closed()
            return None

        response_dict = json.loads(response_string.decode('utf-8'))

        if response_dict.get("type") != "PONG":
            logger.warning("Unexpected keep-alive response: %s", response_dict)
        elif response_dict.get("msg_id") != ping_dict["msg_id"]:
            logger.warning(
                "PONG msg_id mismatch: expected %s, got %s",
                ping_dict["msg_id"],
                response_dict.get("msg_id"),
            )
        else:
            logger.info("Keep-alive RTT: %.2f ms", elapsed_ms)

        writer.close()
        await writer.wait_closed()
        return response_dict

    except ConnectionRefusedError:
        logger.error("The server refused the keep-alive connection.")
        return None
    except TimeoutError:
        logger.error("Could not connect to server due to timed out attempt.")
        return None
    except OSError:
        logger.error("There was a network issue and it was not possible to connect to the server.")
        return None
    except (InterruptedError, KeyboardInterrupt):
        logger.warning("The keep-alive loop was interrupted by a signal.")
        return None
    except json.JSONDecodeError:
        logger.error("The server returned an invalid JSON response for keep-alive.")
        return None

