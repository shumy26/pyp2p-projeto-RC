import asyncio
import json
import logging

import keep_alive
import project_config as cfg


logger = logging.getLogger(__name__)


_active_connections = {}
_server = None


def _local_peer_id():
    config_dict = cfg.getConfig()
    return f"{config_dict.get('name')}@{config_dict.get('namespace')}"


def build_hello(peer_id, version="1.0", features=None, ttl=1):
    if features is None:
        features = []
    return {
        "type": "HELLO",
        "peer_id": str(peer_id),
        "version": str(version),
        "features": list(features),
        "ttl": ttl,
    }


def build_hello_ok(peer_id, version="1.0", features=None, ttl=1):
    if features is None:
        features = []
    return {
        "type": "HELLO_OK",
        "peer_id": str(peer_id),
        "version": str(version),
        "features": list(features),
        "ttl": ttl,
    }


def _peer_id_from_discover_peer(peer):
    if isinstance(peer, dict) and peer.get("peer_id"):
        return peer.get("peer_id")

    name = peer.get("name") if isinstance(peer, dict) else None
    namespace = peer.get("namespace") if isinstance(peer, dict) else None
    if name and namespace:
        return f"{name}@{namespace}"

    return None


def _peer_host_port(peer):
    host = None
    port = None

    if not isinstance(peer, dict):
        return None, None

    for host_key in ("host", "ip", "address", "hostname"):
        if isinstance(peer.get(host_key), str) and peer.get(host_key):
            host = peer.get(host_key)
            break

    if isinstance(peer.get("port"), int):
        port = peer.get("port")

    return host, port


def _keep_connection(peer_id, reader, writer):
    old = _active_connections.get(peer_id)
    if old:
        _, old_writer = old
        if not old_writer.is_closing():
            old_writer.close()

    _active_connections[peer_id] = (reader, writer)


async def _handle_incoming_traffic(peer_id, reader, writer):
    try:
        while not reader.at_eof():
            message = await reader.readline()
            if not message:
                break

            try:
                payload = json.loads(message.decode("utf-8"))
                handled = await keep_alive.handle_keep_alive_message(payload, writer, peer_id=peer_id)
                if not handled:
                    logger.info("Mensagem recebida de %s: %s", peer_id, payload)
            except json.JSONDecodeError:
                logger.warning("Mensagem invalida recebida de %s", peer_id)
    except asyncio.CancelledError:
        return None
    finally:
        conn = _active_connections.get(peer_id)
        if conn and conn[1] is writer:
            _active_connections.pop(peer_id, None)

        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    return None


async def handle_hello(reader, writer):
    config_dict = cfg.getConfig()
    local_features = config_dict.get("features", [])
    local_ttl = config_dict.get("keep_alive_ttl", 1)

    try:
        raw = await reader.readline()
        if not raw:
            writer.close()
            await writer.wait_closed()
            return None

        request = json.loads(raw.decode("utf-8"))
        if request.get("type") != "HELLO":
            writer.close()
            await writer.wait_closed()
            return None

        remote_peer_id = request.get("peer_id")
        response = build_hello_ok(
            peer_id=_local_peer_id(),
            version="1.0",
            features=local_features,
            ttl=local_ttl,
        )

        writer.write((json.dumps(response) + "\n").encode())
        await writer.drain()

        logger.info("HELLO recebido de %s e HELLO_OK enviado.", remote_peer_id)

        _keep_connection(remote_peer_id, reader, writer)
        await _handle_incoming_traffic(remote_peer_id, reader, writer)
        return None

    except json.JSONDecodeError:
        logger.warning("Falha ao decodificar HELLO recebido.")
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        return None
    except asyncio.CancelledError:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        return None


async def start_hello_server():
    global _server

    config_dict = cfg.getConfig()
    listen_port = config_dict.get("port")

    if not isinstance(listen_port, int):
        logger.warning("Porta local invalida para servidor HELLO.")
        return None

    try:
        _server = await asyncio.start_server(handle_hello, host="0.0.0.0", port=listen_port)
        logger.info("Servidor HELLO ouvindo na porta %s", listen_port)
        async with _server:
            await _server.serve_forever()
    except OSError as exc:
        logger.error("Nao foi possivel iniciar servidor HELLO: %s", exc)
        return None
    except asyncio.CancelledError:
        return None


async def hello_peer(host, port, ttl=1, timeout=5):
    config_dict = cfg.getConfig()
    local_features = config_dict.get("features", [])
    local_peer_id = _local_peer_id()

    if not isinstance(host, str) or not isinstance(port, int):
        return None

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)

        hello_message = build_hello(
            peer_id=local_peer_id,
            version="1.0",
            features=local_features,
            ttl=ttl,
        )

        writer.write((json.dumps(hello_message) + "\n").encode())
        await writer.drain()

        raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not raw:
            writer.close()
            await writer.wait_closed()
            return None

        response = json.loads(raw.decode("utf-8"))
        if response.get("type") != "HELLO_OK":
            writer.close()
            await writer.wait_closed()
            return None

        remote_peer_id = response.get("peer_id")
        logger.info("HELLO_OK recebido de %s (%s:%s)", remote_peer_id, host, port)

        _keep_connection(remote_peer_id, reader, writer)
        asyncio.create_task(_handle_incoming_traffic(remote_peer_id, reader, writer))
        return response

    except (TimeoutError, ConnectionRefusedError, OSError):
        return None
    except json.JSONDecodeError:
        return None


async def hello_peers_from_discover(discover_response):
    if not isinstance(discover_response, dict):
        return []

    peers = discover_response.get("peers")
    if not isinstance(peers, list):
        return []

    local_peer_id = _local_peer_id()
    ttl = cfg.getConfig().get("keep_alive_ttl", 1)
    connected = []

    for peer in peers:
        peer_id = _peer_id_from_discover_peer(peer)
        if not peer_id or peer_id == local_peer_id or peer_id in _active_connections:
            continue

        host, port = _peer_host_port(peer)
        if host is None or port is None:
            continue

        response = await hello_peer(host, port, ttl=ttl)
        if response is not None:
            connected.append(peer_id)

    return connected


async def shutdown_hello():
    global _server

    for peer_id, (_, writer) in list(_active_connections.items()):
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except OSError:
            pass
        finally:
            _active_connections.pop(peer_id, None)

    if _server is not None:
        _server.close()
        await _server.wait_closed()
        _server = None


def list_active_peers():
    return list(_active_connections.keys())

