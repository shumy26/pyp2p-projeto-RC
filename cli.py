import asyncio
import logging
import shlex
from dataclasses import dataclass

import peer_connection as pc
import peer_table as pt
import project_config as cfg
import project_log as plog
import rendevouz_connection as rdv
import state
import message_router as router
import teste_parallelism2 as parallel
import p2p_client as p2p

logger = logging.getLogger("CLI")

HELP_TEXT = """
╔════════════════════════════════════════════════════════════════╗
║                P2Pchat — Comandos disponíveis                  ║
╠════════════════════════════════════════════════════════════════╣
║  /peers [* | #namespace]      Discover and List Peers          ║
║  /msg <peer_id> <mensagem>    Send a direct message to a Peer  ║
║  /pub * <mensagem>            Global Broadcast                 ║
║  /pub #<namespace> <mensagem> Publish to Namespace             ║
║  /conn                        List active connections          ║
║  /rtt                         Average RTT per Peer             ║
║  /reconnect                   Force reconciliation of Peers    ║
║  /log <DEBUG|INFO|...>        Log level adjust                 ║
║  /help                        Show this Help Message           ║
║  /quit                        Quit the application             ║
╚════════════════════════════════════════════════════════════════╝"""


@dataclass
class BackgroundService:
    task: asyncio.Task
    name: str

def _configure_logging():
    plog.create_logs()

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

def _show_help():
    print(HELP_TEXT)


async def _ainput(prompt: str = "p2p> "):
    return await asyncio.to_thread(input, prompt)


def _parse_namespace_token(token: str | None):
    if token is None:
        return None
    if token == "*":
        return "*"
    if token.startswith("#"):
        return token[1:]
    return None


def _peer_identity(peer) -> str:
    if not isinstance(peer, pt.Peer):
        return "Unknown"
    name = peer.get("name") or ""
    namespace = peer.get("namespace") or ""
    if name and namespace:
        return f"{name}@{namespace}"
    return name or namespace or "Unknown"


async def _find_peer_by_id(peer_id: str, state:str='both'):
    async with pt.peer_table_lock:
        connected = list(pt.peer_table.get("conn") or set())
        not_connected = list(pt.peer_table.get("nconn") or set())

    match state:
        case 'both':
            for peer in connected + not_connected:
                if _peer_identity(peer) == peer_id:
                    return peer
            return None
        case 'conn':
            for peer in connected:
                if _peer_identity(peer) == peer_id:
                    return peer
            return None
        case 'nconn':
            for peer in not_connected:
                if _peer_identity(peer) == peer_id:
                    return peer
            return None


async def _ensure_connected(peer: pt.Peer):
    if not isinstance(peer, pt.Peer):
        return False

    if peer in (pt.peer_table.get("conn") or set()):
        return True

    return False


def _print_peers(discover_dict):
    if not discover_dict or not isinstance(discover_dict, dict):
        print("No peer was found within the search.")
        return

    peers = discover_dict.get("peers") or []
    if not peers:
        return

    for peer in peers:
        peer_id = peer.get("peer_id") or f"{peer.get('name')}@{peer.get('namespace')}"
        print(f"• {peer_id} | {peer.get('ip')}:{peer.get('port')} | ttl = {peer.get('ttl')}")


def _print_connections():
    peers = list(pt.peer_table.get("conn") or set())
    if not peers:
        print("No active connection.")
        return

    for peer in peers:
        peer_id = _peer_identity(peer)
        print(
            f"• {peer_id} | {peer.get('direction')} | {peer.get('ip')}:{peer.get('port')} | RTT = {(peer.get('rtt') + 'ms') if peer.get('rtt') != -1 else "Not yet measured"}"
        )


def _print_rtt():
    peers = list(pt.peer_table.get("conn") or set())
    valid_rtts = []

    if not peers:
        print("No active connection.")
        return

    for peer in peers:
        rtt = peer.get("rtt")
        peer_id = _peer_identity(peer)
        if isinstance(rtt, (int, float)) and rtt >= 0:
            valid_rtts.append(rtt)
            print(f"• {peer_id}: {rtt:.2f} ms")
        else:
            print(f"• {peer_id}: No RTT acquired")

    if valid_rtts:
        avg = sum(valid_rtts) / len(valid_rtts)
        print(f"Average RTT: {avg:.2f} ms")


async def _reconnect():
    try:
        not_conn_peers = [pc.handle_outbound_connection(not_conn_peer) for not_conn_peer in pt.peer_table.get('nconn') if not_conn_peer.get('direction') != 'STALE']

        if not not_conn_peers:
            logger.debug("No one to reconciliate.")

        connected = await asyncio.gather(*not_conn_peers)

    except:
        logger.error(f"An error occured when trying to reconciliate.")
        return False


async def _send_direct(peer_id: str, message: str):
    peer = await _find_peer_by_id(peer_id,'conn')
    if peer is None:
        print(f"Peer '{peer_id}' not connected.")
        return False

    ok = await router.send_message(peer, router._local_peer_id(), message)
    if ok:
        return True
    else:
        return False


async def _publish(namespace_token: str, message: str):
    namespace = _parse_namespace_token(namespace_token)
    if namespace is None:
        print("Use /pub * <mensagem> or /pub #<namespace> <mensagem>.")
        return

    ok = await router.publish_message(namespace, router._local_peer_id(), message)
    if ok:
        target = "all peers" if namespace == "*" else f"namespace: #{namespace}"
        print(f"Message published to {target}.")
    else:
        print("No peer connected to receive the publish.")


async def _list_peers(namespace_token: str | None):
    namespace = _parse_namespace_token(namespace_token)
    if namespace_token is not None and namespace is None:
        print("Use /peers, /peers * or /peers #<namespace>.")
        return

    discover_dict = await rdv.discover(namespace if namespace != "*" else None)
    state.state(discover_dict)
    _print_peers(discover_dict)


async def _initial_bootstrap():
    cfg.write_config()
    register_response = await rdv.register()
    if isinstance(register_response, dict) and register_response.get("ip"):
        cfg.addConfig("ip", register_response["ip"])

    discover_dict = await rdv.discover()
    state.state(discover_dict)


async def _start_background_services():
    services = [
        BackgroundService(asyncio.create_task(p2p.loop_register()), "register-loop"),
        BackgroundService(asyncio.create_task(p2p.loop_discover()), "discover-loop"),
        BackgroundService(asyncio.create_task(p2p._wait_for_state()), "state-reconcile"),
        BackgroundService(asyncio.create_task(p2p._wait_and_ping()), "ping-loop"),
    ]
    return services


async def _shutdown_background_services(services):
    for service in services:
        service.task.cancel()

    await asyncio.gather(*(service.task for service in services), return_exceptions=True)

    try:
        await p2p.terminate_peers()
    except Exception:
        pass

    try:
        await rdv.unregister()
    except Exception:
        pass


async def repl():
    _configure_logging()
    await _initial_bootstrap()
    services = await _start_background_services()
    # start auto-connect loop to attempt connections to all discovered peers

    print("REPL iniciado. Digite /help para ver os comandos.")

    try:
        while True:
            raw = (await _ainput("p2p> ")).strip()
            if not raw:
                continue

            if raw == "/help":
                _show_help()
                continue

            if raw.startswith("/peers"):
                parts = shlex.split(raw)
                token = parts[1] if len(parts) > 1 else None
                await _list_peers(token)
                continue

            if raw.startswith("/msg "):
                parts = shlex.split(raw, posix=True)
                if len(parts) < 3:
                    print("Uso: /msg <peer_id> <mensagem>")
                    continue
                peer_id = parts[1]
                message = raw.split(None, 2)[2]
                await _send_direct(peer_id, message)
                continue

            if raw.startswith("/pub "):
                parts = shlex.split(raw, posix=True)
                if len(parts) < 3:
                    print("Uso: /pub * <mensagem> ou /pub #<namespace> <mensagem>")
                    continue
                await _publish(parts[1], raw.split(None, 2)[2])
                continue

            if raw == "/conn":
                _print_connections()
                continue

            if raw == "/rtt":
                _print_rtt()
                continue

            if raw == "/reconnect":
                parts = shlex.split(raw)
                await _reconnect()
                continue

            if raw.startswith("/log "):
                parts = shlex.split(raw)
                if len(parts) < 2:
                    print("Uso: /log <DEBUG|INFO|WARNING|ERROR|CRITICAL>")
                    continue

                level_name = parts[1].upper()
                level = getattr(logging, level_name, None)
                if not isinstance(level, int):
                    print("Invalid level. Use DEBUG, INFO, WARNING, ERROR ou CRITICAL.")
                    continue

                logging.getLogger().setLevel(level)
                for handler in logging.getLogger().handlers:
                    handler.setLevel(level)
                print(f"Log level adjusted to: {level_name}.")
                continue

            if raw == "/quit":
                break

            print("Comando desconhecido. Use /help.")

    finally:
        await _shutdown_background_services(services)


def main():
    asyncio.run(repl())


if __name__ == "__main__":
    main()
