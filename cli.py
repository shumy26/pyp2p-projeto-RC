import asyncio
import logging
import shlex
from dataclasses import dataclass

import peer_connection as pc
import peer_table as pt
import project_config as cfg
import rendevouz_connection as rdv
import state
import message_router as router
import p2p_client as p2p

logger = logging.getLogger("CLI")

HELP_TEXT = """
╔════════════════════════════════════════════════════════════════╗
║                  P2Pchat — Available commands                  ║
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

# Creates a custom class with dunder methods to organize and operate tasks running in the background ("Background Services").
@dataclass
class BackgroundService:
    task: asyncio.Task
    name: str

# Show the HELP text.
def _show_help():
    print(HELP_TEXT)

# Show the current ID that you are registered with.
def _show_currentID():
    logger.info(f"You've been registered as: {router._local_peer_id()}.")

# Handles asynchronous inputs.
async def _ainput(prompt: str = "p2p> "):
    return await asyncio.to_thread(input, prompt)

# Parses the namespace specified by /pub and /peers.
def _parse_namespace_token(token: str | None):
    if token is None:
        return None
    if token == "*":
        return "*"
    if token.startswith("#"):
        return token[1:]
    return None

# Returns the PeerID based on the current instance of "Peer".
def _peer_identity(peer) -> str:
    if not isinstance(peer, pt.Peer):
        return "Unknown"
    name = peer.get("name") or ""
    namespace = peer.get("namespace") or ""
    if name and namespace:
        return f"{name}@{namespace}"
    return name or namespace or "Unknown"

# Finds the peer in the connected, not connected or in both by its ID.
async def _find_peer_by_id(peer_id: str, state:str='both'):
    async with pt.peer_table_lock:
        connected = pt.peer_table.get("conn") or set()
        not_connected = pt.peer_table.get("nconn") or set()

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

# List the peers found in a DISCOVER, its pair IP:PORT, TTL, and when it expires.
def _print_peers(discover_dict):
    if not discover_dict or not isinstance(discover_dict, dict):
        print("No peer was found within the search.")
        return
    
    peers = discover_dict.get("peers") or []
    if not peers:
        return

    my_id = f"{cfg.getConfig().get("name")}@{cfg.getConfig().get("namespace")}"
    for peer in peers:
        peer_id = peer.get("peer_id") or f"{peer.get('name')}@{peer.get('namespace')}"
        if peer_id != my_id: 
            print(f"• {peer_id} | {peer.get('ip')}:{peer.get('port')} | ttl = {peer.get('ttl')}s | Expires in: {peer.get('expires_in')}")

# Prints all the currently connected peers, its direction, pair IP:PORT, and RTT.
def _print_connections():
    peers = list(pt.peer_table.get("conn") or set())
    if not peers:
        print("No active connection.")
        return

    for peer in peers:
        peer_id = _peer_identity(peer)
        print(
            f"• {peer_id} | {peer.get('direction')} | {peer.get('ip')}:{peer.get('port')} | RTT = {(str(peer.get('rtt')) + 'ms') if peer.get('rtt') != -1 else "Not yet measured"}"
        )

# Prints the RTT of the current connected peers and the total average
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

# Tries to reconciliate with STALE peers.
async def _reconnect():
    try:
        not_conn_peers = [asyncio.create_task(pc.handle_outbound_connection(not_conn_peer)) for not_conn_peer in pt.peer_table.get('nconn') if not_conn_peer.get('direction') == 'STALE']

        if not not_conn_peers:
            print("No one to reconciliate.")
            return False

    except:
        logger.error(f"An error occured when trying to reconciliate.")
        return False

# Send a message of type SEND to the peer of that ID.
async def _send_direct(peer_id: str, message: str):
    peer = await _find_peer_by_id(peer_id,'conn')
    if peer is None:
        print(f"Peer '{peer_id}' not connected.")
        return False

    try:
        ok = await router.send_message(peer, router._local_peer_id(), message)
        if ok:
            return True
        else:
            return False
    except NotImplementedError:
        return None

# Publish a message to all, or to the specified namespace.
async def _publish(namespace_token: str, message: str):
    namespace = _parse_namespace_token(namespace_token)
    if namespace is None:
        print("Use /pub * <message> or /pub #<namespace> <message>.")
        return None

    try:
        ok = await router.publish_message(namespace, router._local_peer_id(), message)
        if ok:
            target = "all peers" if namespace == "*" else f"namespace: #{namespace}"
            print(f"Message published to {target}.")
        else:
            print("No peer connected to receive the publish.")
    except NotImplementedError:
        return None

# Makes a DISCOVER and list all the peers or the peers in the specified namespace.
async def _list_peers(namespace_token: str | None):
    namespace = _parse_namespace_token(namespace_token)
    if namespace_token is not None and namespace is None:
        print("Use /peers, /peers * or /peers #<namespace>.")
        return

    discover_dict = await rdv.discover(namespace if namespace != "*" else None)
    
    await state.state(discover_dict)
    _print_peers(discover_dict)

# Makes the first REGISTER, DISCOVER and updates the state of the PeerTable.
async def _initial_bootstrap():
    register_response = await rdv.register()
    if isinstance(register_response, dict) and register_response.get("ip"):
        cfg.addConfig("ip", register_response["ip"])

    logger.info("You have been registered in the network.")

    discover_dict = await rdv.discover()
    await state.state(discover_dict)

# Starts the tasks that handles the continuous REGISTER and DISCOVER/STATE loop, and the task to wait for a second to initiate connections with peers OUTBOUND and INBOUND.
async def _start_background_services():
    services = [
        BackgroundService(asyncio.create_task(p2p.loop_register()), "register-loop"),
        BackgroundService(asyncio.create_task(p2p.loop_discover()), "discover-loop"),
        BackgroundService(asyncio.create_task(p2p._wait_for_state()), "connect-loop")
    ]
    return services

# Runs over all the tasks in the current event loop and cancels each one of them except for this one.
async def _shutdown_background_services(services):
    curr_task = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks() if t is not curr_task]
    for t in tasks:
        if not t.cancelled():
            t.cancel()
    
    return True

# Main function of CLI. Verify if it exist a config file (if not, create a standard one); validate it; REGISTER in the rendevouz, make a DISCOVER, updates the PeerTable once, creating a file for it; starts the process of trying to connect to other peers; and awaits for commands.

async def repl():
    cfg.write_config()
    validated = cfg.validate_config()

    if not validated:
        return None

    await _initial_bootstrap()
    services = await _start_background_services()

    # Show HELP text and the current ID that your peer is registered as.

    print("REPL started. Type /help to see the available commands.")
    _show_help()
    _show_currentID()

    try:
        while True:
            raw = (await _ainput("\np2p> ")).strip()
            if not raw:
                continue
            
            # Show help message
            if raw == "/help":
                _show_help()
                continue
            
            # List Peers available in the rendevouz.
            if raw.startswith("/peers"):
                parts = shlex.split(raw)
                token = parts[1] if len(parts) > 1 else None
                await _list_peers(token)
                continue
            
            # Message a peer based on its ID.
            if raw.startswith("/msg "):
                parts = shlex.split(raw, posix=True)
                if len(parts) < 3:
                    print("Use: /msg <peer_id> <message>")
                    continue
                peer_id = parts[1]
                message = raw.split(None, 2)[2]
                try:
                    await _send_direct(peer_id, message)
                    continue
                except OSError:
                    logger.info("The peer already disconnected.")
                    continue
            
            # Publish to all or to a namespace.
            if raw.startswith("/pub "):
                parts = shlex.split(raw, posix=True)
                if len(parts) < 3:
                    print("Use: /pub * <message> or /pub #<namespace> <message>")
                    continue
                await _publish(parts[1], raw.split(None, 2)[2])
                continue
            
            # Print current connected peers.
            if raw == "/conn":
                _print_connections()
                continue
            
            # Print the RTT for each peer and the total average.
            if raw == "/rtt":
                _print_rtt()
                continue
            
            # Tries to reconnect with peers not connected and are STALE.
            if raw == "/reconnect":
                parts = shlex.split(raw)
                await _reconnect()
                continue
            
            # Changes LOG level of the StreamHandler. The RotateFileHandler is always on DEBUG mode 
            # as it is important to have this log somewhere. The root logger is also always on DEBUG mode, 
            # to guarantee that this always works.
            if raw.startswith("/log "):
                parts = shlex.split(raw)
                if len(parts) < 2:
                    print("Use: /log <DEBUG|INFO|WARNING|ERROR|CRITICAL>")
                    continue

                level_name = parts[1].upper()
                level = getattr(logging, level_name, None)
                if not isinstance(level, int):
                    print("Invalid level. Use DEBUG, INFO, WARNING, ERROR ou CRITICAL.")
                    continue

                logging.getLogger().setLevel(level)
                for handler in logging.getLogger().handlers:
                    if isinstance(handler, logging.StreamHandler):
                        handler.setLevel(level)
                print(f"Log level adjusted to: {level_name}.")
                continue
            
            # Quits the loop and the REPL
            if raw == "/quit":
                break

            print("Unknown command. Use /help.")

    # If KeyboardInterrupt is raised, it acts the same as /quit.
    except KeyboardInterrupt:
        pass
    
    # Waits for all the background tasks to be cancelled and UNREGISTER if the peer is still registered
    # in the rendevouz.
    finally:
        await _shutdown_background_services(services)
        try:
            await rdv.unregister()
        except Exception:
            pass

# The function 'main()' just runs the repl() function, starting the event loop on the current thread.
def main():
    asyncio.run(repl())

if __name__ == "__main__":
    main()
