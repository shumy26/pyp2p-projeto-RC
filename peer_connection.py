import asyncio
import json
import logging
# import pathlib
# import socket
# import threading
from datetime import datetime, timezone
import uuid

import project_config as cfg
from peer_table import *

logger = logging.getLogger('PeerConnector')

async def terminate_peers():
    for peer in peer_table.get('conn'):
        disconnected = await disconnect_peer(peer, *peer.get('conn'), peer_table=peer_table)
        if not disconnected:
            try:
                writer = peer.get('conn')[1]
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
            except:
                continue
    
    peer_table.get('conn').clear()
    peer_table.get('nconn').clear()

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

def build_bye(src_id=None, dst_id=None,msg_id=None,reason="Closing session...",ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "BYE",
        "msg_id": (msg_id or uuid.uuid4()),
        "src": src_id,
        "dst": dst_id,
        "reason": reason,
        "ttl": ttl//ttl
}

def build_bye_ok(src_id=None, dst_id=None,msg_id=None,ttl=cfg.getConfig().get('ttl',1)):
    return {
        "type": "BYE_OK",
        "msg_id": (msg_id or uuid.uuid4()),
        "src": src_id,
        "dst": dst_id,
        "ttl": ttl//ttl
            }

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

async def peer_connection(peer:Peer):
    if not isinstance(peer,Peer):
        logger.debug("It seems a Peer object was not passed to this function.")
        return False

    my_id = _local_peer_id()
    peer_id = _peer_id_from_peer_table(peer)
            
    try:
        reader, writer = await asyncio.open_connection(peer.get('ip'), peer.get('port'))

        hello_dict = build_hello(my_id)
        hello_request = json.dumps(hello_dict)
        hello_request = hello_request.encode()

        writer.write(hello_request)
        await writer.drain()

        # Before: While True loop; Now: Each task will be handled separately.

        response = await reader.read()
        response_dict = json.loads(response)

        match response_dict.get('type'):

            case 'HELLO_OK':
                peer.set('direction','Outbound')
                peer.reset_attempts()
                peer.set('conn',(reader,writer))
                async with peer_table_lock:
                    peer_table.update(peer)
                logger.info(f"Outbound Connection Established: {peer_id}.")
                return True

            case _:
                logger.error(f"The peer {peer_id} answered HELLO with an invalid response.")
                peer.decrease_attempts()
                return False

    # Raised when the peer refuses the connection
    except ConnectionRefusedError:
        logger.error(f"The peer {peer_id} refused to connect.")
        peer.decrease_attempts()
        return False

    # Raised when the socket could not connect to the IP address in a given time
    except TimeoutError:
        logger.error(f"Connection to {peer_id} timed out.")
        peer.decrease_attempts()
        return False

    # Raised when there are possibly no routes to that server
    except OSError:
        logger.error(f"There was a network issue and it was not possible to connect to {peer_id} or the socket is already instantiated.")
        peer.decrease_attempts()
        return False

    except json.JSONDecodeError:
        logger.error("The peer answered the request with an invalid JSON object.")
        peer.decrease_attempts()
        return False

    except KeyError:
        logger.debug(f"Peer '{peer_id}' does not exist in PeerTable.")
        return False

    # Raised when the user or the program interrupts the execution with a signal
    except (InterruptedError, KeyboardInterrupt):
        logger.error("The program was interrupted by a signal (like Ctrl+C).")
        try:
            if not writer.is_closing():
                writer.close()
                logger.info(f"Closing connection to {peer_id}...")
                await writer.wait_closed()
                return None
        except NameError:
            peer_table.get('conn').remove(peer)
            return None
        except KeyError:
            return None


# This function handles attempts to connect to an outside host
async def handle_outbound_connection(peer):
    peer_id = _peer_id_from_peer_table(peer)
    connected = False
    # max_reconnect_attempts
    max_n = peer.get('attempts')
    # Initial number to exponential backoff (2**n)
    n = 1
    try:
        # Try to connect to peer for max_reconnect_attempts
        while n < max_n:
            connected = await peer_connection(peer)
            # If the peer is not connected, try again
            if not connected:
                await asyncio.sleep(2**n)
                n += 1
                continue
            # If it is connected or n == max_n, break from the loop
            break

        if peer.get('attempts') == 0:
            logger.info(f"Multiple attempts were made to {peer_id}! Connection failed.")
            peer.set('direction','STALE')

    except asyncio.CancelledError:
        logger.debug(f"The task handling the connection with {peer_id} was cancelled.")
        raise asyncio.CancelledError

    except:
        logger.debug(f"An error occured when trying to connect to {peer_id}.")


# This function handles attempts of a peer trying to connect to this device

async def handle_inbound_connection(reader:asyncio.StreamReader, writer:asyncio.StreamWriter):
    my_id = _local_peer_id()

    try:
        peer_ip, peer_port = writer.get_extra_info('peername')

        current_peer = Peer()
        async with peer_table_lock:
            for peer in peer_table.get('nconn'):
                if (peer.get('ip') == peer_ip and peer.get('port') == peer_port) and (peer.get('direction') != 'STALE'):
                    peer.set('direction','Inbound')
                    peer.set('attempts',peer.get('attempts'))
                    peer.set('conn',(reader,writer))
                    peer_table.update(peer)
                    current_peer = peer
                    break

        if current_peer.get('name') == '':
            raise ConnectionRefusedError

        peer_id = _peer_id_from_peer_table(current_peer)

        raw = await reader.read(32768)
            
        request = raw.decode(encoding='utf-8').strip('\n')
        request_dict = json.loads(request)

        match request_dict.get('type'):

            case "HELLO":
                logger.debug(f"HELLO received from: {peer_id}.")
                response_dict = build_hello_ok(my_id,features=cfg.getConfig().get('features'))
                response = json.dumps(response_dict) + '\n'

                writer.write(response.encode())
                await writer.drain()
                logger.debug(f"HELLO_OK sent to: {peer_id}.")
                logger.info(f"Inbound Connection Established: {peer_id}.")
                await asyncio.sleep(1)

            case _:
                logger.debug(f"Peer:{peer_id} that connected to you has sent an invalid request type.")
            
    except json.JSONDecodeError:
        logger.debug("The peer that tried to connect to you has sent an invalid request - It was not a valid JSON object.")

    except ConnectionRefusedError:
        logger.warning(f"A host that is not currently registered in the network tried to connect. Host: {writer.get_extra_info('peername')}.")

    except asyncio.CancelledError:
        logger.debug(f"The task handling the connection with {peer_id} was cancelled and the socket is closing...")

    except ConnectionError:
        logger.info(f"The connection with {peer_id} was terminated by the peer.")
    
    except NameError:
        pass


async def start_inbound():
    '''Starts the server loop in order to connect to [Inbound] peers'''
    host = 'localhost'
    port = cfg.getConfig().get('port')
    
    if isinstance(port,NoneType):
        logger.debug("The port is not configured. Change it!")
        raise asyncio.CancelledError

    try:
        server = await asyncio.start_server(handle_inbound_connection,host,port)
        async with server:
            await server.serve_forever()

    except asyncio.CancelledError:
        logger.debug(f"Task 'start_inbound' closed.")


# Initiates the attempts to connect to peers externally
async def start_outbound():
    '''Starts the loop in order to connect to [Outbound] peers'''
    try:
        discover_interval = cfg.getConfig().get('discover_interval')
        peers_not_connected = set()
        while True:

            async with peer_table_lock:
                peers_not_connected = peer_table.get('nconn')

            while len(peers_not_connected) == 0:
                async with peer_table_lock:
                    peers_not_connected = peer_table.get('nconn')
                logger.debug("There are no peers disconnected!")
                await asyncio.sleep(discover_interval)

            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(handle_outbound_connection(peer)) for peer in peers_not_connected if peer.get('direction') != 'STALE']
            
            await asyncio.sleep(2 * discover_interval)

    except asyncio.CancelledError:
        logger.debug(f"Task 'start_outbound' closed.")

if __name__ == '__main__':
    pass
        
