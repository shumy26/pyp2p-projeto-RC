import asyncio
import json
import logging
# import pathlib
# import socket
# import threading

import project_config as cfg
from peer_table import *
from message_router import _local_peer_id, _peer_id as _peer_id_from_peer_table, build_hello, build_hello_ok,handle_incoming_message
from keep_alive import ping_loop

logger = logging.getLogger('PeerConnector')

# def _peer_id_from_peer_table(peer:Peer):
#     if not isinstance(peer, Peer):
#         return None

#     name = peer.get("name") if isinstance(peer, Peer) else None
#     namespace = peer.get("namespace") if isinstance(peer, Peer) else None
#     if name and namespace:
#         return f"{name}@{namespace}"

#     return None

async def peer_connection(peer:Peer):
    if not isinstance(peer,Peer):
        logger.debug("It seems a Peer object was not passed to this function.")
        return False

    logger.debug("The code got here 1!")
    my_id = _local_peer_id()
    logger.debug("The code got here 2!")
    peer_id = _peer_id_from_peer_table(peer)
    logger.debug("The code got here 3!")
    msg_size = cfg.getConfig().get('msg_size', 32768)
    try:
        if not isinstance(msg_size, int) or msg_size <= 0:
            msg_size = 32768
    except Exception:
        msg_size = 32768
    host = peer.get('ip')
    port = peer.get('port')

    try:
        logger.debug("Opening a connection..")
        reader, writer = await asyncio.open_connection(host, port)
        logger.debug(f"The code opened a connection to {peer_id} -> {host}:{port}!")

        hello_dict = build_hello(my_id)
        hello_request = json.dumps(hello_dict) + '\n'
        hello_request = hello_request.encode()

        writer.write(hello_request)
        await writer.drain()

        response = await reader.read(msg_size)

        if not response:
            return False

        response_dict = json.loads(response.decode().strip('\n'))

        match response_dict.get('type'):

            case 'HELLO_OK':
                peer.set('direction','Outbound')
                peer.reset_attempts()
                peer.set('conn',(reader,writer))
                async with peer_table_lock:
                    peer_table.update(peer)
                logger.info(f"Outbound Connection Established: {peer_id}.")
                asyncio.create_task(handle_incoming_message(peer))
                asyncio.create_task(ping_loop(peer))
                return True

            case _:
                logger.error(f"The peer {peer_id} answered HELLO with an invalid response.")
                peer.decrease_attempts()
                return False

    # Raised when the peer refuses the connection
    except ConnectionRefusedError:
        logger.debug(f"The peer {peer_id} refused to connect.")
        peer.decrease_attempts()
        return False

    # Raised when the socket could not connect to the IP address in a given time
    except TimeoutError:
        logger.debug(f"Connection to {peer_id} timed out.")
        peer.decrease_attempts()
        return False

    # Raised when there are possibly no routes to that server
    except OSError:
        logger.debug(f"There was a network issue and it was not possible to connect to {peer_id} or the socket is already instantiated.")
        peer.decrease_attempts()
        return False
    
    except json.JSONDecodeError:
        logger.debug("The peer answered the request with an invalid JSON object.")
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
        while n <= max_n:
            connected = await peer_connection(peer)
            # If the peer is not connected, try again
            if not connected and n < max_n:
                await asyncio.sleep(2**n)
                n += 1
                continue
            # If it is connected or n == max_n, break from the loop
            break
        
        if peer.get('attempts') == 0:
            logger.error(f"Multiple attempts were made to {peer_id}! Connection failed. It is being marked as STALE.")
            peer.set('direction','STALE')
        
        # Returns False if it fails forever, and True if it connects at least one time
        return connected
    
    except asyncio.CancelledError:
        logger.debug(f"The task handling the connection with {peer_id} was cancelled.")
        # raise asyncio.CancelledError
        return False

    except:
        logger.debug(f"An error occured when trying to connect to {peer_id}.")
        return False


# This function handles attempts of a peer trying to connect to this device

async def handle_inbound_connection(reader:asyncio.StreamReader, writer:asyncio.StreamWriter):
    my_id = _local_peer_id()
    msg_size = cfg.getConfig().get('msg_size',32768)

    try:
        async with peer_table_lock:
            connected = peer_table.get('conn')
            for peer in connected:
                if peer.get('conn') == (reader,writer):
                    logger.debug("That peer is already connected.")
                    raise Exception

        peer_ip, peer_port = writer.get_extra_info('peername')

        current_peer = Peer()
        # async with peer_table_lock:
        #     for peer in peer_table.get('nconn'):
        #         if (peer.get('ip') == peer_ip and peer.get('port') == peer_port) and (peer.get('direction') != 'STALE'):
        #             current_peer = peer
        #             break

        # if current_peer.get('name') == '':
        #     raise ConnectionRefusedError

        # peer_id = _peer_id_from_peer_table(current_peer)

        raw = await reader.readuntil(b'\n')
            
        if not raw:
            logger.debug("The peer didn't send HELLO. The connection is being stopped!")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
                raise Exception

        request = raw.decode(encoding='utf-8').strip('\n')
        request_dict = json.loads(request)
        peer_id = request_dict.get("peer_id")

        match request_dict.get('type'):

            case "HELLO":
                logger.debug(f"HELLO received from: {peer_id}.")

                name, namespace = tuple(peer_id.split("@"))
                current_peer.set("name",name)
                current_peer.set("namespace",namespace)
                current_peer.set('direction','Inbound')
                current_peer.set('attempts',current_peer.get('attempts'))
                current_peer.set('conn',(reader,writer))
                peer_table.get("conn").add(current_peer)

                logger.info(f"Inbound Connection Established: {peer_id}.")

                asyncio.create_task(handle_incoming_message(current_peer))
                asyncio.create_task(ping_loop(current_peer))

                response_dict = build_hello_ok(my_id,features=cfg.getConfig().get('features'))
                response = json.dumps(response_dict) + '\n'

                writer.write(response.encode())
                await writer.drain()
                logger.debug(f"HELLO_OK sent to: {peer_id}.")

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
    
    except NameError, Exception:
        pass


async def start_inbound():
    '''Starts the server loop in order to connect to [Inbound] peers'''
    host = '0.0.0.0'
    port = cfg.getConfig().get('port')
    
    if isinstance(port,type(None)):
        logger.debug("The port is not configured in the config file. Change it!")
        raise TypeError

    try:
        logger.debug(f"Start listening to Inbound connections on port: {port}.")
        server = await asyncio.start_server(handle_inbound_connection,host,port)
        async with server:
            await server.serve_forever()

    except asyncio.CancelledError:
        logger.debug(f"Task 'start_inbound' closed.")

    except TypeError:
        pass


# Initiates the attempts to connect to peers externally
async def start_outbound():
    '''Starts the loop in order to connect to [Outbound] peers'''
    try:
        discover_interval = cfg.getConfig().get('discover_interval',10)
        peers_not_connected = set()
        while True:

            async with peer_table_lock:
                peers_not_connected = peer_table.get('nconn')

            while (len(peers_not_connected) == 0) or (len(peers_not_connected) != 0 and (False not in [peer.get('dir') == 'STALE' for peer in peers_not_connected])):
                async with peer_table_lock:
                    peers_not_connected = peer_table.get('nconn')
                logger.debug("There are no peers disconnected or all the peers are 'STALE'.")
                await asyncio.sleep(2 * discover_interval) # Interval chosen to wait some seconds before trying to connect to any peer again.

            tasks = [handle_outbound_connection(peer) for peer in peers_not_connected if peer.get('direction') != 'STALE']
            results = await asyncio.gather(*tasks, return_exceptions=True) # The variable is just to hold the operations and not let python garbage collect them
            
            await asyncio.sleep(2 * discover_interval) # Interval chosen to wait some seconds before trying to connect to any peer again.

    except asyncio.CancelledError:
        logger.debug(f"Task 'start_outbound' closed.")

if __name__ == '__main__':
    pass
        
