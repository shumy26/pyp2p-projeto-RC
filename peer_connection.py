import asyncio
import json
import logging
# import pathlib
# import socket
# import threading
import traceback

import project_config as cfg
import rendevouz_connection as rdv
from peer_table import *
from message_router import _local_peer_id, _peer_id as _peer_id_from_peer_table, build_hello, build_hello_ok,handle_incoming_message
from keep_alive import ping_loop

logger = logging.getLogger('PeerConnector')


# This function handles the loop of attempts to connect to a peer 'outboundly'.

async def peer_connection(peer:Peer):
    # Verifies if the argument passed is of the type Peer (defined in peer_table.py).
    if not isinstance(peer,Peer):
        logger.debug("It seems a Peer object was not passed to this function.")
        return False

    # Get some IDs from your peer and the one you are trying to connect.

    my_id = _local_peer_id()
    peer_id = _peer_id_from_peer_table(peer)
    host = peer.get('ip')
    port = peer.get('port')

    # Verifies if the msg_size is not less than zero.

    msg_size = cfg.getConfig().get('msg_size', 32768)
    if msg_size <= 0:
        msg_size = 32768    

    try:
        # Tries to open a connection with a peer registered in the rendevouz.

        logger.debug("Opening a connection...")
        reader, writer = await asyncio.open_connection(host, port)
        logger.debug(f"The code opened a connection to {peer_id} -> {host}:{port}!")

        # Sends a HELLO request to the peer and waits for a HELLO_OK response. If it doesn't answer or it return a wrong answer, it will decrease the total number of attempts of that peer.

        hello_dict = build_hello(my_id)
        hello_request = json.dumps(hello_dict) + '\n'
        hello_request = hello_request.encode()

        writer.write(hello_request)
        await writer.drain()

        response = await reader.readuntil(b'\n')

        if not response:
            return False

        response_dict = json.loads(response.decode().strip('\n'))

        match response_dict.get('type'):

            case 'HELLO_OK':
                # Change the attributes of this peer
                peer.set('direction','Outbound')
                peer.reset_attempts()
                peer.set('conn',(reader,writer))

                # Updates the PeerTable, changing state of peer from disconnected to connected.
                async with peer_table_lock:
                    peer_table.update(peer)
                    logger.debug(f"{peer_table}")

                logger.info(f"Outbound Connection Established: {peer_id}.")

                # Create instances to handle PING and incoming messages of Outbound connections relative to that Peer.
                asyncio.create_task(handle_incoming_message(peer), name=f"handle_msg_{peer_id}")
                asyncio.create_task(ping_loop(peer), name=f"handle_ping_{peer_id}")
                return True

            case _:
                logger.error(f"The peer {peer_id} answered HELLO with an invalid response.")
                peer.decrease_attempts()
                return False

    # Raised when the peer refuses the connection.
    except ConnectionRefusedError:
        logger.debug(f"The peer {peer_id} refused to connect.")
        peer.decrease_attempts()
        return False

    # Raised when the socket could not connect to the IP address in a given time.
    except TimeoutError:
        logger.debug(f"Connection to {peer_id} timed out.")
        peer.decrease_attempts()
        return False

    # Raised when there are possibly no routes to that server.
    except OSError:
        logger.debug(f"There was a network issue and it was not possible to connect to {peer_id} or the socket is already instantiated.")
        logger.debug(f"{traceback.format_exc()}")
        raise OSError
    
    # Raised when there was an error parsing the json.
    except json.JSONDecodeError:
        logger.debug("The peer answered the request with an invalid JSON object.")
        peer.decrease_attempts()
        return False

    # Raised when the peer doesn't exist in the PeerTable.
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

    async with peer_table_lock:
        connected = peer_table.get('conn')
        for conn_peer in connected:
            if conn_peer == peer:
                logger.debug("That peer is already connected.")
                raise OSError

    peer_id = _peer_id_from_peer_table(peer)
    connected = False
    # Get 'max_reconnect_attempts' from the peer.
    max_n = peer.get('attempts')
    # Initial number to exponential backoff '2**n' (Base 2 was chosen because, even if it were to be raised to a number somewhat big, like 15, it won't be an absurd amount of time spend in trying this task.)
    n = 1
    try:
        # Try to connect to peer for max_reconnect_attempts
        while n < max_n:
            connected = await peer_connection(peer)
            # If the peer is not connected, try again
            if not connected and n < max_n:
                await asyncio.sleep(2**n)
                n = max_n - peer.get('attempts')
                continue
            # If it is connected or n == max_n, break from the loop
            break
        
        if peer.get('attempts') == 0:
            logger.error(f"Multiple attempts were made to {peer_id}! Connection failed. It is being marked as STALE.")
            peer.set('direction','STALE')
            peer.reset_attempts()
        
        # Returns False if it fails forever, and True if it connects at least one time
        return connected
    
    except asyncio.CancelledError:
        logger.debug(f"The task handling the connection with {peer_id} was cancelled.")
        return False

    except OSError:
        return False

    except:
        logger.debug(f"An error occured when trying to connect to {peer_id}.")
        return False


# This function handles attempts of a peer trying to connect to this device

async def handle_inbound_connection(reader:asyncio.StreamReader, writer:asyncio.StreamWriter):

    my_id = _local_peer_id()

    try:

        # Pick the IP of the peer, and creates a Peer object to it with that info.

        peer_ip,_ = writer.get_extra_info("peername")

        current_peer = Peer(IP=peer_ip)
        
        # From the peer table, we get the (reader, writer) of each peer. If it already exists (the socket is instantiated), it fails the connection with that peer.
        # async with peer_table_lock:
        #     connected = peer_table.get('conn')
        #     for conn_peer in connected:
        #         if (conn_peer.get("ip"), conn_peer.get("name"), conn_peer.get("namespace")) == (current_peer.get("ip"), current_peer.get("name"), current_peer.get("namespace")):
        #             logger.debug("That peer is already connected.")
        #             raise Exception

        # Read from the buffer the request.
        raw = await reader.readuntil(b'\n')
        
        # If it doesn't have anything, close the socket and quits the task.
        if not raw:
            logger.debug("The peer didn't send HELLO. The connection is being stopped!")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
                raise Exception

        # Receives the request and treat it to see if it is a HELLO
        request = raw.decode(encoding='utf-8').strip('\n')
        request_dict = json.loads(request)
        peer_id = request_dict.get("peer_id")

        match request_dict.get('type'):
            # If the request is a HELLO, sets the Peer attributes accordingly, put it in the PeerTable and sends a HELLO_OK.
            case "HELLO":

                logger.debug(f"HELLO received from: {peer_id}.")

                name, namespace = tuple(peer_id.split("@"))
                current_peer.set("name",name)
                current_peer.set("namespace",namespace)

                # Verify if the peer is in the PeerTable. If it isn't make a discover to see who he is.
                async with peer_table_lock:
                    isInsidePeerTable, peer_port = peer_table.isPeerIDNotConn(current_peer)

                # If the peer is already connected, cancel the attempt.
                if not peer_port:
                    logger.debug("That peer is already connected. Cancelling connection...")
                    raise OSError
                
                if not isInsidePeerTable:
                    discover_dict = await rdv.discover()
                    if discover_dict:
                        peers_list = discover_dict.get("peers",{}).copy()
                        for i in range(len(peers_list)):
                            if (peers_list[i]["ip"], peers_list[i]["name"], peers_list[i]["namespace"]) == (peer_ip, name, namespace):
                                current_peer.set("port",peers_list[i]["port"])
                        
                        if current_peer.get("port") == 0:
                            logger.debug("This peer probably disconnected from the network.")
                            raise Exception
                    else:
                        logger.debug("There is no one in the network.")
                        raise Exception
                
                else:
                    current_peer.set("port", peer_port)

                current_peer.set('direction','Inbound')
                current_peer.set('conn',(reader,writer))

                async with peer_table_lock:
                    peer_table.get("conn").add(current_peer)
                    peer_table.get("nconn").discard(current_peer)
                    logger.debug(f"{peer_table}")

                response_dict = build_hello_ok(my_id,features=cfg.getConfig().get('features'))
                response = json.dumps(response_dict) + '\n'

                writer.write(response.encode())
                await writer.drain()
                logger.debug(f"HELLO_OK sent to: {peer_id}.")

                logger.info(f"Inbound Connection Established: {peer_id}.")
                
                # Create instances to handle PING and incoming messages of Inbound connections relative to that Peer
                asyncio.create_task(handle_incoming_message(current_peer), name=f"handle_msg_{peer_id}")
                asyncio.create_task(ping_loop(current_peer), name=f"handle_ping_{peer_id}")

                # Arbitrary wait time to make other tasks run a bit too.
                await asyncio.sleep(1)

            # If it send a valid JSON object but is not a HELLO, disconnect and ignore it. It will eventually run into the PeerTable 'Not-Connected Peers' if it is from the network.
            case _:
                logger.debug(f"Peer:{peer_id} that connected to you has sent an invalid request type.")
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
                    raise Exception

    # Raised if the request is not a valid JSON object.
    except json.JSONDecodeError:
        logger.debug("The peer that tried to connect to you has sent an invalid request - It was not a valid JSON object.")

    # Raised if your host refuses the connection for some reason.
    except ConnectionRefusedError:
        logger.warning(f"A host that is not currently registered in the network tried to connect. Host: {writer.get_extra_info('peername')}.")

    # Raised when the task is cancelled.
    except asyncio.CancelledError:
        logger.debug(f"The task handling the connection with {peer_id} was cancelled and the socket is closing...")

    # Raised when the Peer actively refuses the connection.
    except ConnectionError:
        logger.info(f"The connection with {peer_id} was terminated by the peer.")
    
    except OSError:
        logger.debug(f"The peer {peer_id} tried to connect, but it is already connected.")
        pass

    # If peer_id not found -> raised NameError. If Excpetion is raised, it is because the code raised it when trying to connect to a peer.
    except (NameError, Exception):
        pass

# This function starts the server in the machine, trying to listen to connections of every network interface on the port specified in the config file. Defaults to listen on port 35500 (arbitrary choice).
async def start_inbound():
    '''Starts the server loop in order to connect to [Inbound] peers'''
    host = '0.0.0.0'
    port = cfg.getConfig().get('port',35500)

    try:
        logger.debug(f"Start listening to Inbound connections on port: {port}.")
        server = await asyncio.start_server(handle_inbound_connection,host,port)
        async with server:
            await server.serve_forever()

    except asyncio.CancelledError:
        logger.debug(f"Task 'start_inbound' closed.")

    except TypeError:
        pass


# Initiates the attempts to connect to peers externally. It is an infinite loop that tries every (2 * 'discover_interval') - that is, two loops of the DISCOVER protocol - to connect to peers that are not connected and not STALE.
async def start_outbound():
    '''Starts the loop in order to connect to [Outbound] peers'''
    try:
        discover_interval = cfg.getConfig().get('discover_interval',10)
        peers_not_connected = set()
        while True:

            async with peer_table_lock:
                peers_not_connected = peer_table.get('nconn')

            # This is a loop that continues doing nothing while there are no new peers to connect.
            while (len(peers_not_connected) == 0) or (len(peers_not_connected) != 0 and (False not in [peer.get('dir') == 'STALE' for peer in peers_not_connected])):
                await asyncio.sleep(2 * discover_interval)
                async with peer_table_lock:
                    peers_not_connected = peer_table.get('nconn')
                logger.debug("There are no peers disconnected or all the peers are 'STALE'.")
                 # Interval chosen to wait some seconds before trying to connect to any peer again.
                
            tasks = [handle_outbound_connection(peer) for peer in peers_not_connected if peer.get('direction') != 'STALE']
            results = await asyncio.gather(*tasks, return_exceptions=True) # The variable is just to hold the operations and not let python garbage collect them

            await asyncio.sleep(2 * discover_interval) # Interval chosen to wait some seconds before trying to connect to any peer again.

    except asyncio.CancelledError:
        logger.debug(f"Task 'start_outbound' closed.")

if __name__ == '__main__':
    pass
        
