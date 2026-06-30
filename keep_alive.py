import asyncio
import json
import logging
import time
import uuid

from peer_table import *
import project_config as cfg
from p2p_client import disconnect_peer
from message_router import _peer_id as _peer_id_from_peer_table, build_ping

logger = logging.getLogger("KeepAlive")

async def ping(peer:Peer):
    wait_time=cfg.getConfig().get('timeout')
    msg_size=cfg.getConfig().get('msg_size',32768)
    attempts=cfg.getConfig().get('max_reconnect_attempts')
    # -----------------------------------------------------
    peer_id = _peer_id_from_peer_table(peer)
    config_dict = cfg.getConfig()
    ttl = config_dict.get("keep_alive_ttl", 1)
    reader, writer = peer.get('conn')
    tries = 1
    # -----------------------------------------------------
    loop = asyncio.get_event_loop()
    pong_future = loop.create_future()
    peer.set('pong',pong_future)

    try:

        ping_dict = build_ping(ttl=ttl)
        request_string = json.dumps(ping_dict) + '\n'

        while tries < attempts:
            logger.debug(f"Sending PING: {json.dumps(ping_dict, ensure_ascii=False)} (Attempt {tries}).")

            async with peer.get('lock'):
                writer.write(request_string.encode())
                await writer.drain()
                
            started_at = time.perf_counter()
            try:
                response_string = await asyncio.wait_for(pong_future, wait_time)
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                peer.set('rtt',round(elapsed_ms,2))
                logger.debug(f"PONG from {peer_id} received. RTT: {round(elapsed_ms,2)}.")

                return True

            except asyncio.TimeoutError:
                await asyncio.sleep(2)
                tries += 1

        logger.warning(f"The peer {peer_id} didn't respond the PingPong protocol after {tries} attemps. It will be disconnected...")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        if disconnected:
            return False


    except ConnectionRefusedError:
        logger.debug("The peer refused the keep_alive connection.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None
    except TimeoutError:
        logger.debug("Could not connect to peer due to timed out attempt.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None
    except OSError:
        logger.debug("There was a network issue and it was not possible to connect to the peer.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None
    except (InterruptedError, KeyboardInterrupt):
        logger.debug("The keep_alive loop was interrupted by a signal.")
        return None
    except json.JSONDecodeError:
        logger.debug("The peer returned an invalid JSON response for keep_alive.")
        disconnected = await disconnect_peer(peer, writer=writer,reader=reader)
        return None

async def ping_loop(peer:Peer):
    interval = cfg.getConfig().get("keep_alive_interval",30)
    try:
        while True:
            try:
                logger.debug(f"The code entered in the ping_loop to {_peer_id_from_peer_table(peer)} with {interval} seconds")
                await ping(peer)
                logger.debug(f"The code tried to ping to {_peer_id_from_peer_table(peer)}.")
                await asyncio.sleep(interval)
            except TypeError:
                logger.debug("Probably, the 'keep_alive_interval' parameter in config file is configured wrong.")
                pass
    except:
        logger.debug("Something went wrong in the ping_loop. Closing the task.")

# async def start_PingPong():

#     async with peer_table_lock:
#         peers_connected = peer_table.get('conn')

#     tasks = [ping_loop(peer) for peer in peers_connected if (peer.get('direction') != 'STALE')]
#     results = await asyncio.gather(*tasks, return_exceptions=True)
        
#     if results:
#         return True
