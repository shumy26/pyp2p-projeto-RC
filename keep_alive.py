import asyncio
import json
import logging
import time
import uuid

from peer_table import *
import project_config as cfg
from message_router import _peer_id as _peer_id_from_peer_table, build_ping
from p2p_client import disconnect_peer

logger = logging.getLogger("KeepAlive")

# Main function to PING peers

async def ping(peer:Peer):

    # ------- Parameters from the config file to PING peers -------- #

    config_dict = cfg.getConfig()
    wait_time=config_dict.get('timeout')
    attempts=config_dict.get('max_reconnect_attempts')
    ttl = config_dict.get("keep_alive_ttl", 1)

    # ------------------- Taking Info from peer --------------------- #

    peer_id = _peer_id_from_peer_table(peer)
    _, writer = peer.get('conn')
    tries = 1

    # -- Creating Future to get PING in a specific amount of time --- #

    loop = asyncio.get_event_loop()
    pong_future = loop.create_future()
    peer.set('pong',pong_future)

    try:
        # Build the PING request

        ping_dict = build_ping(ttl=ttl)
        request_string = json.dumps(ping_dict) + '\n'

        while tries < attempts:
            logger.debug(f"Sending PING: {json.dumps(ping_dict, ensure_ascii=False)} (Attempt {tries}).")

            # Send the PING request to peer
            async with peer.get('lock'):
                writer.write(request_string.encode())
                await writer.drain()
            
            # Wait for the time that takes to receive the PONG to calculate the RTT. If it passes 'timeout' time, it tries again for 'max_reconnect_attempts' after 2 seconds (Arbitrary time). It disconnects the peer and marks it as STALE, if it doesn't work.

            started_at = time.perf_counter()
            try:
                await asyncio.wait_for(pong_future, wait_time)
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                peer.set('rtt',round(elapsed_ms,2))
                logger.debug(f"PONG from {peer_id} received. RTT: {round(elapsed_ms,2)}.")

                return True

            except asyncio.TimeoutError:
                await asyncio.sleep(2)
                tries += 1

        logger.warning(f"The peer {peer_id} didn't respond the PingPong protocol after {tries} attemps. It will be disconnected...")
        disconnected = await disconnect_peer(peer)

        if disconnected:
            return False

    # If any of the following errors are raised, disconnect the peer.

    except ConnectionRefusedError:
        logger.debug("The peer refused the keep_alive connection.")
        disconnected = await disconnect_peer(peer)
        return None
    except TimeoutError:
        logger.debug("Could not connect to peer due to timed out attempt.")
        disconnected = await disconnect_peer(peer)
        return None
    except OSError:
        logger.debug("There was a network issue and it was not possible to connect to the peer.")
        disconnected = await disconnect_peer(peer)
        return None
    except (InterruptedError, KeyboardInterrupt):
        logger.debug("The keep_alive loop was interrupted by a signal.")
        return None
    except json.JSONDecodeError:
        logger.debug("The peer returned an invalid JSON response for keep_alive.")
        disconnected = await disconnect_peer(peer)
        return None

# PING/PONG Loop for each peer (Just sends the PING for 'keep_alive_interval' seconds.)

async def ping_loop(peer:Peer):
    interval = cfg.getConfig().get("keep_alive_interval",30)
    try:
        while True:
            try:
                logger.debug(f"The code entered in the ping_loop to {_peer_id_from_peer_table(peer)} with {interval} seconds")
                pingged = await ping(peer)
                if not pingged:
                    return None

                logger.debug(f"The code tried to ping to {_peer_id_from_peer_table(peer)}.")
                await asyncio.sleep(interval)
            except TypeError:
                logger.debug("Probably, the 'keep_alive_interval' parameter in config file is configured wrong.")
                pass
    except:
        logger.debug("Something went wrong in the ping_loop. Closing the task.")
