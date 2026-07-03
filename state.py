import pathlib
import os
import time
import json
import logging
import asyncio
import rendevouz_connection as rdv
from peer_table import *

logger = logging.getLogger('StateManager')

# This function changes the state of the PeerTable, meaning it updates the 'not_connected' attribute of the PeerTable instance, based on the most recent DISCOVER return, the peers currently connected and the ones not connected.

async def state(discover_dict):
    try:
        # If the discover returned 'None' to this function, than it is a specific search for peers
        # which did not returned anything
        if discover_dict == None:
            logger.debug("There were no peers to be found within this search.")
            return None

        # If it return an empty dict, it is beacuse the user is not registered in the network
        elif discover_dict == {}:
            return []

        # Creates a copy of the peer list
        peers_list = discover_dict["peers"].copy()
        my_ip = cfg.getConfig().get('ip')
        my_name = cfg.getConfig().get('name')
        my_namespace = cfg.getConfig().get('namespace')

        new_set = set()
        # Update the PeerTable with an ID to each peer
        for i in range(len(peers_list)):
            if (peers_list[i]["ip"], peers_list[i]['name'],peers_list[i]["namespace"]) != (my_ip, my_name, my_namespace):
                peer = Peer(peers_list[i]["name"], peers_list[i]["namespace"], peers_list[i]["ip"], peers_list[i]["port"])
                new_set.add(peer)

        # Updates PeerTable verifying if the peers connected to the rendevouz changed. This is made by doing
        # some mathmatical operations upon sets.
        async with peer_table_lock:
            # logger.debug(f"PeerTable: {peer_table}")
        
            union = peer_table.get('nconn').union(new_set)
            # logger.debug(f"union: {[(peer.get('ip'),peer.get('port')) for peer in union]}")
            diff = peer_table.get('nconn').difference(new_set)
            # logger.debug(f"diff: {[(peer.get('ip'),peer.get('port')) for peer in diff]}")
            new_set = union.difference(diff)
            # logger.debug(f"new_set1: {[(peer.get('ip'),peer.get('port')) for peer in new_set]}")

            intersection = peer_table.get('conn').intersection(new_set)
            # logger.debug(f"intersection: {[(peer.get('ip'),peer.get('port')) for peer in intersection]}")
            new_set = new_set.difference(intersection)
            # logger.debug(f"new_set2: {[(peer.get('ip'),peer.get('port')) for peer in new_set]}")

            peer_table.set(new_set, 'nconn')
            # logger.debug([(peer.get("ip"), peer.get("port")) for peer in peer_table.get('nconn')])

        # Verifies if the PeerTable exists. If it doesn't, create it
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)
        file_path = pathlib.Path("PeerTable.json")
        file_path.touch(exist_ok=True)

        # Writes to file the PeerTable
        try:
            with open(file_path, 'w') as peer_file:
                json.dump(peers_list, peer_file, indent=4, ensure_ascii=False)
            
            logger.debug("PeerTable.json was updated.")
            return peer_table
        
        except Exception as e:
            logger.debug(f"Something went wrong during the update of PeerTable.json: {e}.")
            return None
    
    # Raised when it is cancelled.
    except asyncio.CancelledError:
        pass
    
    except Exception as e:
        logger.debug(f"An error occured in state: {e}.")
        
if __name__ == '__main__':
    asyncio.run(rdv.register())
    n = 5
    while n > 0:
        state_change = state(asyncio.run(rdv.discover()))
        print(state_change)
        time.sleep(1)
        if state_change == None:
            break
        n -= 1
        
    asyncio.run(rdv.unregister())    
        