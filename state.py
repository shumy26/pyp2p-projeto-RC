import pathlib
import os
import time
import json
import logging
import asyncio
import rendevouz_connection3 as rdv
from peer_table import *

logger = logging.getLogger('StateManager')

def state(discover_dict):
    # If the discover returned 'None' to this function, than it is a specific search for peers
    # which did not returned anything
    if discover_dict == None:
        logger.warning("There were no peers to be found within this search.")
        return None

    # If it return an empty dict, it is beacuse the user is not registered in the network
    elif discover_dict == {}:
        return []
    
    # Creates a copy of the peer list
    peers_list = discover_dict["peers"].copy()
    my_ip = cfg.getConfig().get('ip')

    new_set = set()
    # Update the PeerTable with an ID to each peer
    for i in range(len(peers_list)):
        if peers_list[i]['ip'] != my_ip:
            peer = Peer(peers_list[i]["name"], peers_list[i]["namespace"], peers_list[i]["ip"], peers_list[i]["port"])
            new_set.add(peer)

    # Updates PeerTable verifying if the peers connected to the rendevouz changed
    union = peer_table.get('nconn').union(new_set) 
    diff = peer_table.get('nconn').difference(new_set)
    peer_table.set(union.difference(diff), 'nconn')

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
    except:
        logger.error("Something went wrong during the update of PeerTable.json")
        return None

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
        