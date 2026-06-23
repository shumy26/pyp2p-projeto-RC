import pathlib
import os
import time
import json
import logging
import rendevouz_connection as rdv

logger = logging.getLogger(__name__)

# This function defines the state of the PeerTable, updating it as it is necessary.

def state(discover_dict):
    # If the discover returned 'None' to this function, than it is a specific search for peers
    # which did not returned anything
    if discover_dict == None:
        logger.warning("There were no peers to be found within this search.")
        return None
    # If it return an empty dict, it is beacuse the user is not registered in the network
    elif discover_dict == {}:
        return []
    
    # Creates a copy of the peer list.
    peers_list = discover_dict["peers"].copy()
    
    # Verifies if the PeerTable exists. If it doesn't, create it.
    cwd = os.path.dirname(os.path.realpath(__file__))
    os.chdir(cwd)
    file_path = pathlib.Path("PeerTable.json")
    file_path.touch(exist_ok=True)

    # Update the Peer List with an ID to each peer
    for i in range(len(peers_list)):
        peers_list[i]["peer_id"] = (peers_list[i].pop("name") + '@' + peers_list[i].pop("namespace"))
    
    # Writes to file the PeerTable
    try:
        with open(file_path, 'w') as peer_file:
            json.dump(peers_list, peer_file, indent=4, ensure_ascii=False)
            return peers_list
    except:
        logging.error("Something went wrong during the update of PeerTabel.json")
        return None
    
    
                
                
        