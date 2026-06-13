import pathlib
import os
import time
import json
import rendevouz_connection2 as rdv

def state(discover_dict):
    if discover_dict == None:
        print("There were no peers to be found within this search.")
        return None
    peers_list = discover_dict["peers"].copy()
    
    cwd = os.path.dirname(os.path.realpath(__file__))
    os.chdir(cwd)
    file_path = pathlib.Path("PeerTable.json")
    file_path.touch(exist_ok=True)

    for i in range(len(peers_list)):
        peers_list[i]["peer_id"] = (peers_list[i].pop("name") + '@' + peers_list[i].pop("namespace"))

    try:
        with open(file_path, 'w') as peer_file:
            json.dump(peers_list, peer_file, indent=4, ensure_ascii=False)
            print(peers_list)
            return {}
    except:
        print("Something went wrong during the update of PeerTabel.json")
        return None
                    
if __name__ == '__main__':
    rdv.register(clientTTL=240)
    n = 5
    while n > 0:
        state_change = state(rdv.discover(interval2discover=0))
        time.sleep(0.2)
        if state_change == None:
            break
        n -= 1
        
    rdv.unregister()
    
    
                
                
        