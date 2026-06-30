import json
import getpass
import pathlib
import socket
import logging
import os

logger = logging.getLogger(__name__)

# -----------------------------

# cfg.write_config(): Writes to config file;
# cfg.getConfig(): Gets the configuration as a dictionary;
# cfg.change_config(): Changes the config file.

# -----------------------------

# Funcion to verify if a port is being used.

def isPortUsed(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as testSocket:
            testSocket.bind(('',port))
        return False
    except OSError:
        return True

# This function creates a standard 'config.json' based on the current user name.
# It is only called when you register the first time.
# So you can actually alter the config file later on without problems.
# The param 'ttl' is an optional one. If the config file has no 'ttl', than, it assumes
# that is not to be put in the request.

def write_config():
    try:
        # Creating config.json
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)
        file_path = pathlib.Path("config.json")
        file_path.touch(exist_ok=True)

        if os.path.getsize(file_path) != 0:
            raise FileExistsError
        
        config_dict = {}
        
        config_dict["rdv_name"] = 'pyp2p.mfcaetano.cc'
        config_dict["rdv_port"] = 8080
        config_dict["name"] = getpass.getuser()
        # The upper limit of the range was defined that way because: 65540 = 65535 + 5
        for port in range(35500,65540,5):
            if (not isPortUsed(port)):
                config_dict["port"] = port
                break
        config_dict["namespace"] = "UnB"
        config_dict["msg_size"] = 32768 # 32 kB
        config_dict["timeout"] = 5 # Time to seconds to receive an ACK
        config_dict["max_reconnect_attempts"] = 6
        # config_dict["ttl"] = 3600
        config_dict["discover_interval"] = 5
        config_dict["keep_alive_interval"] = 30
        config_dict["features"] = ["ack"]
        
        with open("config.json", "w") as config_file:
            json.dump(config_dict, config_file, indent=4, ensure_ascii=False)
            
    except FileExistsError:
        logger.debug("The config file already exists. So it was not created")

    except:
        logger.error("Something went wrong when trying to write to 'config.json'.")

# This function changes the config file. It returns the old configuration if succeeded; otherwise, None.

def change_config(rdv_name=None, rdv_port=None, name=None, port=None, namespace=None, msg_size=None, timeout=None, ttl=None, max_reconnect_attempts=None, discover_interval=None, keep_alive_interval=None, features=None, **kwargs):
    try:
        logger.debug("Trying to change the config file...")
        # Returns a dictionary that contains the local variables scope
        changes = locals().copy()
        
        # Changes the Working Directory to the current file path
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)
        file_path = pathlib.Path("config.json")
        file_path.touch(exist_ok=True)

        # Returns the current info stored in config file
        config_dict = getConfig()
        old_config_dict = config_dict
        
        # Updates the current info to updated info
        n = 0
        for key, value in changes.items():
            if value != None and not isinstance(value,dict):
                n += 1
                config_dict[key] = value
            if isinstance(value,dict) and (len(value) != 0):
                n += 1
                config_dict.update(value)

        if n == 0:
            logger.debug("There were no changes to be made.")
            return None

        # Updates config file
        with open(file_path, "w") as config_file:            
            json.dump(config_dict, config_file, indent=4, ensure_ascii=False)

        logger.debug(f"Changed the config file successfully.")
        return old_config_dict
    except:
        logger.debug("Something went wrong when changing the config file.")
        return None

# This function reads the config file and returns a dict like object with its {options:values}.
# It returns an empty dict, if an exception is raised.

def getConfig():
    try:
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)
        file_path = pathlib.Path("config.json")
        file_path.touch(exist_ok=True)
        
        with open(file_path, "r") as config_file:
            config_dict = json.load(config_file)
        
        return config_dict
    except:
        logger.error("Something went wrong when trying to reach 'config.json'.")
        return {}

# This function adds a new configuration to the config file
# If an exception is raised, it returns an empty dict

def addConfig(new_config, new_value):
    try:
        current_config = getConfig()
        current_config[new_config] = new_value
        change_config(**current_config)
        logger.debug(f"Added new config -> ({new_config}:{new_value}).")
    except:
        logger.error(f"An error occurred when trying to update the config file with a new pair: ({new_config}:{new_value}).")
        return {}
    
if __name__ == "__main__":
    write_config()
    print(getConfig())
    
    change_config()
    print('\n')
    print(getConfig())
    print('\n')
    
    addConfig("ip",'172.0.0.1')
    print(getConfig())

    # change_config(namespace="CIC")
    # print(getConfig())
   
    