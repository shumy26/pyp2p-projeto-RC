import json
import getpass
import pathlib
import socket
# import sys
import os

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
        config_dict["ttl"] = 7200
        config_dict["msg_size"] = 32768
        config_dict["timeout"] = 5
        config_dict["max_reconnect_attempts"] = 10
        config_dict["keep_alive_interval"] = 30
        config_dict["features"] = ["ack", "metrics"]
        
        with open("config.json", "w") as config_file:
            json.dump(config_dict, config_file, indent=4, ensure_ascii=False)

    except:
        print("Something went wrong when trying to write to 'config.json'.")

# This function changes the config file. It returns the old configuration if succeeded; otherwise, None.

def change_config(rdv_name=None, rdv_port=None, name=None, port=None, namespace=None, ttl=None, max_reconnect_attempts=None, keep_alive_interval=None, features=None):
    try:
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
            if value != None:
                n += 1
                config_dict[key] = value
        if n == 0:
            print("There were no changes to be made.")
            return None

        # Updates config file
        with open(file_path, "w") as config_file:            
            json.dump(config_dict, config_file, indent=4, ensure_ascii=False)

        return old_config_dict
    except:
        print("Something went wrong when changing the config file.")
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
        print("Something went wrong when trying to reach 'config.json'.")
        return {}
    
if __name__ == "__main__":
    write_config()
    print(getConfig())
    
    change_config()
    print('\n')
    print(getConfig())
    print('\n')
    
    change_config(namespace="CIC")
    print(getConfig())
   
    