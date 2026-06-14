import json
# import pathlib
# import sys
# import os
import socket
import time
import project_config as cfg

# In this code, if a return statement is "None", then it is either an error that occurred or
# an invalid parameter tha was verified before an error would occur.
# If a return is "{}" (an empty dictionary)

# cfg.write_config(): Writes to config file;
# cfg.getConfig(): Gets the configuration as a dictionary;
# cfg.change_config(): Changes the config file.

# This function defines the JSON object that each 'action_type' needs on the rendevouz.
# It alters the config file if it is called with any of the arguments below (except for 'ttl')

def define_rdv_protocol(action_type:str, name=None, namespace=None, port=None, ttl=None):
    try:
        action_type = action_type.upper()
        protocol_field = {}
        
        config_dict = cfg.getConfig()
        
        if action_type == 'REGISTER':
            protocol_field["type"] = action_type
            protocol_field["namespace"] = str(config_dict["namespace"])
            protocol_field["name"] = str(config_dict["name"])
            protocol_field["port"] = int(config_dict["port"])
            if ttl != None:
                protocol_field["ttl"] = ttl
        
        elif action_type == 'DISCOVER':
            protocol_field["type"] = action_type
           
            # If it is None or the user types '', it will return to the default value.
            # The option with the empty string was put because the rendevouz don't accept it as a valid namespace.
            if (bool(namespace)):
                namespace = str(namespace)
                protocol_field["namespace"] = namespace
                cfg.change_config(namespace=namespace)

        elif action_type == 'UNREGISTER':
            protocol_field["type"] = action_type
            protocol_field["namespace"] = str(config_dict["namespace"])
            
            #name = str(name)
            #if (not bool(name)):
            protocol_field["name"] = str(config_dict["name"])
            #else:
                #protocol_field["name"] = name
                #cfg.change_config(name=name)
                        
            #port = int(port)
            #if (not bool(port)):
            protocol_field["port"] = int(config_dict["port"])
            #else:
                #protocol_field["port"] = port
                #cfg.change_config(port=port)
        else:
            print("There is no such action available to communicate with the rendevouz.\n(Maybe you misspelled it?)")
            return {}
        return protocol_field
    except TypeError:
        print("Some argument is of the incorrect type.")
        return None
    else:
        return None

# Registers the user in the network, using the Rendevouz IP address.
# As I am doing right now, the user would define the ttl or just leave as 'None', which
# would make the rendevouz define ttl=7200s (2h). This is not in the config file as per now.

def register(clientTTL=None):
    
    config_dict = cfg.getConfig()
    connIP = config_dict["rdv_name"]
    connPort = config_dict["rdv_port"]
    
    if not (isinstance(connIP, str) and isinstance(connPort,int)):
        if connPort < 1 and connPort > 65535:
            print("The port chosen for the rendevouz is out of range. It must be between 1 and 65535.")
            return None
        else:
            print("The IP address of the rendevouz must be a string.")
            return None
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as clientSocket:    
            connection_tuple = (connIP,connPort)
            try:
                clientSocket.connect(connection_tuple)
                # Defines the JSON to Register
                register_dict = define_rdv_protocol('REGISTER', ttl=clientTTL)
                request_string = json.dumps(register_dict) + '\n'
                # print(request_string)
                clientSocket.sendall(request_string.encode())
                response_string = clientSocket.recv(32768).decode('utf-8')
                # print(response_string)
                response_dict = json.loads(response_string)
                
                if response_dict["status"] == "ERROR":
                    clientSocket.shutdown(socket.SHUT_RDWR)
                    match response_dict["message"]:
                        case "bad_name":
                            print("The name is invalid. It must be a string between 1 and 64 characters.")
                            return None
                        case "bad_namespace":
                            print("The namespace is invalid. It must be a string between 1 and 64 characters.")
                            return None
                        # TTL treated on function define_rdv_protocol()
                        case "bad_port":
                            print("The port must be an integer between 1 and 65535.")
                            return None
                else:
                    print("You have been registered in the network.")
            
            # Raised when the socket could not connect to the IP address
            # To treat the continuity or beggining of the program as a whole, we can do it when calling the function.
            # If it is None, than an error occured, and we can just not initiate the program and tell the user that something happened.
            except TimeoutError:
                print("Could not connect to server.")
                return None
            
            # Raised when the user or the program interrupts the execution with a signal.
            except (InterruptedError, KeyboardInterrupt):
                print("The program was interrupted by a signal (like Ctrl+C).")
                return None
            
    return response_dict

# Discover Protocol: Namespace=None just means that, as the field is optional, we don't want it sent to the rendevouz
# interval2discover is the interval in which the program sent the discover protocol. The standard is 20 times per minute (3 seconds) 
# Update: This is an old parameter. Now the main file will handle this afterwards.
# -----------------------------------------------------------------------------------------

def discover(namespace=None):
    
    config_dict = cfg.getConfig()
    connIP = config_dict["rdv_name"]
    connPort = config_dict["rdv_port"]
    
    if not (isinstance(connIP, str) and isinstance(connPort,int)):
        if connPort < 1 and connPort > 65535:
            print("The port chosen for the rendevouz is out of range. It must be between 1 and 65535.")
            return None
        else:
            print("The IP address of the rendevouz must be a string.")
            return None
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as clientSocket:    
            connection_tuple = (connIP,connPort)
            try:
                clientSocket.connect(connection_tuple)
            
                # Defines the JSON to Discover
                discover_dict = define_rdv_protocol('DISCOVER', namespace=namespace)
                request_string = json.dumps(discover_dict) + '\n'
                # print(request_string)
                clientSocket.sendall(request_string.encode())
                response_string = clientSocket.recv(32768).decode('utf-8')
                # print(response_string)
                response_dict = json.loads(response_string)
            
                if response_dict["status"] == "ERROR":
                    clientSocket.shutdown(socket.SHUT_RDWR)
                    match response_dict["message"]:
                        case "peer_not_registered":
                            print("You are not registered in the network!")
                            return {}
                        case "bad_namespace":
                            print("The namespace is invalid. It must be a string between 1 and 64 characters.")
                            return None
                
                if (response_dict["status"] == "OK" and len(response_dict["peers"])) == 0:
                    print("There is no peer with the namespace that you entered.")
                    return None

            # Raised when the socket could not connect to the IP address
            except TimeoutError:
                print("Could not connect to server.")
                return None
        
            # Raised when the user or the program interrupts the execution with a signal.
            except (InterruptedError, KeyboardInterrupt):
                print("The program was interrupted by a signal (like Ctrl+C).")
                return None
            
            except TypeError:
                print("A type error occurred when a parameter was passed to a function.")
                return None
                
    return response_dict

# -------------------------------------------------------------------

def unregister():
    
    config_dict = cfg.getConfig()
    connIP = config_dict["rdv_name"]
    connPort = config_dict["rdv_port"]
    
    if not (isinstance(connIP, str) and isinstance(connPort,int)):
        if connPort < 1 and connPort > 65535:
            print("The port chosen for the rendevouz is out of range. It must be between 1 and 65535.")
            return None
        else:
            print("The IP address of the rendevouz must be a string.")
            return None
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as clientSocket:    
            connection_tuple = (connIP,connPort)
            try:
                clientSocket.connect(connection_tuple)
                # Defines the JSON to Register
                unregister_dict = define_rdv_protocol('UNREGISTER', name=config_dict["name"], namespace=config_dict["namespace"], port=config_dict["port"])
                request_string = json.dumps(unregister_dict) + '\n'
                # print(request_string)
                clientSocket.sendall(request_string.encode())
                response_string = clientSocket.recv(32768).decode('utf-8')
                # print(response_string)
                response_dict = json.loads(response_string)
                
                clientSocket.shutdown(socket.SHUT_RDWR)
                if response_dict["status"] == "ERROR":
                    match response_dict["message"]:
                        case "bad_name":
                            print("The name is invalid. It must be a string between 1 and 64 characters.")
                            return None
                        case "bad_namespace":
                            print("The namespace is invalid. It must be a string between 1 and 64 characters.")
                            return None
                        # TTL treated on function define_rdv_protocol()
                        case "bad_port":
                            print("The port must be an integer between 1 and 65535.")
                            return None
                else:
                    print("You have been successfully unregistered.")
            
            # Raised when the socket could not connect to the IP address
            except TimeoutError:
                print("Could not connect to server.")
                return None
            
            # Raised when the user or the program interrupts the execution with a signal.
            except (InterruptedError, KeyboardInterrupt):
                print("The program was interrupted by a signal (like Ctrl+C).")
                return None
    
    # Only returns it when it is "OK"
    return response_dict

# Sends to the Rendevouz
if __name__ == '__main__':
    test_ttl = int(input())
    register(clientTTL=test_ttl)
    discover()

# protocol_data is a dictionary with two lists, that contains dictionaries.
# The first valaue of the dictionary refers to the rendevouz protocol.
# the second element, to the protocol between peers.
# Order in list - Rendevouz: REGISTER, DISCOVER, UNREGISTER.
# Order in list - P2P: HELLO, SEND, PUB, BYE, PING.
# Each element in the list is a valid JSON object.



