import asyncio
import json
import time
import project_config as cfg

# In this code, if a return statement is "None", then it is either an error that occurred or
# an invalid parameter that was verified before an error would occur.

# This function defines the JSON object that each 'action_type' needs on the rendevouz.
# It alters the config file if it is called with any of the arguments below.

def define_rdv_protocol(action_type:str, namespace=None, name=None, port=None, ttl=None):
    """
    Defines the protocol request based on the type of the request.
    
    Args:
        action_type (str): REGISTER, DISCOVER or UNREGISTER.
        
        namespace (str): The namespace in which you desire to be registered in the network.
        
        name (str): The namespace desired to be registered in the network.
        
        port (int: 1-65535): The port in which your device will communicate with the network.
        
        ttl (int|float): The Time To Live of your register.
        
    Returns:
        protocol_field (dict): The request based on the protocol.
        
        None (None): If an error occur.
    """
    try:
        action_type = action_type.upper()
        protocol_field = {}
        
        if action_type == 'REGISTER':
            protocol_field["type"] = action_type
            protocol_field["namespace"] = str(namespace)
            protocol_field["name"] = str(name)
            protocol_field["port"] = int(port)
            # This defines that 'ttl' will only will be written to the protocol if it is a valid number
            if isinstance(ttl,(int,float)):
                protocol_field["ttl"] = ttl
            
            return protocol_field
        
        elif action_type == 'DISCOVER':
            protocol_field["type"] = action_type
           
            # If it is None or the user types '', it will return to the default value.
            # The option with the empty string was put because the rendevouz don't accept it as a valid namespace.
            if (bool(namespace)):
                namespace = str(namespace)
                protocol_field["namespace"] = namespace

            return protocol_field

        elif action_type == 'UNREGISTER':
            protocol_field["type"] = action_type
            protocol_field["namespace"] = str(namespace)
            protocol_field["name"] = str(name)
            protocol_field["port"] = int(port)
 
            return protocol_field
        
        else:
            print("There is no such action available to communicate with the rendevouz. (Maybe you misspelled it?)")
            return None

    except TypeError:
        print("Some argument is of the incorrect type.")
        return None
    else:
        return None

# Registers the user in the network, using the Rendevouz IP address.
# As per now, the ttl is defined in the config file. If there are no 'ttl' parameter, that
# will make the rendevouz define ttl=7200s (2h).

async def register():
    
    # Get the configuration
    config_dict = cfg.getConfig()
    # Get the address and port to the server/rendevouz
    connIP = config_dict["rdv_name"]
    connPort = config_dict["rdv_port"]
    
    # Verifies if it is a valid address and port value
    if not (isinstance(connIP, str) and isinstance(connPort,int)):
        if connPort < 1 and connPort > 65535:
            print("The port chosen for the rendevouz is out of range. It must be between 1 and 65535.")
            return None
        else:
            print("The IP address of the rendevouz must be a string.")
            return None
    
    # Defines the variables used in the protocol
    namespace, name, port, ttl = (config_dict.get("namespace"), config_dict.get("name"), config_dict.get("port"), config_dict.get("ttl"))
    try:
        # Connects to the server/rendevouz
        reader, writer = await asyncio.open_connection(connIP,connPort)
        # Defines the protocol request to be sent
        register_dict = define_rdv_protocol('REGISTER', namespace, name, port, ttl)
        request_string = json.dumps(register_dict) + '\n'
        
        # Writes to buffer and send the request
        writer.write(request_string.encode())
        await writer.drain()
        
        # Wait until receives a response
        response_string = await reader.read(32768)
        response_dict = json.loads(response_string.decode('utf-8'))
        
        # Verifies if the server returned an "ERROR status"
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
            print("You have been registered in the network.")
        
        # Close the connection and returns the response of the server/rendevouz
        writer.close()
        await writer.wait_closed()
        return response_dict
    
    # Raised when the server refuses the connection
    except ConnectionRefusedError:
        print("The server refused to connect.")
        return None
    # Raised when the socket could not connect to the IP address in a given time
    except TimeoutError:
        print("Could not connect to server due to timed out attempt.")
        return None
    # Raised when there are possibly no routes to that server
    except OSError:
        print("There was a network issue and it was not possible to connect to the server.")
        return None
    # Raised when the user or the program interrupts the execution with a signal
    except (InterruptedError, KeyboardInterrupt):
        print("The program was interrupted by a signal (like Ctrl+C).")
        return None

async def discover(namespace=None):

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
    
    try:
        reader, writer = await asyncio.open_connection(connIP,connPort)
        discover_dict = define_rdv_protocol('DISCOVER',namespace)
        request_string = json.dumps(discover_dict) + '\n'

        writer.write(request_string.encode())
        await writer.drain()
        
        response_string = await reader.read(32768)
        response_dict = json.loads(response_string.decode('utf-8'))
        
        if response_dict["status"] == "ERROR":
            match response_dict["message"]:
                case "peer_not_registered":
                    print("You are not registered in the network!")
                    return {}
                case "bad_namespace":
                    print("The namespace is invalid. It must be a string between 1 and 64 characters.")
                    return None
                
        elif (response_dict["status"] == "OK" and len(response_dict["peers"]) == 0):
            print("There is no peer with the namespace that you entered.")
            return None

        writer.close()
        await writer.wait_closed()
        return response_dict
    
    except ConnectionRefusedError:
        print("The server refused to connect.")
        return None
    except TimeoutError:
        print("Could not connect to server due to timed out attempt.")
        return None
    except OSError:
        print("There was a network issue and it was not possible to connect to the server.")
        return None
    except TypeError:
        print("A type error occurred when a parameter was passed to a function.")
        try:
            writer.close()
            await writer.wait_closed()
            return None
        except:
            return None  
    except (InterruptedError, KeyboardInterrupt):
        print("The program was interrupted by a signal (like Ctrl+C).")
        try:
            writer.close()
            await writer.wait_closed()
            return None
        except:
            return None
        
async def unregister():
    
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
    
    namespace, name, port = (config_dict.get("namespace"), config_dict.get("name"), config_dict.get("port"))
    try:
        reader, writer = await asyncio.open_connection(connIP,connPort)
        register_dict = define_rdv_protocol('UNREGISTER', namespace, name, port)
        request_string = json.dumps(register_dict) + '\n'

        writer.write(request_string.encode())
        await writer.drain()
        
        response_string = await reader.read(32768)
        response_dict = json.loads(response_string.decode('utf-8'))
        
        if response_dict["status"] == "ERROR":
            match response_dict["message"]:
                case "bad_name":
                    print("The name is invalid. It must be a string between 1 and 64 characters.")
                    return None
                case "bad_namespace":
                    print("The namespace is invalid. It must be a string between 1 and 64 characters.")
                    return None
                case "bad_port":
                    print("The port must be an integer between 1 and 65535.")
                    return None
        else:
            print("You have been successfully unregistered.")
        
        writer.close()
        await writer.wait_closed()
        return response_dict
    
    except ConnectionRefusedError:
        print("The server refused to connect.")
        return None
    except TimeoutError:
        print("Could not connect to server due to timed out attempt.")
        return None
    except OSError:
        print("There was a network issue and it was not possible to connect to the server.")
        return None
    except (InterruptedError, KeyboardInterrupt):
        print("The program was interrupted by a signal (like Ctrl+C).")
        return None

if __name__ == '__main__':
    asyncio.run(register())
    asyncio.run(discover())
    time.sleep(5)
    asyncio.run(unregister())