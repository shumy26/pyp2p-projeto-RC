import logging
import asyncio
import project_config as cfg

logger = logging.getLogger("PeerTableManager")

# Implementation of class Peer. It instantiates and stores all the information needed for each peer in the network, being connected or not.

class Peer():
    '''Peers information inside a class (NAME, NAMESPACE, IP, PORT, direction (Inbound, Outbound ou STALE), number of remaining attempts to connect, connection objects (a 2-tuple of reader and writer) and its rtt to you)'''
    def __init__(self, NAME='',NAMESPACE='', IP='', PORT=0):
        self.__name = NAME
        self.__namespace = NAMESPACE
        self.__ip = IP
        self.__port = PORT
        # Direction can be 'Inbound', 'Outbound','' and 'STALE'. If it is '', it means it is not connected and 
        # it is the first time it is appearing. If it is 'STALE', it is not connected, 
        # but the host has already tried 'max_reconnect_attempts' times.
        self.__direction = ''
        self.__attempts = cfg.getConfig().get('max_reconnect_attempts',6) # Maximum amout of reconnect attempts allowed for each peer.
        self.__connection = None  # (asyncio.StreamReader, asyncio.StreamWriter) pair for each peer. Needed to define how each peer will handle messages to be sent and received. 
        self.__lock = asyncio.Lock()  # asyncio.Lock: a lock object to each peer, in order to use and modify its resources and info without interfering in other code that may use the same resource or info simultaneously.
        self.__rtt = -1  # if RTT is -1, it is not connected (it takes an infinite time to get to the peer).
        self.__ack_future = None  # asyncio.Future: Needed to timeout an ACK after a SEND .
        self.__pong_future = None  # asyncio.Future: Needed to timeout a PING/PONG and measure the RTT.
        self.__bye_future = None  # asyncio.Future: Needed to timeout the time to receive a BYE_OK from a peer.

    # GET method - gets the attribute.
    def get(self,attribute:str=''):
        try:
            match attribute.lower():
                case 'name':
                    return self.__name
                case 'namespace':
                    return self.__namespace
                case 'ip':
                    return self.__ip
                case 'port':
                    return self.__port
                case 'direction' | 'dir':
                    return self.__direction
                case 'attempts':
                    return self.__attempts
                case 'connection' | 'conn':
                    return self.__connection
                case 'rtt':
                    return self.__rtt
                case 'lock':
                    return self.__lock
                case 'ack':
                    return self.__ack_future
                case 'pong':
                    return self.__pong_future
                case 'bye':
                    return self.__bye_future
                case _:
                    raise AttributeError

        except AttributeError:
            logger.debug(f"A wrong attribute was passed to the method 'get' of {self}. Attribute: {attribute}.")
            return None
    
    # SET method - changes the respective attribute.
    def set(self,attribute:str,new_value):
        try:
            match attribute.lower():
                case 'name':
                    self.__name = new_value
                    return self.__name
                case 'namespace':
                    self.__namespace = new_value
                    return self.__namespace
                case 'ip':
                    self.__ip = new_value
                    return self.__ip
                case 'port':
                    self.__port = new_value
                    return self.__port
                case 'direction' | 'dir':
                    self.__direction = new_value
                    return self.__direction
                case 'attempts':
                    self.__attempts = new_value
                    return self.__attempts
                case 'connection' | 'conn':
                    self.__connection = new_value
                    return self.__connection
                case 'rtt':
                    self.__rtt = round(float(new_value),2)
                    return self.__rtt
                case 'ack':
                    self.__ack_future = new_value
                    return self.__ack_future
                case 'pong':
                    self.__pong_future = new_value
                    return self.__pong_future
                case 'bye':
                    self.__bye_future = new_value
                    return self.__bye_future
                case _:
                    raise AttributeError

        except TypeError:
            logger.debug(f"Something went wrong when trying to update {attribute} of {self}.")
            return None
        except AttributeError:
            logger.debug(f"A wrong attribute was passed to the method 'set' of {self}.")
            return None
    
    # Decrease automatically the attempts of reconnection for the peer.
    def decrease_attempts(self,order=1):
        self.__attempts = self.__attempts - order
        return self.__attempts
    
    # Reset peer attempts to 'max_reconnect_attempts'
    def reset_attempts(self):
        self.__attempts = cfg.getConfig().get('max_reconnect_attempts')

    # Those methods are implemented in order to be possible to use it to compare to other instances and to build set-like objects with it.

    # Defines what it means to two peers to be equal.
    def __eq__(self, other):
        if not isinstance(other,Peer):
            return NotImplemented

        return (self.__ip == other.get('ip') and self.__name == other.get('name') and self.__namespace == other.get('namespace'))

    # Defines a unique 'hash' identifier to each peer.
    def __hash__(self):
        return hash((self.__ip, self.__name, self.__namespace))
    
    # Defines a string representation to Peer.

    def __repr__(self):
        return f"Peer[{self.__name}@{self.__namespace}]"
    
    def __str__(self):
        return f"Peer[{self.__name}@{self.__namespace}]"
    
    # Defines how to be iterated over.
    def __iter__(self):
        return self


# The class PeerTable defines how the variable peer_table will hold information of each peer in the network. It has 2 attributes:
# Not-Connected: a set representing all the peers not connected to you;
# Connected: a set representing all the peers connected to you.
# It is updated as necessary in the code.

class PeerTable():
    def __init__(self):
        self.__connected:set[Peer] = set()
        self.__not_connected:set[Peer] = set()

    # GET method - gets the state of each attribute.
    def get(self, state=''):

        if state.lower() == 'connected' or state.lower() == 'conn':
            return self.__connected
        
        if state.lower() == 'not_connected' or state.lower() == 'nconn':
            return self.__not_connected

        else:
            logger.debug(f"An invalid parameter was passed to method 'get'.")
            return None
    
    # SET method - Changes the attribute to a new value.
    def set(self, new_set:set, state=''):

        if state.lower() == 'connected' or state.lower() == 'conn':
            self.__connected = new_set
            return self.__connected
        
        if state.lower() == 'not_connected' or state.lower() == 'nconn':
            for peer in new_set:
                if peer in self.__not_connected:
                    pass
                else:
                    self.__not_connected.add(peer)
            # self.__not_connected = new_set
            return self.__not_connected
        
        else:
            logger.debug(f"An invalid parameter was passed to method 'set'.")
            return None

    # Moves from 'not_connected' state to 'connected' state.
    def update(self, peer:Peer):
        try:
            self.__connected.add(peer)
            self.__not_connected.remove(peer)
        finally:
            pass
    
    # Moves from 'connected' state to 'not_connected' state.
    def degrade(self, peer:Peer):
        try:
            self.__connected.remove(peer)
            self.__not_connected.add(peer)
        finally:
            pass
    
    # For the context of use of this function, ID is a 3-tuple: (IP, Name, Namespace)
    # Verifies if it is connected.
    def isPeerIDConn(self, peer:Peer):
        for conn_peer in self.__connected:
            if conn_peer == peer:
                logger.debug(f"From State Conn, Port: {peer.get("port")}")
                return (True, -1)
        
        return (False, None)
    
    # Verifies if it is not connected.
    def isPeerIDNotConn(self, peer:Peer):
        for nconn_peer in self.__not_connected:
            if nconn_peer == peer:
                logger.debug(f"From State NConn, Port: {nconn_peer.get("port")}")
                return (True, nconn_peer.get("port"))
    
        return (False, None)


    # Used to iterate over the object.
    def __contains__(self,peer):
        return (peer in self.__connected or peer in self.__not_connected)
    
    # Used to represent the object in DEBUG logs.
    def __repr__(self):
        return f"'Connected':{[peer for peer in self.__connected]},'Not Connected':{[peer for peer in self.__not_connected]}"

    def __str__(self):
        return f"'Connected':{[peer for peer in self.__connected]},'Not Connected':{[peer for peer in self.__not_connected]}"
    
    def __iter__(self):
        for peer in self.__connected.union(self.__not_connected):
            yield peer

# Initialize the peer_table as empty.
# Initialize a lock object too, that will be necessary to change it and to get its state as the asynchronous code is running.
peer_table = PeerTable()
peer_table_lock = asyncio.Lock()
