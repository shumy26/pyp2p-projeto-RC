import logging
import asyncio
import project_config as cfg

logger = logging.getLogger("PeerTableManager")

class Peer():
    '''Peers information inside a class (NAME, NAMESPACE, IP, PORT, direction (Inbound, Outbound ou STALE), number of remaining attempts to connect, connection objects (a 2-tuple of reader and writer) and its rtt to you)'''
    def __init__(self, NAME='',NAMESPACE='', IP='', PORT=65535):
        self.__name = NAME
        self.__namespace = NAMESPACE
        self.__ip = IP
        self.__port = PORT
        # Can be 'Inbound', 'Outbound','' and 'STALE'. If it is '', it means it is not connected and it is the first time it is appearing. If it is 'STALE', it is not connected, but the host has already tried 'max_reconnect_attempts' times.
        self.__direction = ''
        self.__attempts = cfg.getConfig().get('max_reconnect_attempts',6)
        self.__connection = None
        self.__lock = asyncio.Lock()
        self.__rtt = -1

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
                case _:
                    raise AttributeError

        except AttributeError:
            logger.debug(f"A wrong attribute was passed to the method 'get' of {self}. Attribute: {attribute}.")
            return None
        
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
                case _:
                    raise AttributeError

        except TypeError:
            logger.debug(f"Something went wrong when trying to update {attribute} of {self}.")
            return None
        except AttributeError:
            logger.debug(f"A wrong attribute was passed to the method 'set' of {self}.")
            return None
    
    def decrease_attempts(self,order=1):
        self.__attempts = self.__attempts - order
        return self.__attempts
    
    def reset_attempts(self):
        self.__attempts = cfg.getConfig().get('max_reconnect_attempts')

    def __eq__(self, other):
        if not isinstance(other,Peer):
            return NotImplemented

        return (self.__ip == other.get('ip') and self.__port == other.get('port'))

    def __hash__(self):
        return hash((self.__ip, self.__port))
    
    def __repr__(self):
        return f"Peer[{self.__name}@{self.__namespace}]"
    
    def __str__(self):
        return f"Peer[{self.__name}@{self.__namespace}]"
    
    def __iter__(self):
        return self

class PeerTable():
    def __init__(self):
        self.__connected:set[Peer] = set()
        self.__not_connected:set[Peer] = set()

    def get(self, state=''):

        if state.lower() == 'connected' or state.lower() == 'conn':
            return self.__connected
        
        if state.lower() == 'not_connected' or state.lower() == 'nconn':
            return self.__not_connected

        else:
            logger.debug(f"An invalid parameter was passed to method 'get'.")
            return None
    
    def set(self, new_set:set, state=''):

        if state.lower() == 'connected' or state.lower() == 'conn':
            self.__connected = new_set
            return self.__connected
        
        if state.lower() == 'not_connected' or state.lower() == 'nconn':
            self.__not_connected = new_set
            return self.__not_connected
        
        else:
            logger.debug(f"An invalid parameter was passed to method 'set'.")
            return None

    # Moves from 'not_connected' state to 'connected' state
    def update(self, peer:Peer):
        try:
            self.__connected.add(peer)
            self.__not_connected.remove(peer)
        finally:
            pass
    
    def degrade(self, peer:Peer):
        try:
            self.__connected.remove(peer)
            self.__not_connected.add(peer)
        finally:
            pass
        

    def __contains__(self,peer):
        return (peer in self.__connected or peer in self.__not_connected)
    
    def __repr__(self):
        return f"'Connected':{[peer for peer in self.__connected]},'Not Connected:{[peer for peer in self.__not_connected]}'"

    def __str__(self):
        return f"'Connected':{[peer for peer in self.__connected]},'Not Connected:{[peer for peer in self.__not_connected]}'"

peer_table = PeerTable()
peer_table_lock = asyncio.Lock()

if __name__ == '__main__':
    peer1 = Peer(IP=cfg.getConfig().get('ip'),PORT=cfg.getConfig().get('port'))
    print(peer1.get('ip'), peer1.get('port'))
    print(peer1.get('lock'), peer1.get('name'), peer1.get('namespace'))
    pass
    # table = PeerTable()
    # for i in range(10):
    #     peer = Peer(IP=f'{i}', PORT=i)
    #     table.get('nconn').add(peer)

    # not_connected = table.get('nconn')
    # for peer in not_connected:
    #     if peer.get('ip') == '3':
    #         peer.set('direction','STALE')
    #         break
    
    # print(table.get('nconn'), len(table.get('nconn')))
    # peer = table.get('nconn').pop()
    # print(table.get('nconn').discard(peer))
    # print(table.get('nconn'), len(table.get('nconn')))

    # for peer in not_connected:
    #     print(peer.get('ip'),peer.get('dir'))
