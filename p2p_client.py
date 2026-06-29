import asyncio
import state
import logging

import project_config as cfg
import rendevouz_connection as rdv
import peer_connection as pc
import keep_alive as ka
from message_router import _local_peer_id, _peer_id as _peer_id_from_peer_table, build_bye
from peer_table import *
logger = logging.getLogger("P2PHandler")

# -------- Exit Functions ----------

async def terminate_peers():
    for peer in peer_table.get('conn'):
        disconnected = await disconnect_peer(peer, *peer.get('conn'))
        if not disconnected:
            try:
                _, writer = peer.get('conn')
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
            except:
                continue
    
    peer_table.get('conn').clear()
    peer_table.get('nconn').clear()

    return True

async def disconnect_peer(peer:Peer, writer:asyncio.StreamWriter, reader:asyncio.StreamReader):

    my_id = _local_peer_id()
    peer_id = _peer_id_from_peer_table(peer)
    msg_size = cfg.getConfig().get('msg_size',32768)
    lock = peer.get('lock')
    try:
        bye_request = (json.dumps(build_bye(my_id, peer_id)) + '\n').encode()
        
        async with lock:
            writer.write(bye_request)
            await writer.drain()

            try:
                response = await asyncio.wait_for(reader.read(msg_size), timeout=cfg.getConfig().get('timeout',5))
                response_dict = json.loads(response.decode())
                if response_dict.get('type') == 'BYE_OK':
                    pass
            except (asyncio.TimeoutError, json.JSONDecodeError, AttributeError):
                pass

            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            peer.set('rtt',-1)
            peer.set('direction','STALE')
            peer.set('conn', None)
            peer_table.degrade(peer)
        return True
    except:
        return False

# -------- Loops ----------

async def loop_register():
    # Defines the ttl
    ttl = cfg.getConfig().get("ttl")
    ttl = 7200 if (ttl == None) else abs(ttl)
    try:
        while True:
            await asyncio.sleep(ttl - 0.5)
            await rdv.register()
    except asyncio.CancelledError:
        return None

async def loop_discover():
    time2discover = cfg.getConfig().get('discover_interval',10) # Gets the interval to discover (Default value = 10)
    try:
        while True:
            d1 = await rdv.discover()
            d2 = state.state(d1)
            logger.debug(f"Peers discovered: {d2}")
            await asyncio.sleep(time2discover - 0.4)
    except asyncio.CancelledError:
        return None

async def ainput(prompt: str = ""):
    return await asyncio.to_thread(input, prompt)

async def exit_control():
    try:
        while True:
            user_input = await ainput()
            if user_input == '/quit':
                curr_task = asyncio.current_task()
                tasks = [t for t in asyncio.all_tasks() if t is not curr_task]
                for t in tasks:
                    t.cancel()
                
                terminated = await pc.terminate_peers()

                return None
    except:
        return None

async def _wait_for_state():
    interval = cfg.getConfig().get('discover_interval',10)
    await asyncio.sleep(2*interval) # Wait 2 loops to try to connect to peers

    try:
        tasks = (pc.start_outbound(), pc.start_inbound())
        res = await asyncio.gather(*tasks,return_exceptions=True)
    except:
        return None

async def _wait_and_ping(interval = cfg.getConfig().get('discover_interval')):
    await asyncio.sleep(3*interval)
    try:
        res = await asyncio.gather(ka.start_PingPong(),return_exceptions=True)
    except:
        return None

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(loop_register())
            tg.create_task(loop_discover())
            tg.create_task(_wait_for_state())
            tg.create_task(_wait_and_ping())
            tg.create_task(exit_control())
    except:
        return None
        
if __name__ == '__main__':
    try:
        asyncio.run(main())
        asyncio.run(rdv.unregister())
    except KeyboardInterrupt:
        asyncio.run(rdv.unregister())
        print('Ctrl-C')