import asyncio
import state
import project_config as cfg
import rendevouz_connection as rdv
import peer_connection as pc
import keep_alive as ka

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

async def loop_discover(time2discover:(int|float)=cfg.getConfig().get('discover_interval')):
    try:
        while True:
            d1 = await rdv.discover()
            d2 = state.state(d1)
            print(d2)
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

async def wait_for_state(interval = cfg.getConfig().get('discover_interval')):
    await asyncio.sleep(interval)
    try:
        tasks = (pc.start_outbound(), pc.start_inbound())
        res = await asyncio.gather(*tasks,return_exceptions=True)
        # async with asyncio.TaskGroup() as tg:
        #     tg.create_task(pc.start_outbound())
        #     tg.create_task(pc.start_inbound())

    except:
        return None

async def wait_and_ping(interval = cfg.getConfig().get('discover_interval')):
    await asyncio.sleep(3*interval)
    try:
        res = await asyncio.gather(ka.start_PingPong(),return_exceptions=True)
        # async with asyncio.TaskGroup() as tg:
        #     tg.create_task(ka.start_PingPong())
    except:
        return None
async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(loop_register())
            tg.create_task(loop_discover())
            tg.create_task(wait_for_state())
            tg.create_task(wait_and_ping())
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