# This program is a just a test! 

import concurrent.futures as concf
import threading
# import os
import queue
import time
import rendevouz_connection as rdv
# import project_config as cfg
import state
# import asyncio

def loop_discover(local_data:queue.Queue, stop_signal:threading.Event, time2discover:(int|float)):
    while not stop_signal.is_set():
        d1 = rdv.discover()
        # d1.result() == discover_response
        d2 = state.state(d1)
        print(d2)
        if d1 == {}:
            local_data.put(d1)
            time.sleep(1)
            continue
        time.sleep(time2discover - 0.4)
    return None
        
    
def loop_register(ttl, discover_data:queue.Queue, stop_signal:threading.Event):
    while not stop_signal.is_set():
        if not discover_data.empty():
            discover_data.get()
            rdv.register(ttl)
    return None
    
def exit_test(stop_signal:threading.Event):
    exit_response = ''
    while exit_response != 'exit':
        exit_response = input()
    stop_signal.set()
    return None
    
def stop_everything2(stop_signal):
    while True:
        if stop_signal.is_set():
            time.sleep(3)
            rdv.unregister()
            break
    return None
    
def parallel_test2(ttl,time2discover:int):
    data = queue.Queue(maxsize=1)
    register_response = rdv.register(ttl)
    if register_response == None:
        return None
    stop_signal = threading.Event()
    with concf.ThreadPoolExecutor(max_workers=10) as executor:
        executor.submit(loop_discover,data,stop_signal,time2discover)
        executor.submit(loop_register,ttl,data,stop_signal)
        executor.submit(stop_everything2, stop_signal)
        executor.submit(exit_test,stop_signal)
    return None

if __name__ == '__main__':
    # Teste de execução de 'Register' e 'Discover' em paralelo.
    # Ao digitar 'exit' no terminal, o programa manda um 'Unregister' e sai.
    parallel_test2(10,3)
            
                
            
        

        
