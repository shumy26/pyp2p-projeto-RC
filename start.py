# This is just a test program. It isn't intended to go to the main branch.
# It initiates 'config.json', connects to the rendevouz and  constantly fetches data with the discover.
# Then it create the PeerTable and updates it.
import logging
import asyncio
import rendevouz_connection as rdv
import project_config as cfg
import teste_parallelism2 as parallel

logger = logging.basicConfig(level=logging.INFO, encoding='utf-8')

cfg.write_config()
try:
    asyncio.run(parallel.main())
    asyncio.run(rdv.unregister())
except KeyboardInterrupt:
    asyncio.run(rdv.unregister())
    print('Ctrl-C')