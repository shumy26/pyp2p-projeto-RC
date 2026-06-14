# This is just a test program. It isn't intended to go to the main branch.
# It initiates 'config.json', connects to the rendevouz and  constantly fetches data with the discover.
# Then it create the PeerTable and updates it.
import rendevouz_connection as rdv
import project_config as cfg
import state
import teste_parallelism as parallel

cfg.write_config()
parallel.parallel_test2(10,3)