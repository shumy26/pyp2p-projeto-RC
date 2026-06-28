# This is just a test program. It isn't intended to go to the main branch.
# It initiates 'config.json', connects to the rendevouz and constantly fetches data with the discover.
# Then it create the PeerTable and updates it.
import logging
import logging.handlers
import asyncio
import os
import rendevouz_connection as rdv
import project_config as cfg
import teste_parallelism2 as parallel
import project_log as plog
import hello

plog.create_logs()


class ColorLogFormatter(logging.Formatter):
    styles = {
        'BOLD': "\033[1m",
        'ITALIC': "\033[3m",
        'UNDERLINE': "\033[4m",
        'BLINK': "\033[5m",
        'RESET': "\033[0m"
    }

    fg_colors = {
        'BLACK': "\033[30m",
        'RED': "\033[31m",
        'GREEN': "\033[32m",
        'YELLOW': "\033[33m",
        'BLUE': "\033[34m",
        'MAGENTA': "\033[35m",
        'CYAN': "\033[36m",
        'WHITE': "\033[37m",
        'ORANGE': "\033[38;5;202m"
    }

    bg_colors = {
        'BLACK': "\033[40m",
        'RED': "\033[41m",
        'GREEN': "\033[42m",
        'YELLOW': "\033[43m",
        'BLUE': "\033[44m",
        'MAGENTA': "\033[45m",
        'CYAN': "\033[46m",
        'WHITE': "\033[47m",
        'ORANGE': "\033[38;5;202m"
    }

    levels = {
        logging.DEBUG: fg_colors.get('BLUE'),
        logging.INFO: fg_colors.get('GREEN'),
        logging.WARNING: fg_colors.get('YELLOW'),
        logging.ERROR: fg_colors.get('RED'),
        logging.CRITICAL: bg_colors.get('RED', '') + fg_colors.get('WHITE', '')
    }

    def format(self, record):
        color = self.levels.get(record.levelno, '')
        log_fmt = f"{color}{self.styles.get('BOLD', '')}[%(levelname)s]{self.styles.get('RESET', '')} {color}[%(asctime)s]{self.styles.get('RESET', '')} %(message)s{self.styles.get('RESET', '')}"
        formatter = logging.Formatter(log_fmt, datefmt="%a %d/%m/%Y %H:%M:%S")
        return formatter.format(record)


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorLogFormatter())
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

file_handler = logging.handlers.RotatingFileHandler(
    filename=os.path.join('logs', 'p2p_app.log'),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8',
    mode='a'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('[%(levelname)s][%(asctime)s] %(name)s.%(funcName)s: %(message)s', datefmt='%a %d/%m/%Y %H:%M:%S'))
logger.addHandler(file_handler)

cfg.write_config()

try:
    asyncio.run(parallel.main())
    asyncio.run(rdv.unregister())
    asyncio.run(hello.shutdown_hello())
except KeyboardInterrupt:
    asyncio.run(rdv.unregister())
    asyncio.run(hello.shutdown_hello())
    print('Ctrl-C')