# ------------------------------------------------------------------------- #
# -------------------- Projeto ChatP2P : Grupo 1 -------------------------- #
# Integrantes:                                                              #
# - Yuri Shumyatsky 231012826                                               #
# - Vinicius de Melo Moraes 231036274                                       #
# - André Ricardo de Faria Santos 231025889                                 #
# ------------------------------------------------------------------------- #
import logging
import logging.handlers
import os
import asyncio

import peer_connection as pc
import peer_table as pt
import project_config as cfg
import project_log as plog
import rendevouz_connection as rdv
import state
import message_router as router
import teste_parallelism2 as parallel
import p2p_client as p2p
import cli as cli

# ------ Formatting logs ------- #

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
        
        log_fmt = f"{color}{self.styles.get('BOLD',"")}[%(name)s]{self.styles.get('RESET',"")} {color}[%(asctime)s]{self.styles.get('RESET',"")} %(message)s{self.styles.get('RESET',"")}"
        
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
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8',
    mode='a'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('[%(levelname)s][%(asctime)s] %(name)s.%(funcName)s: %(message)s', datefmt='%a %d/%m/%Y %H:%M:%S'))
logger.addHandler(file_handler)

try:
    # `cli.main()` already runs the asyncio loop internally; call it directly.
    cli.main()
    asyncio.run(rdv.unregister())
except KeyboardInterrupt:
    try:
        asyncio.run(rdv.unregister())
    except Exception:
        pass
    print('Ctrl-C')