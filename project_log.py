import os
import pathlib
import logging
# import logging.handlers
# class ColorLogFormatter(logging.Formatter):
    
#     styles = {
#         'BOLD': "\033[1m",
#         'ITALIC': "\033[3m",
#         'UNDERLINE': "\033[4m",
#         'BLINK': "\033[5m",
#         'RESET': "\033[0m" 
#         }
    
#     fg_colors = {
#         'BLACK': "\033[30m",
#         'RED': "\033[31m",
#         'GREEN': "\033[32m",
#         'YELLOW': "\033[33m",
#         'BLUE': "\033[34m",
#         'MAGENTA': "\033[35m",
#         'CYAN': "\033[36m",
#         'WHITE': "\033[37m",
#         'ORANGE': "\033[38;5;202m"
#         }
    
#     bg_colors = {
#         'BLACK': "\033[40m",
#         'RED': "\033[41m",
#         'GREEN': "\033[42m",
#         'YELLOW': "\033[43m",
#         'BLUE': "\033[44m",
#         'MAGENTA': "\033[45m",
#         'CYAN': "\033[46m",
#         'WHITE': "\033[47m",
#         'ORANGE': "\033[38;5;202m"
#         }

#     levels = {
#         logging.DEBUG: fg_colors.get('BLUE'),
#         logging.INFO: fg_colors.get('GREEN'),
#         logging.WARNING: fg_colors.get('YELLOW'),
#         logging.ERROR: fg_colors.get('RED'),
#         logging.CRITICAL: bg_colors.get('RED', '') + fg_colors.get('WHITE', '')
#         }
    
#     def format(self, record):
        
#         color = self.levels.get(record.levelno, '')
        
#         log_fmt = f"{color}{self.styles.get('BOLD',"")}[%(levelname)s]{self.styles.get('RESET',"")} {color}[%(asctime)s]{self.styles.get('RESET',"")} %(message)s{self.styles.get('RESET',"")}"
        
#         formatter = logging.Formatter(log_fmt, datefmt="%a %d/%m/%Y %H:%M:%S")
        
#         return formatter.format(record)

logger = logging.getLogger(__name__)

# logger.setLevel(logging.DEBUG)

# console_handler = logging.StreamHandler()
# console_handler.setFormatter(ColorLogFormatter())
# console_handler.setLevel(logging.DEBUG)
# logger.addHandler(console_handler)

# #logging.basicConfig(format='[%(levelname)s][%(asctime)s] %(message)s',datefmt='%a %d/%m/%Y %H:%M:%S', level=logging.DEBUG)

# file_handler = logging.handlers.RotatingFileHandler(filename='rendevouz.log',maxBytes=1*1024,backupCount=3,encoding='utf-8',mode='a')
# file_handler.setLevel(logging.DEBUG)
# file_handler.setFormatter(logging.Formatter('[%(levelname)s][%(asctime)s] %(message)s',datefmt='%a %d/%m/%Y %H:%M:%S'))
# logger.addHandler(file_handler)

# logger.debug('This message should go to the log file')
# logger.info('So should this')
# logger.warning('And this, too')
# logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
# logger.critical('Teste de criticidadee!')


def create_logs():
    try:
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)
        logs_dir_path = os.path.join(cwd,"logs")
        logs_dir = pathlib.Path(logs_dir_path)
        logs_dir.mkdir(parents=True)

    except FileExistsError:
        pass
