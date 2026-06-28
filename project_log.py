import os
import pathlib
import logging

logger = logging.getLogger(__name__)

def create_logs():
    try:
        cwd = os.path.dirname(os.path.realpath(__file__))
        os.chdir(cwd)
        logs_dir_path = os.path.join(cwd,"logs")
        logs_dir = pathlib.Path(logs_dir_path)
        logs_dir.mkdir(parents=True)

    except FileExistsError:
        pass
