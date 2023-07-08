import sys
import logging


def logger(name):
    logging.basicConfig(filename='GW_FRB_listener.log',
                        level=logging.INFO,
                        format = "%(asctime)s - %(name)s - %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z"
                        )
    logger = logging.getLogger(name)
    logger.handlers.append(logging.StreamHandler(sys.stdout))
    return logger