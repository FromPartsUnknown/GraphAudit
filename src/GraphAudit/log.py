import logging
import sys

class InfoOnlyFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO

def log_init(name, level=logging.INFO, filename="errors.log"):
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    handler   = logging.FileHandler(filename, mode='a')
    formatter = logging.Formatter('{module} - {asctime} - {levelname} - {message}', style='{')
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.INFO)
    handler.addFilter(InfoOnlyFilter())
    formatter = logging.Formatter('{message}', style='{')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(level)
    logger.propagate = False
    return logger 