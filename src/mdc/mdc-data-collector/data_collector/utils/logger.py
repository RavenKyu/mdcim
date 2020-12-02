import logging


def get_logger(name, level='debug'):
    log_format = '%(asctime)s | %(levelname)7s | %(name)20s | %(message)s ' \
                 '| %(pathname)s:%(lineno)d'
    logging.basicConfig(format=log_format)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    return logger
