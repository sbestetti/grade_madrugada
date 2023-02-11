# Sistema
import sys

# Ferramentas
import logging

# Internos
import config as cfg


def get_logger():
    logging.basicConfig(
        filename=cfg.log_config['file'],
        encoding=cfg.log_config['encoding'],
        format=cfg.log_config['format'],
        level=cfg.log_config['level']
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    return logging
