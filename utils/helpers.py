import logging
from sqlalchemy import text
import yaml
import argparse

# Logger
def create_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(name)
    return logger

def load_cfg(cfg_file):
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)
    return cfg

def parse_args(description: str, help: str):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', type=str, required=True,
                       help=help)
    return parser.parse_args()
