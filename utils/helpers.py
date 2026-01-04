import json
import logging
from pathlib import Path
from sqlalchemy import text
import yaml
import argparse

# Logger
def create_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(name)
    return logger

def load_cfg(cfg_file: str):
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / cfg_file

    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)


def parse_config_args():
    """Standard config parser for all Spark apps"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    args = parser.parse_args()
    return json.loads(args.config)