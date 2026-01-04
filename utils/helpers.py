import json
import logging
from sqlalchemy import text
import yaml
import argparse
<<<<<<< HEAD
from pathlib import Path
=======
>>>>>>> origin/main

# Logger
def create_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(name)
    return logger

<<<<<<< HEAD
def load_cfg(cfg_file: str):
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / cfg_file

    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

=======
def load_cfg(cfg_file):
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)
    return cfg
>>>>>>> origin/main

def parse_config_args():
    """Standard config parser for all Spark apps"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    args = parser.parse_args()
    return json.loads(args.config)