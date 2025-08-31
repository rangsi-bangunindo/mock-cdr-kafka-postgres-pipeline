import os

import yaml

from common.config.bridge.models import SparkConfig

CONFIG_PATH = os.getenv("SPARK_CONFIG", "/opt/app/config/spark.yml")


def load_config() -> SparkConfig:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Missing config file at {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    return SparkConfig(**cfg)
