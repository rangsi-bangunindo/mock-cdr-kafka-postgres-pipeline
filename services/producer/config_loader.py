import os

import yaml
from pydantic import BaseModel

from common.config.bridge.models import KafkaSettings, ProducerTuning

CONFIG_PATH = os.getenv("PRODUCER_CONFIG", "/opt/app/config/producer.yml")


class ProducerConfig(BaseModel):
    kafka: KafkaSettings
    tuning: ProducerTuning
    logging: dict


def load_config() -> ProducerConfig:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Missing config file at {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    return ProducerConfig(**cfg)
