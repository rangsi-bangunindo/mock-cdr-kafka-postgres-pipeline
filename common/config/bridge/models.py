from typing import Optional

from pydantic import BaseModel


class KafkaSettings(BaseModel):
    bootstrap_servers: str
    topic_raw: str
    topic_flat: str
    topic_error: str
    security_protocol: str = "PLAINTEXT"
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int = 10000


class ProducerTuning(BaseModel):
    linger_ms: int = 5
    batch_bytes: int = 32768
    compression: str = "lz4"
    retries: int = 10
    enable_idempotence: bool = True
    max_in_flight: int = 5


class SparkSettings(BaseModel):
    checkpoint_dir: str
    watermark_minutes: int = 10
    window_minutes: int = 5
    fail_on_data_loss: bool = True


class PostgresSettings(BaseModel):
    host: str
    port: int = 5432
    database: str
    db_schema: str = "public"
    user: str
    password: str
    jdbc_options: str = "stringtype=unspecified"


class LoggingSettings(BaseModel):
    config_path: str


class SparkConfig(BaseModel):
    kafka: KafkaSettings
    spark: SparkSettings
    postgres: PostgresSettings
    logging: Optional[LoggingSettings] = None


class GeneratorSettings(BaseModel):
    rate_per_sec: int = 10
    seed: int = 42
