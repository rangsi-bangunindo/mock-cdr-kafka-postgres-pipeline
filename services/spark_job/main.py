import json
import logging
import logging.config
import os
import signal
import sys
import time
from typing import List

import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from services.spark_job.config_loader import load_config
from services.spark_job.transform import aggregate_cdr, flatten_cdr

# Global registry for active queries
ACTIVE_QUERIES: List[StreamingQuery] = []


def setup_logging(cfg):
    config_path = cfg.logging.config_path if cfg.logging else None
    if not config_path:
        candidate = "/opt/app/config/logging.json"
        if os.path.exists(candidate):
            config_path = candidate

    if not config_path or not os.path.exists(config_path):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        return

    with open(config_path) as f:
        config = json.load(f)
    logging.config.dictConfig(config)


def write_to_postgres(df, epoch_id, cfg, table, upsert=False):
    if df.isEmpty():
        return

    df_local = df.toPandas()
    conn = None
    try:
        conn = psycopg2.connect(
            host=cfg.postgres.host,
            port=cfg.postgres.port,
            dbname=cfg.postgres.database,
            user=cfg.postgres.user,
            password=cfg.postgres.password,
        )
        conn.autocommit = False
        cur = conn.cursor()

        if table == "cdr_flattened":
            sql = f"""
            INSERT INTO {cfg.postgres.db_schema}.{table} (
                call_id, caller_id, caller_location, callee_id, callee_location,
                start_time, end_time, duration_sec, bytes_used,
                service_type, cell_id, quality, generated_at, source
            )
            VALUES (%(call_id)s, %(caller_id)s, %(caller_location)s,
                    %(callee_id)s, %(callee_location)s, %(start_time)s,
                    %(end_time)s, %(duration_sec)s, %(bytes_used)s,
                    %(service_type)s, %(cell_id)s, %(quality)s,
                    %(generated_at)s, %(source)s)
            ON CONFLICT (call_id) DO NOTHING;
            """
            execute_batch(cur, sql, df_local.to_dict("records"))

        elif table == "cdr_usage_summary":
            sql = f"""
            INSERT INTO {cfg.postgres.db_schema}.{table} (
                caller_id, window_start, window_end,
                call_count, total_duration_s, avg_duration_s, total_bytes
            )
            VALUES (%(caller_id)s, %(window_start)s, %(window_end)s,
                    %(call_count)s, %(total_duration_s)s,
                    %(avg_duration_s)s, %(total_bytes)s)
            ON CONFLICT (caller_id, window_start, window_end)
            DO UPDATE SET
                call_count = EXCLUDED.call_count,
                total_duration_s = EXCLUDED.total_duration_s,
                avg_duration_s = EXCLUDED.avg_duration_s,
                total_bytes = EXCLUDED.total_bytes,
                last_updated_at = NOW();
            """
            execute_batch(cur, sql, df_local.to_dict("records"))

        conn.commit()
        cur.close()
    except Exception as e:
        logging.error(f"DB batch write failed: {e}")
        if conn:
            conn.rollback()
        time.sleep(2)  # basic backoff
    finally:
        if conn:
            conn.close()


def register_shutdown_handler(spark, logger):
    """Ensure all active queries and Spark stop gracefully on shutdown."""

    def shutdown_handler(signum, frame):
        logger.info(f"Received signal {signum}. Stopping queries gracefully...")
        for q in ACTIVE_QUERIES:
            try:
                if q.isActive:
                    q.stop()
            except Exception as e:
                logger.error(f"Error stopping query {q.name}: {e}")
        try:
            spark.stop()
        except Exception as e:
            logger.error(f"Error stopping SparkSession: {e}")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


def main():
    cfg = load_config()
    setup_logging(cfg)
    logger = logging.getLogger("spark_job")

    spark = (
        SparkSession.builder.appName("cdr-spark-job")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    register_shutdown_handler(spark, logger)

    # Load schema
    with open("/opt/app/common/schemas/cdr_spark_schema.json") as f:
        schema_dict = json.load(f)
    schema = StructType.fromJson(schema_dict)

    # Read raw Kafka stream
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("subscribe", cfg.kafka.topic_raw)
        .option("startingOffsets", cfg.kafka.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.kafka.max_offsets_per_trigger)
        .load()
    )

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("value")
    )

    valid = parsed.filter(col("value").isNotNull()).select("value.*")
    errors = parsed.filter(col("value").isNull()).withColumn(
        "error", lit("Invalid CDR JSON")
    )

    # Flattened records
    flattened = flatten_cdr(valid)

    ACTIVE_QUERIES.append(
        flattened.selectExpr(
            "CAST(call_id AS STRING) AS key", "to_json(struct(*)) AS value"
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("topic", cfg.kafka.topic_flat)
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/flattened")
        .outputMode("append")
        .start()
    )

    ACTIVE_QUERIES.append(
        flattened.writeStream.foreachBatch(
            lambda df, eid: write_to_postgres(df, eid, cfg, "cdr_flattened")
        )
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/flat_db")
        .outputMode("append")
        .start()
    )

    # Aggregations
    aggregated = aggregate_cdr(flattened, cfg)

    ACTIVE_QUERIES.append(
        aggregated.writeStream.foreachBatch(
            lambda df, eid: write_to_postgres(
                df, eid, cfg, "cdr_usage_summary", upsert=True
            )
        )
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/aggregates")
        .outputMode("update")
        .start()
    )

    # Error routing
    ACTIVE_QUERIES.append(
        errors.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("topic", cfg.kafka.topic_error)
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/errors")
        .outputMode("append")
        .start()
    )

    logger.info("All streams started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
