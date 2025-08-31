# Spark-native base with Hadoop, JVM
FROM bitnami/spark:3.5.1

USER root
WORKDIR /opt/app

# Install Python deps for Spark job
COPY services/spark_job/requirements.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip

# Pre-bundle kafka + commons-pool2 jars
RUN apt-get update && apt-get install -y curl \
    && curl -L -o $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    && curl -L -o $SPARK_HOME/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    && curl -L -o $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    && curl -L -o $SPARK_HOME/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copy whole services tree and common (preserve package imports)
COPY services /opt/app/services
COPY common /opt/app/common
COPY scripts/wait-for-it.sh /usr/local/bin/wait-for-it.sh

# Ensure wait-for-it.sh works in Linux
RUN chmod +x /usr/local/bin/wait-for-it.sh \
    && apt-get update && apt-get install -y dos2unix libpq-dev gcc \
    && dos2unix /usr/local/bin/wait-for-it.sh \
    && apt-get purge -y dos2unix && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Env setup (use Bitnami’s Java)
ENV JAVA_HOME=/opt/bitnami/java
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j*.zip:/opt/app"
ENV SPARK_CONFIG=/opt/app/config/spark.yml

VOLUME ["/opt/app/config"]

# Entrypoint: wait for Kafka & DB before starting Spark job
ENTRYPOINT ["sh", "-c", "wait-for-it.sh kafka:9092 -- wait-for-it.sh db:5432 -- python -m common.config.bridge.build_config --service spark --out /opt/app/config && python -m services.spark_job.main"]
