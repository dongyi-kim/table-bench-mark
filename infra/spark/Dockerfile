# Spark 3.5 image with Iceberg runtime + AWS bundle, used as the Session A writer via
# Spark Connect. Spark writes Iceberg (full v3 + MERGE INTO upsert); StarRocks queries.
ARG SPARK_VERSION=3.5.3
FROM apache/spark:${SPARK_VERSION}

ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.10.2
ARG SCALA_VERSION=2.12
ARG MVN=https://repo1.maven.org/maven2

USER root
RUN set -eux; \
    J=/opt/spark/jars; \
    # Iceberg Spark runtime (includes Spark SQL extensions + SparkCatalog)
    curl -fSL -o "$J/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      "${MVN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
    # Iceberg AWS bundle (S3 FileIO + REST signer for Polaris on MinIO)
    curl -fSL -o "$J/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "${MVN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    # Spark Connect server + its grpc/protobuf deps (bake to avoid runtime ivy resolution)
    curl -fSL -o "$J/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "${MVN}/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar"

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Server is launched via docker-compose `command`. Reads spark-defaults.conf for the
# Polaris/Iceberg catalog config so every Connect session inherits it.
USER spark
