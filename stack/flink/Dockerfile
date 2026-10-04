# Flink 2.2 + the connectors our SQL jobs use, baked into the image.
# Production rule: jobs never download jars at runtime. Every jar is pinned and checksum-verified here,
# and the same image runs locally, in CI and on Kubernetes (Flink Kubernetes Operator).
FROM flink:2.2.1-scala_2.12-java21

ARG MAVEN=https://repo1.maven.org/maven2
ARG KAFKA_CONNECTOR=5.0.0-2.2
ARG JDBC_CONNECTOR=4.1.0-2.2
ARG FLINK=2.2.1
ARG ICEBERG=1.12.0
ARG HADOOP=3.4.3
ARG PG_DRIVER=42.7.9
ARG OPENLINEAGE=1.32.0   # the JDBC connector 4.1 calls the OpenLineage client while planning

# path:version pairs; the .sha1 next to each artifact on Maven Central is verified before the jar is kept.
RUN set -eux; cd /opt/flink/lib; \
    for a in \
      org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR} \
      org/apache/flink/flink-sql-avro-confluent-registry/${FLINK} \
      org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR} \
      org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR} \
      org/postgresql/postgresql/${PG_DRIVER} \
      io/openlineage/openlineage-java/${OPENLINEAGE} \
      io/openlineage/openlineage-sql-java/${OPENLINEAGE} \
      org/apache/iceberg/iceberg-flink-runtime-2.2/${ICEBERG} \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG} \
      org/apache/hadoop/hadoop-client-api/${HADOOP} \
      org/apache/hadoop/hadoop-client-runtime/${HADOOP} \
    ; do \
      ver=${a##*/}; art=${a%/*}; art=${art##*/}; jar=${art}-${ver}.jar; \
      wget -q "${MAVEN}/${a}/${jar}" "${MAVEN}/${a}/${jar}.sha1"; \
      echo "$(cat ${jar}.sha1 | cut -c1-40)  ${jar}" | sha1sum -c -; rm ${jar}.sha1; \
    done; \
    chown -R flink:flink /opt/flink/lib

# Job definitions (Flink SQL) and the submit script live in the image too.
COPY --chown=flink:flink jobs /opt/flink/jobs
