FROM bitnami/spark:3.5.1

WORKDIR /app

USER root
RUN mkdir -p /tmp/spark-checkpoints && chown -R 1001:1001 /app /tmp/spark-checkpoints

USER 1001

COPY spark_jobs ./spark_jobs
COPY src ./src

ENV PYTHONPATH=/app
