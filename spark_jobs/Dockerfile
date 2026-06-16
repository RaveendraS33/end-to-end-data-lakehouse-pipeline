FROM apache/spark:3.5.6

WORKDIR /app

USER root
RUN mkdir -p /tmp/spark-checkpoints /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /app /tmp/spark-checkpoints /home/spark/.ivy2

USER spark

COPY spark_jobs ./spark_jobs
COPY src ./src

ENV PYTHONPATH=/app
