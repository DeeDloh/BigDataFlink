FROM flink:1.18.0-scala_2.12-java11

RUN apt-get update -q && \
    apt-get install -y -q python3 python3-pip python-is-python3 curl default-jdk-headless && \
    pip3 install --no-cache-dir apache-flink==1.18.0 psycopg2-binary && \
    curl -fsSL "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

COPY job.py /app/job.py
