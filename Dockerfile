FROM python:3.11-slim-bookworm

RUN apt-get update && \
  apt-get install -y --no-install-recommends openjdk-17-jdk-headless wget curl ca-certificates && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --no-cache-dir pyspark

RUN mkdir -p /opt/spark/conf
COPY metrics.properties /opt/spark/conf/metrics.properties

COPY pyspark_test.py /app/pyspark_test.py
WORKDIR /app

CMD ["python", "pyspark_test.py"]
