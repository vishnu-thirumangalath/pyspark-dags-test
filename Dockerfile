FROM python:3.11-slim

RUN pip install --no-cache-dir pyspark

RUN apt-get update && apt-get install -y openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY pyspark_test.py /app/pyspark_test.py
WORKDIR /app

CMD ["python", "pyspark_test.py"]
