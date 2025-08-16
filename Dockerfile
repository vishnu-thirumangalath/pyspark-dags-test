FROM bitnami/spark:3.5.0 

# Copy PySpark application
COPY pyspark_test.py /app/pyspark_test.py

# Copy metrics config
COPY metrics.properties /opt/spark/conf/metrics.properties

# Ensure Python dependencies are installed
RUN pip install --no-cache-dir pyspark

# Optional: add Hadoop AWS libs for s3a:// support
# RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/
# RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar -P /opt/spark/jars/

WORKDIR /app
