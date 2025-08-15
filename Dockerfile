FROM python:3.11-slim

RUN pip install --no-cache-dir pyspark

COPY pyspark_test.py /app/pyspark_test.py
WORKDIR /app

CMD ["python", "pyspark_test.py"]
