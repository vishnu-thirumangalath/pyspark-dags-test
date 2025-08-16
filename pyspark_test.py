import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Configure Python logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Start Spark
logger.info("Starting Spark session...")
spark = SparkSession.builder.appName("TestFilter").getOrCreate()

# Sample data
data = [
    ("Booking", "api-gateway-adapter"),
    ("Prebooking", "web-frontend"),
    ("Prebooking", "hotel-adapter"),
    ("Cancel", "flight-adapter"),
    ("Cancel", "mobile-ui"),
    ("Hey", "car-adapter"),
    ("Hey", "car-no"),
    ("Booking", "api-gateway"),
]
columns = ["log_type", "source"]
processed_df = spark.createDataFrame(data, columns)
logger.info("Data loaded into DataFrame.")

# Filtering function
def filter_logs(log_types_list):
    if set(log_types_list).difference({"Booking", "Prebooking"}):
        logger.info(f"Filtering with adapter exclusion: {log_types_list}")
        return processed_df.filter(
            (col("log_type").isin(log_types_list)) &
            (~expr("source LIKE '%adapter%'"))
        )
    else:
        logger.info(f"Filtering without adapter exclusion: {log_types_list}")
        return processed_df.filter(col("log_type").isin(log_types_list))

# Test 1
logger.info("Running Test 1...")
filtered_df_1 = filter_logs(["Booking", "Prebooking"])
filtered_df_1.show()

# Test 2
logger.info("Running Test 2...")
filtered_df_2 = filter_logs(["Booking", "Cancel", "Prebooking", "Hey"])
filtered_df_2.show()

logger.info("Sleeping for 20 seconds to keep pod alive for Prometheus scrape...")
time.sleep(60)

logger.info("Job complete. Shutting down Spark.")
spark.stop()
