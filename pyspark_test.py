from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import time

# Create Spark session
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

# Function to apply filtering logic
def filter_logs(log_types_list):
    if set(log_types_list).difference({"Booking", "Prebooking"}):
        # Contains log types other than Booking/Prebooking
        print(f"Filtering with adapter exclusion (log_types_list: {log_types_list})")
        return processed_df.filter(
            (col("log_type").isin(log_types_list)) &
            (~expr("source LIKE '%adapter%'"))
        )
    else:
        # Only Booking and/or Prebooking
        print(f"Filtering without adapter exclusion (log_types_list: {log_types_list})")
        return processed_df.filter(
            col("log_type").isin(log_types_list)
        )

# Test 1
log_types_list_1 = ["Booking", "Prebooking"]
filtered_df_1 = filter_logs(log_types_list_1)
print("✅ Test 1 Output:")
filtered_df_1.show()

# Test 2
log_types_list_2 = ["Booking", "Cancel", "Prebooking", "Hey"]
filtered_df_2 = filter_logs(log_types_list_2)
print("✅ Test 2 Output:")
filtered_df_2.show()

time.sleep(20)
