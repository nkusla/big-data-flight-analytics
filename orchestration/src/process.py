#!/usr/bin/env python3
"""Process data from HDFS and save to MongoDB."""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from shared import *

def main():
	spark = None
	try:
		mongo_collection = "airline_otp"
		spark = build_spark_session("Process Data")

		df = spark.read.parquet("/data/transformed/*.parquet", header=True, inferSchema=True)

		otp_result = df.groupBy("Operating_Airline").agg(
			F.avg(
				F.when(F.col("ActualElapsedTime") - F.col("CRSElapsedTime") < 15.0, 1.0).otherwise(0.0)
			).alias("OTP")
		).orderBy("OTP", ascending=False)

		# Write result to HDFS (backup)
		# hdfs_path = "/data/curated/otp"
		# result.write.mode("overwrite").csv(hdfs_path, header=True)
		# print(f"✓ Result written to HDFS: {hdfs_path}")

		otp_result.write \
			.format("mongodb") \
			.mode("overwrite") \
			.option("collection", mongo_collection) \
			.save()
		print(f"✓ Result written to MongoDB: {MONGO_DATABASE}.{mongo_collection}")

	except Exception as e:
		print(f"✗ Failed to process data: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()