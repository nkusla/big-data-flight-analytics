#!/usr/bin/env python3
"""Process data from HDFS and save to MongoDB."""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

def main():
	spark = None
	try:
		# MongoDB connection details
		mongo_database = "flight_analytics"
		mongo_uri = f"mongodb://admin:admin123@mongodb:27017/{mongo_database}?authSource=admin"
		mongo_collection = "airline_otp"

		# Create SparkSession with MongoDB connector
		spark = SparkSession.builder \
			.appName("Process Data") \
			.config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
			.config("spark.mongodb.write.connection.uri", mongo_uri) \
			.getOrCreate()

		spark.sparkContext.setLogLevel("ERROR")

		df = spark.read.csv("/data/raw/*.csv", header=True, inferSchema=True)

		result = df.groupBy("Airline").agg(
			F.avg(
				F.when(F.col("ActualElapsedTime") - F.col("CRSElapsedTime") < 15.0, 1.0).otherwise(0.0)
			).alias("OTP")
		).orderBy("OTP", ascending=False)

		# result.show()

		# Write result to HDFS (backup)
		hdfs_path = "/data/curated/otp"
		result.write.mode("overwrite").csv(hdfs_path, header=True)
		print(f"✓ Result written to HDFS: {hdfs_path}")

		# Write result to MongoDB
		result.write \
			.format("mongodb") \
			.mode("overwrite") \
			.option("collection", mongo_collection) \
			.save()
		print(f"✓ Result written to MongoDB: {mongo_database}.{mongo_collection}")

	except Exception as e:
		print(f"✗ Failed to process data: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()