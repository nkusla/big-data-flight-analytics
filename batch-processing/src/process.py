#!/usr/bin/env python3
"""Process data from HDFS and save to MongoDB."""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

def main():
	spark = None
	try:
		# Create SparkSession
		spark = SparkSession.builder \
			.appName("Process Data") \
			.config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
			.getOrCreate()

		spark.sparkContext.setLogLevel("ERROR")

		df = spark.read.csv("hdfs://hdfs-namenode:9000/data/raw/*.csv", header=True, inferSchema=True)

		result = df.groupBy("Airline").agg(
			F.avg(
				F.when(F.col("ActualElapsedTime") - F.col("CRSElapsedTime") < 15.0, 1.0).otherwise(0.0)
			).alias("OTP")
		).orderBy("OTP", ascending=False)

		result.show()

		# Write result to HDFS
		path = "/data/processed/otp"
		result.write.mode("overwrite").csv(path, header=True)
		print(f"✓ Result written to {path}")

	except Exception as e:
		print(f"✗ Failed to process data: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()