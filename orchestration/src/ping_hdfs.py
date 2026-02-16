#!/usr/bin/env python3
"""Simple HDFS connectivity check."""

from shared import *

def main():
	spark = None
	try:
		spark = build_spark_session("HDFS Ping")

		hadoop = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
			spark.sparkContext._jvm.java.net.URI("hdfs://hdfs-namenode:9000"),
			spark.sparkContext._jsc.hadoopConfiguration()
		)

		root = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/")
		if hadoop.exists(root):
			print("✓ HDFS is accessible!")
		else:
			print("✗ HDFS is not accessible")

	except Exception as e:
		print(f"✗ Failed to connect to HDFS: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()