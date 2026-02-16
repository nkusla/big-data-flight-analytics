from pyspark.sql import SparkSession

MONGO_DATABASE = "flight_analytics"
MONGO_URI = f"mongodb://admin:admin123@mongodb:27017/{MONGO_DATABASE}?authSource=admin"
HDFS_DEFAULT_FS = "hdfs://hdfs-namenode:9000"

def build_spark_session(app_name: str):
	spark = SparkSession.builder \
		.appName(app_name) \
		.config("spark.hadoop.fs.defaultFS", HDFS_DEFAULT_FS) \
		.config("spark.mongodb.write.connection.uri", MONGO_URI) \
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
	return spark