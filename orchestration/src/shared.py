import time
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from confluent_kafka.admin import AdminClient, NewTopic

MONGO_DATABASE = "flight_analytics"
MONGO_URI = f"mongodb://admin:admin123@mongodb:27017/{MONGO_DATABASE}?authSource=admin"
HDFS_DEFAULT_FS = "hdfs://hdfs-namenode:9000"
AIRLINES_DB_PATH = "/data/metadata/Airlines.csv"
AIRPORTS_DB_PATH = "/data/metadata/Airports.csv"
DELAY_THRESHOLD = 15.0

LOOKUP_TOPIC_PARTITIONS = 3
LOOKUP_TOPIC_REPLICATION = 1

def build_spark_session(app_name: str):
	spark = SparkSession.builder \
		.appName(app_name) \
		.config("spark.hadoop.fs.defaultFS", HDFS_DEFAULT_FS) \
		.config("spark.mongodb.write.connection.uri", MONGO_URI) \
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
	return spark

def join_with_airlines_metadata(spark: SparkSession, df: DataFrame):
	airlines_df = spark.read.csv(AIRLINES_DB_PATH, header=True, inferSchema=True)
	airlines_df = airlines_df.select("Code", col("Description").alias("AirlineName"))
	joined = df.join(airlines_df, df.AirlineCode == airlines_df.Code, "left")
	return joined.drop(airlines_df.Code)

def join_with_airports_metadata(spark: SparkSession, df: DataFrame):
	airports_df = spark.read.csv(AIRPORTS_DB_PATH, header=True, inferSchema=True)
	airports_df = airports_df.select("iata", "latitude", "longitude", "name")
	joined = df.join(airports_df, df.AirportCode == airports_df.iata, "left")
	return joined.drop(airports_df.iata)

def save_to_mongodb(df: DataFrame, mongo_collection: str):
	df.write \
		.format("mongodb") \
		.mode("overwrite") \
		.option("collection", mongo_collection) \
		.save()

	print(f"✓ Result written to MongoDB: {MONGO_DATABASE}.{mongo_collection}")

def refresh_topic(topic: str, bootstrap_servers: str) -> None:
	conf = {"bootstrap.servers": bootstrap_servers}
	admin = AdminClient(conf)
	futures = admin.delete_topics([topic])
	try:
		for t, f in futures.items():
			f.result(timeout=30)
	except Exception as e:
		err_str = str(e).lower()
		if "unknown" in err_str and ("topic" in err_str or "partition" in err_str):
			pass
		else:
			raise

	time.sleep(2)
	new_topic = NewTopic(topic, num_partitions=LOOKUP_TOPIC_PARTITIONS, replication_factor=LOOKUP_TOPIC_REPLICATION)
	create_futures = admin.create_topics([new_topic])

	for t, f in create_futures.items():
		f.result(timeout=30)

	time.sleep(1)
	print(f"✓ Topic '{topic}' refreshed (deleted and recreated)")