from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("flight_consumer") \
    .master("local") \
    .getOrCreate()

# Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Read from the Kafka topic 'flight'
kafka_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("subscribe", "flight") \
    .load()

# Write to the text file
query = kafka_data \
    .writeStream \
    .outputMode("append") \
    .format("text") \
    .option("path", "/chemin/vers/votre/repertoire/fichier.txt") 
    .start()

# Attendre la terminaison du streaming
query.awaitTermination()
