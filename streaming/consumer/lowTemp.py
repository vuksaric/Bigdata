#Temp bellow 0 and snow days in November
import os
from tokenize import String
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Streaming processing") \
    .getOrCreate()

quiet_logs(spark)

try:
  weather = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
    .option("subscribe", "accidents") \
    .load()

  print("CONSUMER")

  schema = StructType() \
    .add("Sunrise", StringType()) \
    .add("Sunset", StringType()) \
    .add("Max_temp",StringType()) \
    .add("Min_temp", StringType()) \
    .add("Avg_temp", StringType()) \
    .add("Total_snow", StringType()) \
    .add("Date", StringType()) \
    .add("Temp_1", StringType()) \
    .add("Wind_1", StringType()) \
    .add("Visibility_1", StringType()) \
    .add("Precipitation_1", StringType()) \
    .add("Temp_2", StringType()) \
    .add("Wind_2", StringType()) \
    .add("Visibility_2", StringType()) \
    .add("Precipitation_2", StringType()) \
    .add("Temp_3", StringType()) \
    .add("Wind_3", StringType()) \
    .add("Visibility_3", StringType()) \
    .add("Precipitation_3", StringType()) \
    .add("Temp_4", StringType()) \
    .add("Wind_4", StringType()) \
    .add("Visibility_4", StringType()) \
    .add("Precipitation_4", StringType()) \
    .add("Temp_5", StringType()) \
    .add("Wind_5", StringType()) \
    .add("Visibility_5", StringType()) \
    .add("Precipitation_5", StringType()) \
    .add("Temp_6", StringType()) \
    .add("Wind_6", StringType()) \
    .add("Visibility_6", StringType()) \
    .add("Precipitation_6", StringType()) \
    .add("Temp_7", StringType()) \
    .add("Wind_7", StringType()) \
    .add("Visibility_7", StringType()) \
    .add("Precipitation_7", StringType()) \

  weather.printSchema()

  weather = weather.select(
      from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

  weather = weather.withColumn("Max_temp",col("Max_temp").cast(IntegerType())) \
                    .withColumn("Min_temp", col("Min_temp").cast(IntegerType())) \
                    .withColumn("Avg_temp",col("Avg_temp").cast(IntegerType())) \
                    .withColumn("Total_snow",col("Total_snow").cast(FloatType())) \
                    .withColumn("Temp_1",col("Temp_1").cast(IntegerType())) \
                    .withColumn("Wind_1", col("Wind_1").cast(IntegerType())) \
                    .withColumn("Visibility_1",col("Visibility_1").cast(IntegerType())) \
                    .withColumn("Precipitation_1",col("Precipitation_1").cast(FloatType())) \
                    .withColumn("Temp_2",col("Temp_2").cast(IntegerType())) \
                    .withColumn("Wind_2", col("Wind_2").cast(IntegerType())) \
                    .withColumn("Visibility_2",col("Visibility_2").cast(IntegerType())) \
                    .withColumn("Precipitation_2",col("Precipitation_2").cast(FloatType())) \
                    .withColumn("Temp_3",col("Temp_3").cast(IntegerType())) \
                    .withColumn("Wind_3", col("Wind_3").cast(IntegerType())) \
                    .withColumn("Visibility_3",col("Visibility_3").cast(IntegerType())) \
                    .withColumn("Precipitation_3",col("Precipitation_3").cast(FloatType())) \
                    .withColumn("Temp_4",col("Temp_4").cast(IntegerType())) \
                    .withColumn("Wind_4", col("Wind_4").cast(IntegerType())) \
                    .withColumn("Visibility_4",col("Visibility_4").cast(IntegerType())) \
                    .withColumn("Precipitation_4",col("Precipitation_4").cast(FloatType())) \
                    .withColumn("Temp_5",col("Temp_5").cast(IntegerType())) \
                    .withColumn("Wind_5", col("Wind_5").cast(IntegerType())) \
                    .withColumn("Visibility_5",col("Visibility_5").cast(IntegerType())) \
                    .withColumn("Precipitation_5",col("Precipitation_5").cast(FloatType())) \
                    .withColumn("Temp_6",col("Temp_6").cast(IntegerType())) \
                    .withColumn("Wind_6", col("Wind_6").cast(IntegerType())) \
                    .withColumn("Visibility_6",col("Visibility_6").cast(IntegerType())) \
                    .withColumn("Precipitation_6",col("Precipitation_6").cast(FloatType())) \
                    .withColumn("Temp_7",col("Temp_7").cast(IntegerType())) \
                    .withColumn("Wind_7", col("Wind_7").cast(IntegerType())) \
                    .withColumn("Visibility_7",col("Visibility_7").cast(IntegerType())) \
                    .withColumn("Precipitation_7",col("Precipitation_7").cast(FloatType())) \

  weather = weather.withColumn("Bad_conditions", when((col("Min_temp") <= 0) | (col("Total_snow") > 0), "Bad_weather").otherwise("Ok_weather")) \
                   .groupBy("Bad_conditions").count()

  query = weather.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start(truncate=False)

  query.awaitTermination()

except Exception as e:
  print(e)
  pass








