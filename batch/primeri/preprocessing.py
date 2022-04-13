import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Deleting unneccessary columns") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

df = spark.read \
  .option("delimiter", ",") \
  .option("header", "true") \
  .csv(HDFS_NAMENODE + "/data/hadoop-data/Motor_Vehicle_Collisions.csv")

df = df.select(
"CRASH_DATE", 
"CRASH_TIME", 
"STATE_REGISTRATION", 
"VEHICLE_TYPE",
"VEHICLE_MAKE",
"VEHICLE_YEAR",
"TRAVEL_DIRECTION",
"VEHICLE_OCCUPANTS",
"DRIVER_SEX",
"DRIVER_LICENSE_STATUS",
"PRE_CRASH",
"POINT_OF_IMPACT",
"VEHICLE_DAMAGE",
"VEHICLE_DAMAGE_1",
"VEHICLE_DAMAGE_2",
"VEHICLE_DAMAGE_3",
"PUBLIC_PROPERTY_DAMAGE",
"CONTRIBUTING_FACTOR_1",
"CONTRIBUTING_FACTOR_2"
)

df = df.withColumn("CRASH_DATE", col("CRASH_DATE").cast(StringType())) \
    .withColumn("CRASH_TIME", col("CRASH_TIME").cast(StringType())) \
    .withColumn("STATE_REGISTRATION", col("STATE_REGISTRATION").cast(StringType())) \
    .withColumn("VEHICLE_TYPE", col("VEHICLE_TYPE").cast(StringType())) \
    .withColumn("VEHICLE_MAKE", col("VEHICLE_MAKE").cast(StringType())) \
    .withColumn("VEHICLE_YEAR", col("VEHICLE_YEAR").cast(StringType())) \
    .withColumn("TRAVEL_DIRECTION", col("TRAVEL_DIRECTION").cast(StringType())) \
    .withColumn("VEHICLE_OCCUPANTS", col("VEHICLE_OCCUPANTS").cast(IntegerType())) \
    .withColumn("DRIVER_SEX", col("DRIVER_SEX").cast(StringType())) \
    .withColumn("DRIVER_LICENSE_STATUS", col("DRIVER_LICENSE_STATUS").cast(StringType())) \
    .withColumn("PRE_CRASH", col("PRE_CRASH").cast(StringType())) \
    .withColumn("POINT_OF_IMPACT", col("POINT_OF_IMPACT").cast(StringType())) \
    .withColumn("VEHICLE_DAMAGE", col("VEHICLE_DAMAGE").cast(StringType())) \
    .withColumn("VEHICLE_DAMAGE_1", col("VEHICLE_DAMAGE_1").cast(StringType())) \
    .withColumn("VEHICLE_DAMAGE_2", col("VEHICLE_DAMAGE_2").cast(StringType())) \
    .withColumn("VEHICLE_DAMAGE_3", col("VEHICLE_DAMAGE_3").cast(StringType())) \
    .withColumn("PUBLIC_PROPERTY_DAMAGE", col("PUBLIC_PROPERTY_DAMAGE").cast(StringType())) \
    .withColumn("CONTRIBUTING_FACTOR_1", col("CONTRIBUTING_FACTOR_1").cast(StringType())) \
    .withColumn("CONTRIBUTING_FACTOR_2", col("CONTRIBUTING_FACTOR_2").cast(StringType())) \

df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(HDFS_NAMENODE + "/preprocessed/batch-preprocessed.csv")
