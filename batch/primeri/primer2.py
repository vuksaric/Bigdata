#The average age of the car in a collision
import os
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("batch").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']
df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .load(HDFS_NAMENODE + "/preprocessed/batch-preprocessed.csv") \

df = df.filter(col("VEHICLE_YEAR").isNotNull()).filter((col("VEHICLE_YEAR").cast("integer") <= 2020) & (col("VEHICLE_YEAR").cast("integer") >= 1990)) \
    .withColumn("YEAR",col("VEHICLE_YEAR").cast("integer")) \
    .withColumn("CRASH_YEAR",substring("CRASH_DATE",7,4).cast("integer")) \
    .withColumn("CRASH_MONTH",substring("CRASH_DATE",1,2)) \
    .withColumn("YEARS_DIFF", col("CRASH_YEAR") - col("YEAR") ) \
    .groupBy("VEHICLE_YEAR") \
    .agg(
        avg("YEARS_DIFF").alias("AvgCrashTime(years)")
    ).orderBy(desc("VEHICLE_YEAR"))
 
df.write.csv(HDFS_NAMENODE + "/results/primer2.csv")
df.show()