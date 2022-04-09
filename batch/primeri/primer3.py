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

df = df.withColumn("CRASH_MONTH",substring("CRASH_DATE",1,2)) \
    .groupBy("CRASH_MONTH") \
    .agg(count("*").alias("count")).orderBy(desc("count"))

df2 = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .load(HDFS_NAMENODE + "/preprocessed/batch-preprocessed.csv") \

df2 = df2.withColumn("TIME",substring("CRASH_TIME",1,2)) \
    .groupBy("TIME") \
    .agg(count("*").alias("count")).orderBy(desc("count"))

 
df.write.csv(HDFS_NAMENODE + "/results/primer3.csv")
df.show()
df2.show()