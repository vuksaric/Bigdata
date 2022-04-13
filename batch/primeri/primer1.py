#The most common cause of collisions in women
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

df = df.withColumn("DRIVER_SEX",col("DRIVER_SEX")).withColumn("CONTRIBUTING_FACTOR_1",col("CONTRIBUTING_FACTOR_1")) \
.filter(col('DRIVER_SEX').isNotNull()) \
.filter(df.CONTRIBUTING_FACTOR_1 != "Unspecified") \
.filter(df.DRIVER_SEX == "F") \
.groupBy("CONTRIBUTING_FACTOR_1").agg(count("*").alias("count")).orderBy(desc("count"))
 
df.write.csv(HDFS_NAMENODE + "/results/primer1.csv")
df.show()
