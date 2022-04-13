#Number of collisions by driver's license status of people who made collisions without being from New York
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

df = df.filter(col("DRIVER_LICENSE_STATUS").isNotNull()) \
    .filter(col("STATE_REGISTRATION").isNotNull()) \
    .filter(col("STATE_REGISTRATION") != "NY") \
    .groupBy("DRIVER_LICENSE_STATUS") \
    .agg(count("*").alias("count")).orderBy(desc("count"))
    #.filter(col("DRIVER_LICENSE_JURISDICTION") != "NY") \
    #.filter(col("DRIVER_LICENSE_JURISDICTION").isNotNull()) \
    

df.write.csv(HDFS_NAMENODE + "/results/primer5.csv")
df.show()