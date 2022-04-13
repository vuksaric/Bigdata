#The sum, average, minimum and maximum of the people who participated as passengers in the collision
import os
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.window import Window


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

window = Window.partitionBy("DRIVER_LICENSE_STATUS").orderBy("DRIVER_LICENSE_STATUS")

df = df.filter(col("DRIVER_LICENSE_STATUS").isNotNull()) \
    .filter(col("VEHICLE_OCCUPANTS").isNotNull()) \
    .filter(col("VEHICLE_OCCUPANTS") < 100 ) \
    .withColumn("group_rank", f.row_number().over(window)) \
    .withColumn("AVG", avg(col("VEHICLE_OCCUPANTS")).over(window)) \
    .withColumn("SUM", sum(col("VEHICLE_OCCUPANTS")).over(window)) \
    .withColumn("MIN", min(col("VEHICLE_OCCUPANTS")).over(window)) \
    .withColumn("MAX", max(col("VEHICLE_OCCUPANTS")).over(window)) \
    .where(col("group_rank") == 1) \
    .select("DRIVER_LICENSE_STATUS","AVG","SUM","MIN","MAX") \
    

df.write.csv(HDFS_NAMENODE + "/results/primer6.csv")
df.show()