from pyspark.sql import SparkSession

from lib.logger import Log4j
from pyspark.sql.functions import spark_partition_id


if __name__ == '__main__':
    # Since we want to work with AVRO, we will require to add a scala dependency, since we are working in python env we can't do it directly
    # So, we need to add it in spark-defaults.conf - spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.5

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSessionDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimePARQUET_DF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    flightTimePARQUET_DF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro-1/") \
        .save()

    logger.info("Number of Partition before : " + str(flightTimePARQUET_DF.rdd.getNumPartitions()))

    # Here I got One avro file, but I have two partitions but we should get 2 file for 2 partitions

    flightTimePARQUET_DF.groupBy(spark_partition_id()).count().show()
    # We do have two partitions but second partition got nothing, so we got one output file

    


