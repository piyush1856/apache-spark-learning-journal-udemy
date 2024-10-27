from pyspark.sql import SparkSession

from lib.logger import Log4j
from pyspark.sql.functions import spark_partition_id

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSessionDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimePARQUET_DF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    logger.info("Num Partitions before: " + str(flightTimePARQUET_DF.rdd.getNumPartitions()))
    flightTimePARQUET_DF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimePARQUET_DF.repartition(5)
    logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro-2/") \
        .save()

    flightTimePARQUET_DF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()