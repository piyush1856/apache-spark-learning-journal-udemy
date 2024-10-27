from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == '__main__':

    # We are going to create a managed table inside Apache Spark which need persistent Metastore
    # It depends upon hive metastore
    # we need spark hive for this example

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSessionDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimePARQUET_DF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    '''
    flightTimePARQUET_DF.write \
        .mode("overwrite") \
    .saveAsTable("flight_data_tbl")
    '''

    '''
    flightTimePARQUET_DF.write \
        .mode("overwrite") \
        .partitionBy("ORIGIN", "OP_CARRIER")
        .saveAsTable("flight_data_tbl")
    '''

    flightTimePARQUET_DF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))


