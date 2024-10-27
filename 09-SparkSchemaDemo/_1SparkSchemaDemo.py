from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSessionDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    # Read csv file
    flightTimeCSV_DF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")

    flightTimeCSV_DF.show(5)
    logger.info("CSV Schema : " + flightTimeCSV_DF.schema.simpleString())

    # Read json file
    flightTimeJSON_DF = spark.read \
        .format("json") \
        .load("data/flight*.json")

    flightTimeJSON_DF.show(5)
    logger.info("JSON Schema : " + flightTimeJSON_DF.schema.simpleString())


    # Read parquet file
    # parquet is a binary file format that comes with schema information
    # we should prefer using this format as long as it is available, it is also recommended and default file format for apache spark
    flightTimePARQUET_DF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flightTimePARQUET_DF.show(5)
    logger.info("PARQUET Schema : " + flightTimePARQUET_DF.schema.simpleString())






