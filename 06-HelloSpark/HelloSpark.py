from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == "__main__" :

    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting HelloSpark")

    # Reading the config file
    logger.info(spark.sparkContext.getConf().toDebugString())


    logger.info("Finished HelloSpark")
    # spark.stop()

