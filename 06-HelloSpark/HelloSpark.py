from pyspark.sql import *
import sys
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__" :

    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    # Checking Command Line argument
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    # Reading the config file
    logger.info(spark.sparkContext.getConf().toDebugString())

    #Loading the dataframe
    survey_raw_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    logger.info(count_df.collect())             # collect returns the DF as python list
    # survey_raw_df.show()                      # show is more of a utility function


    input("Press Enter")   # To stop the program here and see the wen ui of spark
    logger.info("Finished HelloSpark")
    # spark.stop()

