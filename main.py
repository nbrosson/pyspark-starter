import argparse

from datetime import datetime
from jobs.ini_config import CONFIG
from jobs.detector.etl_job import detection_etl_main
from jobs import LOGGER
from pyspark.sql import SparkSession


if __name__ == "__main__":
    sc = SparkSession.builder.getOrCreate()
    sc.sparkContext.setLogLevel("ERROR")
    LOGGER.info("Parse additional arguments...")
    parser = argparse.ArgumentParser()
    parser.add_argument('--etl-date', required=False, help="Give the date of the ETL: format YYYY-MM-DD")
    args = parser.parse_args()
    
    LOGGER.info("Starting detection ETL job...")
    detection_etl_main(sc, input_data_folder=CONFIG["data"]["DATA_PATH"], specific_etl_date=args.etl_date)
    LOGGER.info("PySpark job finished")