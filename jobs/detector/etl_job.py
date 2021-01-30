from jobs import LOGGER
from jobs.ini_config import CONFIG
from jobs.detector.data_extractor import IdentityExtractor, TransactionExtractor
from jobs.detector.data_transformer import IdentityTransformer, TransactionTransformer
from jobs.detector.data_loader import load_result
from pyspark.sql import SQLContext


def detection_etl_main(sc, input_data_folder, specific_etl_date):
    """
    :param sc: Spark Session
    :param input_data_folder: path where the data is stored
    :param specific_etl_date: If we want the ETL to be run on a specific day (not today), specific_etl_date is the day
    :type sc: SparkSession
    :type input_data_folder: String
    :type specific_etl_date: String of shape YYYY-MM-DD
    :return: 
    :rtype: None
    """

    LOGGER.info("Get sql contexts...")

    # Extract identity and transaction data
    LOGGER.info("Retrieve raw datasets...")
    transaction_extractor = TransactionExtractor(sc)
    transaction_schema = transaction_extractor.get_schema()
    df_transaction = transaction_extractor.extract(
        input_data_folder + CONFIG["data"]["TRANSACTION_DATA"], transaction_schema
    )

    identity_extractor = IdentityExtractor(sc)
    identity_schema = identity_extractor.get_schema()
    df_identity = identity_extractor.extract(
        input_data_folder + CONFIG["data"]["IDENTITY_DATA"], identity_schema
    )

    # Make some transformations and join tables
    LOGGER.info("Transform datasets and merge them...")
    transformed_df_transaction = TransactionTransformer.transform(df_transaction)
    transformed_df_identity = IdentityTransformer.transform(df_identity)
    df_join = TransactionTransformer.join_tables_on_transaction_id(
        transformed_df_transaction, transformed_df_identity
    )

    # Load the result
    LOGGER.info("Write the results...")
    load_result(df_join, specific_etl_date)