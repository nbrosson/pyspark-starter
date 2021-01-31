from jobs.detector.data_extractor import TransactionExtractor, IdentityExtractor
from pyspark.sql import SparkSession


def test_transaction_extractor():
    sc = SparkSession.builder.getOrCreate()

    transaction_extractor = TransactionExtractor(sc)
    transaction_schema = transaction_extractor.get_schema()
    df_transaction = transaction_extractor.extract(
        "./tests/mock_data/mock_test_transaction.csv", transaction_schema
    )

    assert df_transaction.columns == transaction_schema
    assert df_transaction.count() == 5


def test_identity_extractor():
    sc = SparkSession.builder.getOrCreate()

    identity_extractor = IdentityExtractor(sc)
    identity_schema = identity_extractor.get_schema()
    df_identity = identity_extractor.extract(
        "./tests/mock_data/mock_test_identity.csv", identity_schema
    )

    assert df_identity.columns == identity_schema