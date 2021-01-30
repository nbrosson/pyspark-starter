from abc import ABC, abstractmethod
from pyspark.sql.functions import when, col


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self):
        pass


class IdentityTransformer(BaseTransformer):
    @staticmethod
    def transform(input_data):
        """
        :param df_to_write: input DataFrame
        :type df_to_write: DataFrame
        :return: Dataframe where DeviceInfo is not null
        :rtype: Dataframe
        """
        return input_data.filter(col("DeviceInfo").isNotNull())


class TransactionTransformer(BaseTransformer):
    @staticmethod
    def transform(input_data):
        """
        :param df_to_write: input DataFrame
        :type df_to_write: DataFrame
        :return: Dataframe with transformed columns
        :rtype: Dataframe
        """
        transformed_data = input_data.withColumn(
            "TransactionAmtTransformed",
            when(
                input_data.TransactionAmt.isNotNull(),
                input_data.TransactionAmt * 100,
            ).otherwise(100),
        )
        return transformed_data.drop("TransactionAmt")

    @staticmethod
    def join_tables_on_transaction_id(
        transaction_table, other_table, join_type="inner", delete_key_after_join=True
    ):
        df_join = transaction_table.join(
            other_table,
            how=join_type,
            on=transaction_table.TransactionID == other_table.TransactionId,
        )
        if delete_key_after_join is True:
            return df_join.drop("TransactionID")
        return df_join
