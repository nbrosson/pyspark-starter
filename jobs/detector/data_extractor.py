from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    def __init__(self, sql_context):
        self.sql_context = sql_context

    def extract(self, data_path, columns_to_select=None, is_header=True):
        raw_df = self.sql_context.read.option("header", is_header).csv(data_path)
        if columns_to_select:
            return raw_df.select(columns_to_select)
        return raw_df

    @abstractmethod
    def get_schema():
        pass


class TransactionExtractor(BaseExtractor):
    def __init__(self, sql_context):
        BaseExtractor.__init__(self, sql_context)

    @staticmethod
    def get_schema():
        return [
            "TransactionID",
            "TransactionDT",
            "TransactionAmt",
            "ProductCD",
            "card1",
            "card2",
            "card3",
            "card4",
            "card5",
            "card6",
            "addr1",
            "addr2",
            "P_emaildomain",
            "R_emaildomain",
        ]


class IdentityExtractor(BaseExtractor):
    def __init__(self, sql_context):
        BaseExtractor.__init__(self, sql_context)

    @staticmethod
    def get_schema():
        return ["TransactionId", "DeviceType", "DeviceInfo", "id-12"]
