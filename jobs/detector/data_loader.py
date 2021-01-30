import subprocess

from datetime import datetime
from jobs import LOGGER
from jobs.ini_config import CONFIG


def load_result(df_to_write, specific_etl_date):
    """
    :param df_to_write: DataFrame that we write as PARQUET
    :param specific_etl_date: If we want the ETL to be run on a specific day (not today), specific_etl_date is the day
    :type df_to_write: DataFrame
    :type specific_etl_date: String of shape YYYY-MM-DD
    :return: 
    :rtype: None
    """
    etl_date = (
        specific_etl_date if specific_etl_date else datetime.now().strftime("%Y-%m-%d")
    )
    LOGGER.info(f"ETL date: {etl_date}")
    LOGGER.warning(f"Creating a new folder {CONFIG["data"]["OUTPUT_DATA_PATH"]}{etl_date}")
    shell_command = f"mkdir {CONFIG["data"]["OUTPUT_DATA_PATH"]}{etl_date}"
    p = subprocess.Popen(shell_command, shell=True)
    p.wait()
    df_to_write.write.parquet(f"{CONFIG["data"]["OUTPUT_DATA_PATH"]}{etl_date}" + "/Result.parquet")
    LOGGER.info(f"Data successfully written in {CONFIG["data"]["OUTPUT_DATA_PATH"]}{etl_date}/Result.parquet")
