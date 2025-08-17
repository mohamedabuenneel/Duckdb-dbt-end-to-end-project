import duckdb
import os
from loguru import logger


def create_table_from_dataframe(duckdb_con,table_name:str,dataframe):
    #ana 3aslshan sha8al fe 2 scopes fa el duckdb mesh 3arfa tshof feen el dataframe de
    # fa 3alsahn keda 3amltha explicitly we saglt el object el awel fe duckdb database
    duckdb_con.register("tmp_df", dataframe)
    duckdb_con.sql(
        f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM tmp_df
        """
    )

def load_aws_credentials(duckdb_con, profile: str):
    logger.info(f"Loading AWS credentials for profile: {profile}")
    duckdb_con.sql(f"CALL load_aws_credentials('{profile}');")

def write_to_s3(duckdb_con, table:str, s3_path:str, timestamp_column: str):
    logger.info("Writing data to S3 ")
    duckdb_con.sql(
        f"""
        COPY (
            SELECT *,
                YEAR(month_start_date) AS year, 
                MONTH(month_start_date) AS month 
            FROM {table}
        ) 
        TO '{s3_path}/{table}' 
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
        """
    )