import duckdb
from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
from ingestion.awsFuns import *
from datetime import datetime
from loguru import logger
import fire
from ingestion.models import (
    validate_table,
    FileDownloads,
    PypiJobParameters,
)


def main(params: PypiJobParameters):
    start_time = datetime.now()
    # Loading data from BigQuery
    df = get_bigquery_result(
        query_str=build_pypi_query(params),
        bigquery_client=get_bigquery_client(project_name=params.gcp_project)
    )

    # validate_table(df, FileDownloads)
    # Loading to DuckDB

    logger.info("now i write data into csv file locally")
    conn=duckdb.connect()
    create_table_from_dataframe(conn,params.table_name,df)

    
    logger.info(f"Sinking data to {params.destination}")
    if "local" in params.destination:
        conn.sql(f"COPY {params.table_name} TO '{params.table_name}.csv';")


    if "s3" in params.destination:
        load_aws_credentials(conn,params.aws_profile)
        write_to_s3(
            conn,f"{params.table_name}",params.s3_path,"timestamp"
        )
    


   


if __name__ == "__main__":
    #Satr el code dah by5alini a2bl mn CLI keys and values we a7wlhm le object ytb3t lel fun
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
