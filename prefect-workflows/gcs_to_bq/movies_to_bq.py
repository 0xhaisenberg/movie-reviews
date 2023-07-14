import pandas as pd
import time
from datetime import date, timedelta
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials




@task(retries=3)
def extract_from_gcs() -> pd.DataFrame:
    """Download trip data from GCS"""

    yesterdays_dt = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    past_quarter = (date.today() - timedelta(90)).strftime('%Y-%m-%d')
    # API date filter is start_dt:end_dt and uses the following format "YYYY-MM-DD:YYYY-MM-DD"
    date_filter = f'{past_quarter}:{yesterdays_dt}'

    gcs_path = f'data/review/{date_filter}.parquet'
    gcs_block = GcsBucket.load('movie-reviews')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')

    df = pd.read_parquet(gcs_path)

    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("movie-reviews-credentials")
    df.to_gbq(
        destination_table='movie_reviews_all.movie_data',
        project_id='movie-reviews-392119',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100_000,
        if_exists='replace'
    )


@flow()
def movie_reviews_to_bq():
     """Main ETL flow to load data into BigQuery"""

     try:

        data = extract_from_gcs()
        print('File Extraction Successful')
        write_bq(data)
        print('File Load To BigQuery Successful')
     except Exception as e:
        print(f"{e} \nFile Operation not successful, please check errors")


"""
if __name__ == '__main__':
    movie_reviews_to_bq()
"""