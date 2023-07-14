import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials




@task(retries=3)
def fetch_from_gcs() -> pd.DataFrame:
    """Download trip data from GCS"""
    file = 'critics'
    gcs_path = f'data/critics/{file}.parquet'
    gcs_block = GcsBucket.load('movie-reviews')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')

    df = pd.read_parquet(gcs_path)

    return df


@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("movie-reviews-credentials"))
    df.to_gbq(
        destination_table='movie_reviews_all.movie_critics',
        project_id='movie-reviews-392119',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists='replace'
    )


@flow()
def critics_to_bq():
     """Main ETL flow to load data into BigQuery"""
     data = fetch_from_gcs()

     write_to_bq(data)


if __name__ == '__main__':
    critics_to_bq()