from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from movies_etl import load_movies_data
from critics_etl import load_critics_data

@flow()
def etl_web_to_gcs() -> None:
    """ The main ETL function"""
    load_movies_data()
    load_critics_data()


if __name__ == '__main__':
    etl_web_to_gcs()



