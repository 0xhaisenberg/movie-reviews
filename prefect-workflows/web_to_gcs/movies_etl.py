import pandas as pd
import requests
import json
import time
from datetime import date, timedelta
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from api_info import API_KEY


@task(retries=2)
def extract_data(publication_dt: str) -> pd.DataFrame:
    """
    NYT movie reviews API connection to be re-used across functions.
    API call will return 20 most recent movies reviews from the NYT API. 
    """

    base_url = 'https://api.nytimes.com/svc/movies/v2/'

    payload = {"api-key": API_KEY, "publication-date": publication_dt}
    r = requests.get(f"{base_url}reviews/all.json", params=payload)
    df = pd.DataFrame()
    
    r = requests.get(f"{base_url}reviews/all.json", params=payload)

    result = r.json()
    movies = result['results']
    frame = pd.json_normalize(movies)
    df = pd.concat([df, frame], ignore_index=True)
    print('Sequence finished')
        
    return df


@task(log_prints=True)
def rearrange_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selecting necessary columns and rearranging columns
    """

    df.rename(columns = {'display_title': 'title', 'critics_pick': 'recommendation',
                        'byline': 'critic', 'summary_short': 'description', 
                         'link.url': 'review_url' }, inplace=True)
    
    cols=["title", "critic", "description", "recommendation", "opening_date",\
                                         "publication_date","mpaa_rating", "review_url"]
    df = df[cols]
    
    return df


@flow()
def filter_movies_data() -> pd.DataFrame:
    """
    Filter movies data for last 3 months as the ETL frequency will be quarterly.
    Returns desired pandas df.
    """
    today = date.today()
    last_week = (date.today() - timedelta(7)).strftime('%Y-%m-%d')
    # API date filter is start_dt:end_dt and uses the following format "YYYY-MM-DD:YYYY-MM-DD"
    date_filter = f'{last_week}:{today}'    

    df = extract_data(date_filter)
    movies_df = rearrange_data(df)

    return(movies_df)


@task()
def validate_data(df: pd.DataFrame) -> bool:
    """
    Data validation used before proceeding to load stage.
    Check if there is data and if dates match. 
    """
    if df.empty:
        print("No movie reivews found. Finishing execution")
        return False 

    # Check if there are publication dates in the date range selected 
    date_list = []
    for day in range(1,8):
        date_list.append((date.today() - timedelta(day)).strftime('%Y-%m-%d'))
    last_week_set = set(date_list)
    timestamps = set(df["publication_date"].tolist())
    # check if there is intersection between dates in the last week and the set of publication dates
    if not (last_week_set & timestamps):
        raise Exception("None of the dates returned match the dates selected")

    return True

@task()
def write_local(df: pd.DataFrame) -> Path:
    """Write DataFrame out as parquet file and save to local"""

    today = date.today()
    last_week = (date.today() - timedelta(7)).strftime('%Y-%m-%d')
    # API date filter is start_dt:end_dt and uses the following format "YYYY-MM-DD:YYYY-MM-DD"
    date_filter = f'{last_week}:{today}'    

    data_dir = f'data/review'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{date_filter}.parquet')
    df.to_parquet(path, compression='gzip')
    print('File saved to local.')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("movie-reviews")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

@flow()
def load_movies_data():
    """
    Load movies df into parquet file, then load to GCS
    Before loading, use validate_data function to validate results.
    """


    final_df = filter_movies_data()

    # Validate results
    if validate_data(final_df):
        print("Data validated, proceeding to load stage")


    try:
        # Convert to parquet, upload to GCS
        path = write_local(final_df)
        print('File saved successfully')
        write_gcs(path)
        print('File exported successfully')
    except Exception as e:
        print(f"{e} \nData not exported, please check errors")



"""
if __name__ == '__main__':
    load_movies_data()
"""