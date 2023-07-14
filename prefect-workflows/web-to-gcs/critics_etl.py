import pandas as pd
import numpy as np
import requests
import json
import time
from datetime import date, timedelta
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from api_info import API_KEY


@task(retries=3)
def extract_data() -> str:
    """
    NYT movie reviews API connection to be re-used across functions.
    API call will return all the critics data from the NYT. 
    """
    payload = {"api-key": API_KEY}
    r = requests.get(f"{base_url}/critics/all.json", params=payload)
    
    return r

@task(log_prints=True)
def arrange_data(r) -> pd.DataFrame:
    """
    Use NYT API to find critics including name, status and bio.
    Return pandas df.
    """
    
    data = r.json()
    critics = data['results']
    
    name, status, bio = ([] for i in range(3))
     #insert all the critics info into the relevant lists
    for critic in critics:
        name.append(critic['sort_name'])
        status.append(critic['status'])
        bio.append(critic['bio'])
        
    critics_df = pd.DataFrame(np.column_stack([name, status, bio]),
                             columns=["name", "status", "bio"])
    
    return critics_df


@task()
def validate_data(df: pd.DataFrame) -> bool:
    """
    Data validation used before proceeding to load stage.
    Check if there is data and if the primary key is unique. 
    """
    if df.empty:
        print("No critics' data found. Finishig execution")
        return False
    
    if pd.Series(df['name']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated, names are not unique")
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")
    
    return True


@task()
def write_local(df: pd.DataFrame) -> Path:
    """Save dataframe as parquet file"""
    data_dir = f'data/critics'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/critics.parquet')
    df.to_parquet(path, compression='gzip')
    print('File saved to local.')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("movie-reviews")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)



@flow()
def load_critics_data():
    """
   ETL function to load critics data, check validity and 
   save as parquet to local and then load into GCS
   
    """

    base_url = 'https://api.nytimes.com/svc/movies/v2/'

    r = extract_data()
    critics_df = arrange_data(r)
    
    # Validate results
    if validate_data(critics_df):
        print("Data valid, proceed to load")
        
    
    try:
        # Convert to parquet, upload to GCS
        write_local(critics_df)
        print('File saved successfully')
        write_gcs(path)
        print('File exported successfully')
    except Exception as e:
        print(f"{e} \nData not exported, please check errors")