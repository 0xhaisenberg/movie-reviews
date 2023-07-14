from movies_to_bq import movie_reviews_to_bq
from critics_to_bq import critics_to_bq

@flow()
def gcs_to_bq() -> None:
    """ The main ETL function"""
    load_movies_data()
    load_critics_data()


if __name__ == '__main__':
    gcs_to_bq()

