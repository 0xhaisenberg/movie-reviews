from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from prefect.server.schemas.schedules import CronSchedule
from web_to_gcs.movies_etl import load_movies_data
from web_to_gcs.critics_etl import load_critics_data
from web_to_gcs.movies_etl import API_KEY
from gcs_to_bq.movies_to_bq import movie_reviews_to_bq
from gcs_to_bq.critics_to_bq import critics_to_bq
from web_to_gcs.etl_web_to_gcs import etl_web_to_gcs
from gcs_to_bq.gcs_to_bq import gcs_to_bq


if __name__ == '__main__':
    etl_web_to_gcs_container = DockerContainer.load("web2gcs")
    etl_web_to_gcs_deployment = Deployment.build_from_flow(
        flow=etl_web_to_gcs,
        name='web-to-gcs-docker-flow',
        infrastructure=etl_web_to_gcs_container,
        entrypoint='etl_web_to_gcs.py:etl_web_to_gcs',
        schedule=CronSchedule(cron='0 0 1 7/3 *', timezone='UTC', day_or=True)
    )
    etl_web_to_gcs_deployment.apply()

    etl_gcs_to_bq_container = DockerContainer.load("gcs2bq")
    etl_gcs_to_bq_deployment = Deployment.build_from_flow(
        flow=gcs_to_bq,
        name='gcs-to-bq-docker-flow',
        infrastructure=etl_gcs_to_bq_container,
        entrypoint='gcs_to_bq.py:gcs_to_bq',
        schedule=CronSchedule(cron='0 1 1 7/3 *', timezone='UTC', day_or=True)
    )
    etl_gcs_to_bq_deployment.apply()

