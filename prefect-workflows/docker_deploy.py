from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from prefect.orion.schemas.schedules import CronSchedule
from movies_etl import load_movies_data
from critics_etl import load_critics_data
from movies_to_bq import movie_reviews_to_bq
from critics_to_bq import critics_to_bq

if __name__ == '__main__':
    etl_web_to_gcs_container = DockerContainer.load('etl-web-to-gcs')
    etl_web_to_gcs_deployment = Deployment.build_from_flow(
        flow=etl_web_to_gcs,
        name='web-to-gcs-docker-flow',
        infrastructure=etl_web_to_gcs_container,
        entrypoint='etl_web_to_gcs.py:etl_web_to_gcs',
        schedule=CronSchedule(cron='0 0 * * *', timezone='UTC', day_or=True)
    )
    etl_web_to_gcs_deployment.apply()

    etl_gcs_to_bq_container = DockerContainer.load('etl-gcs-to-bq')
    etl_gcs_to_bq_deployment = Deployment.build_from_flow(
        flow=etl_gcs_to_bq,
        name='gcs-to-bq-docker-flow',
        infrastructure=etl_gcs_to_bq_container,
        entrypoint='etl_gcs_to_bq.py:etl_gcs_to_bq',
        schedule=CronSchedule(cron='0 1 * * *', timezone='UTC', day_or=True)
    )
    etl_gcs_to_bq_deployment.apply()