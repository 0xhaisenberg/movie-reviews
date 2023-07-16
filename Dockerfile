FROM prefecthq/prefect:2.10.21-python3.11

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN mkdir /opt/prefect-workflows

COPY web-to-gcs/etl_web_to_gcs.py /opt/prefect-workflows/etl_web_to_gcs.py
COPY gcs-to-bq/gcs_to_bq.py /opt/prefect-workflows/gcs_to_bq.py