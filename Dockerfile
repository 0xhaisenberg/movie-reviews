FROM prefecthq/prefect:2.10.21-python3.11

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN mkdir /opt/prefect-workflows

COPY prefect-workflows /opt/flows/prefect-workflows