FROM harbor.aws.c.dk/datascience/base/python-prod:latest

LABEL maintainer=molheh@nuuday.dk

ARG GCM_CREDENTIAL_FILE

COPY $GCM_CREDENTIAL_FILE /app/credentials/paid-media-db-ingestion-e95489028100.json

ENV GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE=/app/credentials/paid-media-db-ingestion-e95489028100.json

ENV GOOGLE_ADS_DELEGATED_ACCOUNT=nuuday-paid@paid-media-db-ingestion.iam.gserviceaccount.com

COPY src/ /app/src/

COPY requirements.txt /app

RUN pip install -r requirements.txt --no-cache-dir

WORKDIR /app/src/

CMD ["sh", "-c", "python main.py"]