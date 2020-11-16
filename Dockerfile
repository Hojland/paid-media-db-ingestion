FROM python:3.8.2-slim-buster

RUN apt-get update && apt-get install -y \
  curl \
  default-libmysqlclient-dev \
  gcc \
  g++ \
  htop \
  locales \
  python3-dev \
  && apt-get clean -y && rm -rf /var/lib/apt/lists/*

LABEL maintainer=molheh@nuuday.dk

ARG GCM_CREDENTIAL_FILE

COPY $GCM_CREDENTIAL_FILE /app/credentials/paid-media-db-ingestion-e95489028100.json

ENV GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE=/app/credentials/paid-media-db-ingestion-e95489028100.json

ENV GOOGLE_ADS_DELEGATED_ACCOUNT=nuuday-paid@paid-media-db-ingestion.iam.gserviceaccount.com

ENV GOOGLE_CM_API_USER_ID=6002761

COPY src/ /app/

COPY requirements.txt /app

RUN pip install -r requirements.txt --no-cache-dir

WORKDIR /app

CMD ["sh", "-c", "python main.py"]