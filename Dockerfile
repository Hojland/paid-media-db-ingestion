FROM harbor.aws.c.dk/datascience/base/python-prod:latest

LABEL maintainer=molheh@nuuday.dk

ARG GCM_CREDENTIAL_FILE

COPY $GCM_CREDENTIAL_FILE /app/credentials/

COPY src/ /app/src/

COPY requirements.txt /app

RUN pip install -r requirements.txt --no-cache-dir

WORKDIR /app/src/

CMD ["sh", "-c", "python main.py"]