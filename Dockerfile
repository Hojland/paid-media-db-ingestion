FROM harbor.aws.c.dk/datascience/base/python-prod:latest

LABEL maintainer=molheh@nuuday.dk

COPY src/ /app/src/

COPY credentials/ /app/credentials/

COPY requirements.txt /app

RUN pip install -r requirements.txt --no-cache-dir

WORKDIR /app/src/

CMD ["sh", "-c", "python main.py"]