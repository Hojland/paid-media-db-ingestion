FROM harbor.aws.c.dk/datascience/base/python-prod:latest

LABEL maintainer=molheh@nuuday.dk

COPY src/ /app/

COPY credentials/ credentials/

COPY requirements.txt /app

RUN pip install -r requirements.txt --no-cache-dir

CMD ["sh", "-c", "python google.py"]