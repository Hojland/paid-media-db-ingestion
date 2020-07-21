FROM harbor.aws.c.dk/datascience/base/python-prod:latest

COPY src/ /app/

COPY requirements.txt /app

LABEL maintainer=molheh@nuuday.dk

RUN pip install -r requirements.txt --no-cache-dir

CMD ["sh", "-c", "python main.py"]