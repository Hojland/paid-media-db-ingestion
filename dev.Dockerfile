FROM harbor.aws.c.dk/datascience/base/python-dev:latest

COPY credentials/ /app/credentials/

COPY requirements.txt /app

RUN pip install -r requirements.txt

RUN pip install jupyterlab

WORKDIR /app/src/

CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token=paid"]