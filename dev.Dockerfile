FROM harbor.aws.c.dk/datascience/base/python-dev:latest

COPY src/ /app/

COPY credentials/ credentials/

COPY requirements.txt /app

RUN pip install -r requirements.txt

RUN pip install jupyterlab

CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token=paid"]