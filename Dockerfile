FROM python:3.7-stretch

RUN pip install google-cloud-datastore==1.7.3 elasticsearch==6.3.1

COPY indexer.py /

ENTRYPOINT ["python", "/indexer.py"]
