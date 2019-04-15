FROM python:3.7-stretch

ENV PYTHONUNBUFFERED "yes"

RUN pip install google-cloud-datastore==1.7.3 elasticsearch==6.3.1 python-dateutil==2.8.0

COPY indexer.py /

ENTRYPOINT ["python", "/indexer.py"]
