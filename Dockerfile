FROM python:3.7-buster

ENV PYTHONUNBUFFERED "yes"

ADD requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /requirements.txt

COPY indexer.py /

ENTRYPOINT ["python", "/indexer.py"]
