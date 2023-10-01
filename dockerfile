FROM apache/airflow:2.7.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && apt install tesseract-ocr -y\
  && apt install libtesseract-dev -y \
  && apt install tesseract-ocr-rus -y\
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir --user install apache-airflow-providers-telegram

