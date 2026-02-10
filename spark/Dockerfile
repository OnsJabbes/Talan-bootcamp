FROM bde2020/spark-master:3.0.0-hadoop3.2-java11

USER root
RUN mkdir -p /opt/spark/jobs/data
COPY spark/*.py /opt/spark/jobs/
COPY data/Amazon.csv /opt/spark/jobs/data/Amazon.csv
WORKDIR /opt/spark/jobs
