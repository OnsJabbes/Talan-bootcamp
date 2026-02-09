FROM bde2020/spark-master:3.0.0-hadoop3.2-java11

USER root
RUN mkdir -p /opt/spark/jobs
COPY spark/*.py /opt/spark/jobs/
WORKDIR /opt/spark/jobs
