from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "talan-bootcamp_netw")
SPARK_IMAGE = os.getenv("SPARK_JOBS_IMAGE", "amazon-spark-jobs:latest")
PRODUCER_IMAGE = os.getenv("PRODUCER_IMAGE", "amazon-producer:latest")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "amazon-sales")
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres-mart:5432/martdb")
PG_USER = os.getenv("PG_USER", "mart")
PG_PASS = os.getenv("PG_PASS", "mart")
RATE = os.getenv("RATE", "200")

with DAG(
    dag_id="amazon_dual_pipeline_orchestration",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "oltp", "olap", "spark", "hdfs", "trino"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("oltp_pipeline") as oltp_pipeline:
        truncate_orders = DockerOperator(
            task_id="truncate_orders_live",
            image="postgres:15",
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "PGPASSWORD": PG_PASS,
            },
            command=(
                "bash -lc "
                "'psql -h postgres-mart -U {user} -d martdb -c "
                "\"TRUNCATE TABLE orders_live;\"'"
            ).format(user=PG_USER),
            docker_url="unix://var/run/docker.sock",
        )

        produce_to_kafka = DockerOperator(
            task_id="produce_csv_to_kafka",
            image=PRODUCER_IMAGE,
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "KAFKA_BROKER": KAFKA_BROKER,
                "KAFKA_TOPIC": KAFKA_TOPIC,
                "CSV_FILE": "/app/data/Amazon.csv",
                "RATE": RATE,
            },
            command="python kafka_producer_amazon.py",
            docker_url="unix://var/run/docker.sock",
        )

        spark_oltp_streaming = DockerOperator(
            task_id="spark_oltp_streaming_to_postgres",
            image=SPARK_IMAGE,
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "KAFKA_BROKER": KAFKA_BROKER,
                "KAFKA_TOPIC": KAFKA_TOPIC,
                "KAFKA_STARTING_OFFSETS": "earliest",
                "PG_URL": PG_URL,
                "PG_USER": PG_USER,
                "PG_PASS": PG_PASS,
                "RUN_SECONDS": os.getenv("RUN_SECONDS", "60"),
            },
            command=(
                "bash -lc "
                "'/spark/bin/spark-submit "
                "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.postgresql:postgresql:42.7.3 "
                "--master spark://spark-master:7077 "
                "--deploy-mode client "
                "/opt/spark/jobs/spark_oltp_streaming_to_postgres.py'"
            ),
            docker_url="unix://var/run/docker.sock",
            working_dir="/opt/spark/jobs",
        )

        truncate_orders >> [spark_oltp_streaming, produce_to_kafka]

    with TaskGroup("olap_pipeline") as olap_pipeline:
        kafka_to_bronze = DockerOperator(
            task_id="kafka_to_bronze_csv",
            image=SPARK_IMAGE,
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "KAFKA_BROKER": KAFKA_BROKER,
                "KAFKA_TOPIC": KAFKA_TOPIC,
                "HDFS_URL": HDFS_URL,
            },
            command=(
                "bash -lc "
                "'/spark/bin/spark-submit "
                "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 "
                "--master spark://spark-master:7077 "
                "--deploy-mode client "
                "/opt/spark/jobs/spark_olap_kafka_to_bronze_csv.py'"
            ),
            docker_url="unix://var/run/docker.sock",
            working_dir="/opt/spark/jobs",
        )

        bronze_to_silver = DockerOperator(
            task_id="bronze_to_silver",
            image=SPARK_IMAGE,
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "HDFS_URL": HDFS_URL,
            },
            command=(
                "bash -lc "
                "'/spark/bin/spark-submit "
                "--master spark://spark-master:7077 "
                "--deploy-mode client "
                "/opt/spark/jobs/spark_olap_bronze_to_silver.py'"
            ),
            docker_url="unix://var/run/docker.sock",
            working_dir="/opt/spark/jobs",
        )

        silver_to_gold = DockerOperator(
            task_id="silver_to_gold",
            image=SPARK_IMAGE,
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "HDFS_URL": HDFS_URL,
            },
            command=(
                "bash -lc "
                "'/spark/bin/spark-submit "
                "--master spark://spark-master:7077 "
                "--deploy-mode client "
                "/opt/spark/jobs/spark_olap_silver_to_gold.py'"
            ),
            docker_url="unix://var/run/docker.sock",
            working_dir="/opt/spark/jobs",
        )

        kafka_to_bronze >> bronze_to_silver >> silver_to_gold

    start >> truncate_orders
    produce_to_kafka >> kafka_to_bronze
    [silver_to_gold, spark_oltp_streaming] >> end
