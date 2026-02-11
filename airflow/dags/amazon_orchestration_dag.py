from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "talan-bootcamp-final-stage_netw")
SPARK_IMAGE = os.getenv("SPARK_JOBS_IMAGE", "amazon-spark-jobs:latest")
PRODUCER_IMAGE = os.getenv("PRODUCER_IMAGE", "amazon-producer:latest")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "amazon-sales")
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres-mart:5432/martdb")
PG_USER = os.getenv("PG_USER", "mart")
PG_PASS = os.getenv("PG_PASS", "mart")
RATE = os.getenv("RATE", "1000")

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
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    # ── OLTP Pipeline (Producer + Spark Streaming in PARALLEL) ────

    with TaskGroup("oltp_pipeline") as oltp_pipeline:

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
                "PYTHONUNBUFFERED": "1",
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
                "RUN_SECONDS": os.getenv("RUN_SECONDS", "180"),
            },
            command=(
                "bash -lc "
                "'/spark/bin/spark-submit "
                "--packages "
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
                "org.postgresql:postgresql:42.7.3 "
                "--master spark://spark-master:7077 "
                "--deploy-mode client "
                "/opt/spark/jobs/spark_oltp_streaming_to_postgres.py'"
            ),
            docker_url="unix://var/run/docker.sock",
            working_dir="/opt/spark/jobs",
        )

        def _restart_streamlit():
            """Restart the Streamlit container so it picks up fresh data."""
            import docker
            import time
            client = docker.DockerClient(base_url="unix://var/run/docker.sock")
            ct = client.containers.get("streamlit-oltp")
            print("Restarting streamlit-oltp ...")
            ct.restart(timeout=10)
            time.sleep(10)
            ct.reload()
            assert ct.status == "running", "status=%s" % ct.status
            print("Streamlit dashboard is live -> http://localhost:8502")

        restart_streamlit = PythonOperator(
            task_id="restart_streamlit_dashboard",
            python_callable=_restart_streamlit,
        )

        # Producer and Spark Streaming run IN PARALLEL
        # When both finish, restart Streamlit
        [produce_to_kafka, spark_oltp_streaming] >> restart_streamlit

    # ── OLAP Pipeline (Sequential: Bronze → Silver → Gold) ────────

    with TaskGroup("olap_pipeline") as olap_pipeline:
        csv_to_bronze_hdfs = DockerOperator(
            task_id="csv_to_bronze_hdfs",
            image=SPARK_IMAGE,
            auto_remove=True,
            mount_tmp_dir=False,
            network_mode=DOCKER_NETWORK,
            environment={
                "HDFS_URL": HDFS_URL,
                "CSV_FILE": "/opt/spark/jobs/data/Amazon.csv",
            },
            command=(
                "bash -lc "
                "'/spark/bin/spark-submit "
                "--master local[*] "
                "--deploy-mode client "
                "/opt/spark/jobs/spark_olap_csv_to_bronze_hdfs.py'"
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

        csv_to_bronze_hdfs >> bronze_to_silver >> silver_to_gold

    # ── DAG Dependencies ──────────────────────────────────────────
    # OLTP (parallel producer+streaming) and OLAP (sequential) run in parallel
    start >> [oltp_pipeline, olap_pipeline] >> end