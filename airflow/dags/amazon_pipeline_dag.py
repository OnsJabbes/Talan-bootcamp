from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.docker.operators.docker import DockerOperator

DEFAULT_ARGS = {"owner": "airflow"}

with DAG(
    dag_id="amazon_value_chain_orchestration",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["amazon", "oltp", "olap", "spark"],
) as dag:

    ensure_tables = PostgresOperator(
        task_id="ensure_postgres_tables",
        postgres_conn_id="mart_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS kpi_daily (
          day DATE,
          category TEXT,
          country TEXT,
          totalorders BIGINT,
          totalquantity BIGINT,
          totalrevenue DOUBLE PRECISION,
          totalnetrevenue DOUBLE PRECISION,
          uniquecustomers BIGINT,
          refreshed_at TIMESTAMP DEFAULT now()
        );
        """,
    )

    run_spark_batch = DockerOperator(
        task_id="spark_batch_daily_kpi",
        image="bde2020/spark-master:3.0.0-hadoop3.2-java11",
        api_version="auto",
        auto_remove=True,
        network_mode="amazon-data-value-chain_netw",
        command="""
        bash -lc '
          /spark/bin/spark-submit
          --packages org.postgresql:postgresql:42.7.3
          --master spark://spark-master:7077
          --deploy-mode client
          /app/spark/spark_batch_daily_kpi_to_postgres.py
        '
        """,
        volumes=["/var/run/docker.sock:/var/run/docker.sock", f"{'/'}:/"],  # simple demo mount
        mount_tmp_dir=False,
        environment={
            "HDFS_URL": "hdfs://namenode:8020",
            "PG_URL": "jdbc:postgresql://postgres-mart:5432/martdb",
            "PG_USER": "mart",
            "PG_PASS": "mart",
        },
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
    )

    ensure_tables >> run_spark_batch
