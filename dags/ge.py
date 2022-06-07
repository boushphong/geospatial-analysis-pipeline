from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

import os

from datetime import datetime


home = os.environ.get("HOME")

ge_root_dir = "great_expectations"

default_args = {
    "start_date": datetime(2020, 1, 1)
}

with DAG("pipeline", schedule_interval=None, catchup=False, default_args=default_args) as dag:

    validation = GreatExpectationsOperator(
        task_id="validation",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="my_checkpoint",
        fail_task_on_validation_failure=False
    )

    submit = SparkSubmitOperator(
        task_id="spark_job",
        application=home + "/spark-submit-jobs/etl.py",
        conn_id="spark_conn",
        executor_memory="16G",
        driver_memory="8G",
        jars="jars/geotools-wrapper-1.1.0-25.2.jar,jars/sedona-python-adapter-3.0_2.12-1.2.0-incubating.jar,jars/sedona-viz-3.0_2.12-1.2.0-incubating.jar",
        num_executors=1
    )

    validation >> submit
