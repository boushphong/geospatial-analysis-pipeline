from airflow import DAG
import os
from datetime import datetime
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_cwd():
    print(os.getcwd())

default_args = {
    'start_date': datetime(2020, 1, 1)
}


with DAG('print_cwd', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    bash = BashOperator(
        task_id='bash_operator',
        bash_command='pwd'
    )

    bash2 = BashOperator(
        task_id='bash_operator_2',
        bash_command='echo $(pwd)'
    )

    pwd = PythonOperator(
        task_id='print_current_working_directory',
        python_callable=print_cwd
    )

    bash >> bash2 >> pwd
