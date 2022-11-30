import os
import pendulum
import yaml
import glob

import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

from airflow.operators.email_operator import EmailOperator


def _ex_time(ti, execution_date, **kwargs):
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    hour = execution_date.strftime("%H")
    path = f'include/files/{year}/{month}/{day}/{hour}'
    ti.xcom_push(key='file_path', value=path)
    print(f"file path year: {path}")

def _process_file(ti):
    filepath= ti.xcom_pull(key='file_path', task_ids='get_path')
    file_list = glob.glob(f'{filepath}/*.txt')
    with open('include/files/data/total.txt', 'w') as total_txt:
        for fi in file_list:
            with open(fi) as infile:
                for line in infile:
                    total_txt.write(line)            

with DAG(
    dag_id='f_dag',
    schedule= None,
    start_date=pendulum.datetime(2022,11,30, tz="UTC"),
    default_args={
        "owner":"airflow",
        "email": "your@gmail.com",
        "email_on_failure": False,
        "email_on_retly":False
    }
):

    get_path = PythonOperator(
        task_id='get_path',
        python_callable=_ex_time
    )
            

    exist_file=FileSensor(
            task_id='exist_file',
            fs_conn_id='filepath',
            filepath="{{ ti.xcom_pull(key='file_path', task_ids='get_path') }}/*.txt",
            poke_interval=10,
            timeout=30,
    )

    process_file = PythonOperator(
        task_id ='process_file',
        python_callable=_process_file
    )

    send_email = EmailOperator(
        task_id='send_email',
        to=['your@gmail.com'],
        subject='Total txt file has been updated!',
        html_content='Please check your updated file!'
    )


    get_path >> exist_file >> process_file >> send_email


