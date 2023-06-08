import os
from datetime import datetime, timedelta
import sys
import logging
import pendulum

from airflow import DAG
from airflow import configuration
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.date_time_sensor import DateTimeSensor
from airflow.decorators import task

from pytz import timezone
import pytz





def _populate_variables(**kwargs):

    print(f"{kwargs['data_interval_start'] =}")
    print(f"{kwargs['data_interval_end'] =}")


DEFAULT_ARGS = {
    # "owner": OWNER,
    "depends_on_past": False,
    # "email": EMAILS_ON_FAILURE,
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 3,
}

with DAG(
    dag_id='test_dates',
    description="Incremental data ingestion for Iberia from IBSISPER.HR and IBSISPER.CICLOPE to Redshift",
    start_date=datetime(2023, 1, 1),
    schedule_interval= None, #"0 8 * * *"
    max_active_runs=1,
    max_active_tasks=4, # deprecated max_concurrency
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=[
        "integrations",
        "sqoop",
        "s3",
        "emr",
        "redshift",
        ],
    doc_md="docs",

) as dag:
    
    init = PythonOperator(
        task_id="init", 
        python_callable=_populate_variables, 
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    
    
init
        


