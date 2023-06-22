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
from airflow.models import Variable

from pytz import timezone
import pytz


# def get_overwritten_start_end_dates(default_start_date, default_end_date, config_variable_name):
#     """
#     How to use this function?
#     This function expects a config defined in Airflow Variables, which can
#     be set from the Airflow web ui.
#     The function looks for START_DATE and END_DATE in the config and
#     returns the values. It returns the default start/end dates otherwise.
#     :param default_start_date:
#     :param default_end_date:
#     :param config_variable_name:
#     :return:
#     """
#     try:
#         config = Variable.get(config_variable_name, deserialize_json=True)
#         start = config.get('START_DATE', default_start_date)
#         end = config.get('END_DATE', default_end_date)
#         return start, end
#     except KeyError as e:
#         return default_start_date, default_end_date

# # This can be used as below. We use these START_DATE and END_DATE for further processing in our DAG
# START_DATE, END_DATE = get_overwritten_start_end_dates(default_start_date='{{ macros.ds_add(ds, -1) }}',
#                                                        default_end_date='{{ ds }}',
#                                                        config_variable_name='my.custom.jobname.config')



def _populate_variables(**kwargs):

    print(f"{kwargs['data_interval_start'] =}")
    print(f"{kwargs['data_interval_end'] =}")

    if kwargs["data_interval_start"] == kwargs["data_interval_end"]: # Si es una ejecución manual, deben ser iguales
        print("Manual trigger testing")
        prev_insert_date_ci = (kwargs["data_interval_end"] - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        print("Scheduled trigger testing")
        prev_insert_date_ci = kwargs["data_interval_start"].strftime("%Y-%m-%d")
    print(f"{prev_insert_date_ci =}")


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
        


