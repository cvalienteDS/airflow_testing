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



d = {'a':10, 'b':11}


def _populate_variables(**kwargs):

    insert_date_ci_as_datetime = kwargs["data_interval_start"]
    insert_date_ci = insert_date_ci_as_datetime.strftime("%Y-%m-%d")
    prev_insert_date_ci = (insert_date_ci_as_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
    data_interval_end = kwargs["data_interval_end"].strftime("%Y-%m-%d")
    print(kwargs)
    print("insert_date_ci: ", insert_date_ci)
    print("prev_insert_date_ci: ", prev_insert_date_ci)
    print("data_interval_end: ", data_interval_end)
    print(f"Target time: {timezone('Europe/Madrid').localize(datetime.now().replace(hour=10,minute=8)).astimezone(pytz.utc)}")
    # next_insert_date_ci = (insert_date_ci_as_datetime + timedelta(days=1)).strftime("%Y-%m-%d")
    # insert_date_ci_no_dash = insert_date_ci_as_datetime.strftime("%Y%m%d")

    kwargs["ti"].xcom_push(key="insert_date_ci", value=insert_date_ci)


DEFAULT_ARGS = {
    # "owner": OWNER,
    "depends_on_past": False,
    # "email": EMAILS_ON_FAILURE,
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 3,
}

with DAG(
    dag_id='test_hr_scope',
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
    
    for schema_name, h in d.items():
        hour=datetime(2023,6,5,1).replace(hour=h,minute=0)
        print(f'Expected time to extract got from dict: {h}')
        print(f'Actual time: {datetime.now()}')
        print(f'Actual time with tz: {datetime.now(pendulum.timezone("Europe/Madrid"))}')
        print(f'Expected time to extract with timezone: {hour}')
        
        delay = DateTimeSensor(
                    task_id=f"delay_{schema_name.lower()}",
                    # target_time=f"{{{{ data_interval_end.replace(hour={h}, minute=37) }}}}",
                    # target_time="{{ timezone('Europe/Madrid').localize(datetime.now().replace(hour=10, minute=12)).astimezone(pytz.utc) }}",
                    # target_time="{{ task_instance.xcom_pull(task_ids='init', key='target_time') }}",
                    # target_time=f'{{{{ data_interval_end.in_timezone(dag.timezone).replace(hour={h}, minute=23) }}}}',
                    target_time=f"{{{{ data_interval_end.in_timezone('Europe/Madrid').replace(hour={h}, minute=40) }}}}",
                    poke_interval= 60*15,
                    mode="reschedule",
                    timeout=60*60*24
                    )

        init >> delay
init
        


