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
from airflow.contrib.sensors.file_sensor import FileSensor

from pytz import timezone
import pytz

class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'params', 'parameters')

d = {'table1':'1984-02-02', 'table2':'1978-01-01'}

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

    

    for table, date_before in d.items():

        kwargs["ti"].xcom_push(
                        key=f"DATE_BEFORE_{table}", # table with schema because there are duplicate table names
                        value=date_before
                    )


DEFAULT_ARGS = {
    # "owner": OWNER,
    "depends_on_past": False,
    # "email": EMAILS_ON_FAILURE,
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 3,
}

with DAG(
    dag_id='test_postgres',
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

    
    checkpoint_end_etl = DummyOperator(
        task_id="checkpoint_end_etl",
        trigger_rule='none_failed_or_skipped'
    )

    for i in range(3):

        iter = DummyOperator(
        task_id=f"iter_{i}"
    )
        init >> iter

        schema_has_views = FileSensor(task_id=f'schema_has_views_{i}', 
            filepath=f"{os.path.dirname(os.path.abspath(__file__))}/redshift/TABLE{i}.sql",
            poke_interval=10,
            timeout=20,
            soft_fail=True)

        at_the_end_of_every_schema = DummyOperator(
            task_id=f"at_the_end_of_every_schema_{i}"
        )

        for table, date_before in d.items():

            redshift_fname_path="/redshift/" + table.upper() + ".sql"
            xcom_date_bf_key=f"DATE_BEFORE_{table}"
            date_before = '{{ ti.xcom_pull(task_ids="init", key="{xcom_date_bf_key}") }}'
            print(f"{date_before = }")

            copy_files_to_redshift = PostgresOperator(
                task_id=f"copy_{i}_{table}_to_redshift",
                # sql=f"/tmp/integrations_hr_ciclope_redshift/{db}/{schema_name}/{table_name.upper()}.sql",
                sql=redshift_fname_path,
                postgres_conn_id='aip_prod_oracle_ibgat_integrations',
                # parameters={ "date_before": f'{{{{ ti.xcom_pull(task_ids="init", key="DATE_BEFORE_{table}") }}}}' },
                params={
                    "table": 'table2',
                }
            )

            iter >> copy_files_to_redshift >> schema_has_views >> at_the_end_of_every_schema

        
        at_the_end_of_every_schema >> checkpoint_end_etl
    

        


