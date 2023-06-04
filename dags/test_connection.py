from airflow import DAG

from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.operators.python import PythonOperator

import pendulum

def my_uri():
    from airflow.hooks.base import BaseHook
    return BaseHook.getconnection('htttp_default').get_uri()

with DAG(

   dag_id = 'test_connection',

   schedule=None,

   start_date=pendulum.datetime(2022, 9, 1),

   catchup=False,

   default_args={

       "retries": 2,

   },

   tags=['http','example']

) as dag:
    t1 = SimpleHttpOperator(

        task_id="http",

        endpoint="planets/1/",

        method="GET",

        http_conn_id="http_default",

        log_response=True

    )

    t2 = PythonOperator(

        task_id = "print_uri",

        python_callable = my_uri

   )

    t1 >> t2
