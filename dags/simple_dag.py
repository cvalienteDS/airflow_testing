from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta


default_args = {
    'retry':5,
    'retry_delay': timedelta(minutes=5)
}

def _downloading_data(**kwargs):
    print(kwargs)

def _create_file(**kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data in a row')
    return 42

def _checking_data(ti):
    my_xcom = ti.xcom_pull(key='return_value', task_ids=['create_file'])
    print(my_xcom)

with DAG(dag_id='simple_dag', schedule_interval='@daily', default_args=default_args,
    # start_date = datetime(2021, 1, 1), 
    start_date= days_ago(2),
    catchup=True,
    max_active_runs=3) as dag:
    
    # task1 = DummyOperator(
    #     task_id = 'task_1'
    # )

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )

    create_file = PythonOperator(
        task_id='create_file',
        python_callable= _create_file
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data
    )
    
    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        poke_interval=30
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 1'
    )

    downloading_data >> create_file >> [waiting_for_data, processing_data] >> checking_data