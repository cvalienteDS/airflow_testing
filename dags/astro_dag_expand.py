import os
from datetime import datetime, timedelta
import sys
import logging

from airflow import DAG
from airflow import configuration
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.date_time_sensor import DateTimeSensor
from airflow.models import Variable
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from operators.emr.create_job_flow import IBEmrCreateJobFlowOperator, Costcodes
from operators.emr.add_step import IBEmrAddStepsOperator
from airflow.operators.bash import BashOperator



DAG_PATH = os.path.dirname(os.path.abspath(__file__))

d = { # Después de pruebas cambiar s3://iberiasbx-data-lake/dev/ por s3://iberia-data-lake
    "ibgat":{
        "config":{
            # "host":"bdgl-scan.ib:1521",
            # "db":"ibgat",
        },
        'schemas':{
            'DWH_BIPROD':{
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value # TODO: Set as 7
                    "expected_total": 27
                    },
                "tables":{
                    "DWH_DELAY": { # Define only table name
                        "table_name": "DWH_BIPROD.DWH_DELAY", # Define schema.table_name in capital letters
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/DWH_BIPROD/DWH_DELAY/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        # "date_from": "(insert_date_ci_as_datetime - timedelta(days=100)).strftime('%Y-%m-%d')",
                        # "incremental_field":"LOAD_UPDATE_DATE"
                    },
                    
                }
            }
        }     
    },

    "ibsisper":{
        "config":{
            # "host":"bdibel3.ib:1521",
            # "db":"ibsisperc_pr",
        },
        'schemas':{
            'BESME':{
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 'X'
                    },
                "tables":{
                    
                    "TSMELIMTFOTO": {
                        "table_name": "BESME.TSMELIMTFOTO",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BESME/TSMELIMTFOTO/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        "incremental":"#TODO",
                    },
                    

                    
                    
                }
            },
            'BEIBP':{
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 13
                    },
                "tables":{
                    "TIBPPLA": {
                        "table_name": "BEIBP.TIBPPLA",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEIBP/TIBPPLA/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        "incremental_field":"PLAFECHA",
                        "date_from": "(insert_date_ci_as_datetime.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')"
                    },
                    

                    
                }
            },
            'ASEJUR':{ # Solo hay una sin usos, así que las meto todas
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 10
                    },
                "tables":{
                    "TASJEXD": {
                        "table_name": "ASEJUR.TASJEXD",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/ASEJUR/TASJEXD/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },
                    "TASJEXP": {
                        "table_name": "ASEJUR.TASJEXP",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/ASEJUR/TASJEXP/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },

                    
                    
                }
            },
            'BEBIL':{
                "config":{
                    "timeframe_for_querying":3, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 2
                    },
                "tables":{
                    "TBILGV_PROPUESTA": {
                        "table_name": "BEBIL.TBILGV_PROPUESTA",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEBIL/TBILGV_PROPUESTA/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },
                    "TBILGV_DIC_ESTADO": {
                        "table_name": "BEBIL.TBILGV_DIC_ESTADO",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEBIL/TBILGV_DIC_ESTADO/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },
                }
            },
            'BEPBP':{
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 3
                    },
                "tables":{
                    "TPBPDPE": {
                        "table_name": "BEPBP.TPBPDPE",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEPBP/TPBPDPE/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },
                    "TPBPDIR": {
                        "table_name": "BEPBP.TPBPDIR",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEPBP/TPBPDIR/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },
                    "TPBPUSU": {
                        "table_name": "BEPBP.TPBPUSU",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEPBP/TPBPUSU/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                    },                    
                }
            },
            'GESTDOC':{
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 2
                    },
                "tables":{
                    "GD_CARPETAS": {
                        "table_name": "GESTDOC.GD_CARPETAS",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/GESTDOC/GD_CARPETAS/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        
                    },
                    "GD_CRITERIOS_DOCUMENTOS": {
                        "table_name": "GESTDOC.GD_CRITERIOS_DOCUMENTOS",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/GESTDOC/GD_CRITERIOS_DOCUMENTOS/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        
                    },
                    "GD_VERSIONES": {
                        "table_name": "GESTDOC.GD_VERSIONES",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/GESTDOC/GD_VERSIONES/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        
                    },
                    "GD_DOCUMENTOS": {
                        "table_name": "GESTDOC.GD_DOCUMENTOS",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/GESTDOC/GD_DOCUMENTOS/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        
                    },
                    
                                   
                }
            },            
        }
               
    },
    
    "ibzib":{
        "config":{
            # "host":"zbdl-scan.ib:1521",
            # "db":"ibzib",
        },
        'schemas':{
            'BEBIL':{
                "config":{
                    "timeframe_for_querying":0, # Please, set Madrid local time as number, e.g.: 13 for 13.00h. Maybe dict should be sorted according to this value
                    "expected_total": 9
                },
                "tables": {
                    "TBILCUP": {
                        "table_name": "BEBIL.TBILCUP",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEBIL/TBILCUP/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        "incremental_field":"CUP_TIMESTAMP",
                    },
                    "TBILBIL": {
                        "table_name": "BEBIL.TBILBIL",
                        "s3_path": "s3://iberia-data-lake/integraciones/hr_ciclope_ingest/raw/BEBIL/TBILBIL/snapshot_date={insert_date_ci}/",
                        "delimiter": "|",
                        "quote": '"',
                        "incremental_field":"BIL_FEMISION",
                    },
                    
                }
            }
        }

    }
}



def _populate_variables(**kwargs):
    import re

    insert_date_ci_as_datetime = kwargs["data_interval_start"]
    insert_date_ci = insert_date_ci_as_datetime.strftime("%Y-%m-%d")

    data_interval_end = kwargs["data_interval_end"].strftime("%Y-%m-%d")

    print(kwargs)
    print("insert_date_ci or start of interval: ", insert_date_ci)
    print("End of interval: ", data_interval_end)

    kwargs["ti"].xcom_push(key="insert_date_ci", value=insert_date_ci)

    previous_month = (insert_date_ci_as_datetime.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"{previous_month = }")
    days_before_100 = (insert_date_ci_as_datetime - timedelta(days=100)).strftime('%Y-%m-%d')
    print(f"{days_before_100 = }")

    first_time = Variable.get('integrations_integrations_hr_ciclope_ingest_first_dag_run')
    kwargs["ti"].xcom_push(key="first_dag_run", value=first_time)
    print(f"{first_time = }")
    if  first_time == "1":
        ds_1900 = "1900-01-01"
        print(f"First time dag run. All incremental tables will be extracted from {ds_1900}")
        Variable.set('integrations_integrations_hr_ciclope_ingest_first_dag_run', 0)

    # Using Basehook instead of template engine because I expect it will allows to call connection once and stores it in python, instead of calling meta db 4 times for each table (user, pwd, host…) and 300 tables.
    ORACLE_CONN_dict = {
        'IBGAT' : BaseHook.get_connection("aip_prod_oracle_ibgat_integrations"),
        'IBSISPER' : BaseHook.get_connection("aip_prod_oracle_ibsisper_integrations"),
        'IBZIB' : BaseHook.get_connection("aip_prod_oracle_ibzib_integrations")
    }
    

    
    # ----------------- Task sqoop ------------------------------------------------
    for db, db_data in d.items():

        
      
        
        # db=db_data["config"]["db"]
        ORACLE_CONN = ORACLE_CONN_dict[db.upper()]
        SQOOP_STEP = globals()[f"SQOOP_STEP_{db.upper()}"]
        # logging.info(f"Getting host: '{ORACLE_CONN.host}' and db name: '{ORACLE_CONN.schema}'")
        

        for schema_name, data in db_data['schemas'].items():

            for table_name, tbs in data["tables"].items():


                
                

                # From here above, insert_date_ci will be date_before for extracting purposes. Insert_date_ci will have snapshot marks purposes
                if first_time == "1": # First time dag run logic
                    date_before = ds_1900
                    is_incremental = tbs.get("incremental_field", None)
             
                # subsequent executions after the first
                elif "date_from" in tbs: # If table has a "date_from" in "d" dict
                    date_before = eval(tbs["date_from"])
                    print(f'{table_name}: date_from before eval: {tbs["date_from"]}')
                    print(f'{table_name}: date_from after eval: {date_before}')
                else: # In case not "date_from" we use data_interval_start. Usually will be the day before
                    date_before = insert_date_ci
                
  

                kwargs["ti"].xcom_push(
                    key=f"DATE_BEFORE_{db.upper()}_{schema_name}_{table_name}", # table with schema because there are duplicate table names
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

    checkpoint_start_etl = DummyOperator(
        task_id="checkpoint_start_etl"
    )

    
    for db, db_data in d.items():

        with TaskGroup(group_id=db, ui_color='#D7DBDD') as tg_1:
            db=db.upper()

            h_list = []

            for schema_name, data in db_data['schemas'].items():

                h=data.get('config', 0).get('timeframe_for_querying',0) # If key doesnt exist, get 0 hour as default value
                h_list.append(h)

                delay = DateTimeSensor.partial(
                            task_id="delay",
                            poke_interval= 60*15,
                            mode="reschedule",
                            timeout=60*60*24
                            ).expand(
                                target_time=f"{{{{ data_interval_end.in_timezone('Europe/Madrid').replace(hour={h}, minute=0) }}}}",
                            )
                checkpoint_start_etl >> delay



                with TaskGroup(group_id=schema_name, parent_group=tg_1, ui_color='#D98880') as tg_2:

                    schema_has_views_sensor = FileSensor(
                        task_id=f'{schema_name}_has_views', 
                        filepath=f"{DAG_PATH}/redshift/{db.lower()}/{schema_name}/views/create_views.sql",
                        poke_interval=10,
                        mode="reschedule",
                        timeout=20,
                        soft_fail=True)
                    
                    create_views = PostgresOperator(
                            task_id=f"create_views_{db.lower()}_{schema_name}",
                            sql=f"/redshift/{db.lower()}/{schema_name}/views/create_views.sql",
                            postgres_conn_id='REDSHIFT_CONNECTION_ID',
                            retries=0,
                            trigger_rule='all_success'
                        )
                    key_list = []
                    redshift_fname_path_list = []
                    table_names_list= []

                    step_sqoopimport = BashOperator.partial(
                            task_id=f"extract_{schema_name}",
                            
                            retries=0,
                        ).expand(
                            command=f'{{{{ ti.xcom_pull(task_ids="init", key="{key_list}") }}}}',
                        ) 
                    
                    @task
                    def create_copy_kwargs(src_list):
                        result_list = []
                        for tb in src_list:
                            result_list.append(
                                {
                            'sql': "/redshift/" + db.lower() + "/" + schema_name + "/", + tb.upper() + ".sql"
                            'params': {
                                'table': tb,
                                'db': db,
                                'src_schema': schema_name,
                                'tgt_schema': 'integrations'
                                    }
                                }
                            )
                        return result_list
                    
                    copy_kwargs = create_copy_kwargs(table_names_list)

                    copy_files_to_redshift = PostgresOperator.partial(
                            task_id=f"copy_{schema_name}_to_redshift",                            
                            postgres_conn_id='REDSHIFT_CONNECTION_ID',
                            retries=0,
                            trigger_rule='none_failed'
                        ).expand_kwargs(copy_kwargs)
                    
                    for table_name, tbs in data["tables"].items():
                        key_list.append(f"SQOOP_STEP_{db}_{tbs['table_name'].upper()}")

                        # def _is_first_dag_run(**kwargs):
                        #     first_dag_run = kwargs["ti"].xcom_pull(key='first_dag_run', task_ids='init')
                        #     print(f"{first_dag_run =}")
                        #     print(f"{kwargs =}")

                        #     if first_dag_run == "1":
                        #         return kwargs['drop_task']
                        #     else:
                        #         return kwargs['usual_next_task']
                            
                        
                        # is_first_dag_run = BranchPythonOperator(
                        #     task_id= f"is_first_run_{tbs['table_name'].lower()}",
                        #     python_callable=_is_first_dag_run,
                        #     op_kwargs={'drop_task':f"{db.lower()}.{schema_name}.drop_if_first_run_{tbs['table_name'].lower()}",
                        #                'usual_next_task': f"{db.lower()}.{schema_name}.copy_{tbs['table_name'].lower()}_to_redshift",
                        #                },
                        #     do_xcom_push=False,
                        #     retries=0
                        # )

                        # drop_if_first = PostgresOperator(
                        #     task_id=f"drop_if_first_run_{tbs['table_name'].lower()}",
                        #     sql="/redshift/drop_table.sql",
                        #     postgres_conn_id=REDSHIFT_CONNECTION_ID,
                        #     params={
                        #         'tgt_schema': 'integrations',
                        #         'table': table_name,
                        #     },
                        #     retries=0,
                        #     trigger_rule='none_failed_or_skipped'
                        # )   
                        
                      
                        
                        # redshift_fname_path=f"{DAG_PATH}/redshift/{db}/{schema_name}/{table_name.upper()}.sql"
                        redshift_fname_path_list.append("/redshift/" + db.lower() + "/" + schema_name + "/" + table_name.upper() + ".sql")
                        table_names_list.append(table_name)
                       

                        delay >> step_sqoopimport >> copy_files_to_redshift >> schema_has_views_sensor

                    

                    
                    
                    schema_has_views_sensor >> create_views

        


