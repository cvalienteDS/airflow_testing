import os
from datetime import datetime, timedelta, date
import requests
import json
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

from airflow.utils.task_group import TaskGroup

from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from operators.emr.create_job_flow import IBEmrCreateJobFlowOperator, Costcodes
from operators.emr.add_step import IBEmrAddStepsOperator




import boto3

from airflow.models import Variable


######################################## VARS ########################################
# Global environment variables
# Avoid loading variables from files or from Airflow's own variables. For that, use the "init_params" task.
ENV = "production"
VERTICAL = "integrations"
COSTCODE = "integrations"
PROJECT = "hr_ciclope_ingest"
DAG_NAME = "integrations_integrations_hr_ciclope_ingest"
OWNER = "iberia.integrations@nextdigital.es"
RESPONSIBLE = "carlos.valiente@nextdigital.es"
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
DAG_PATH = os.path.dirname(os.path.abspath(__file__))
FILENAME = "hr_ciclope_ingest"

# The following variables refer to Jinja templates. If needed, uncomment it.
# INSERT_DATE_CI = "{{ ds }}"
# INSERT_DATE_CI_NODASH = "{{ ds_nodash }}"
# PREV_INSERT_DATE_CI = "{{ prev_ds }}"
# PREV_INSERT_DATE_CI_NODASH = "{{ prev_ds_nodash }}"
# NEXT_INSERT_DATE_CI_NODASH = "{{ next_ds_nodash }}"





REDSHIFT_CONNECTION_ID_PROVISIONED = 'aip_prod_redshift_hr_integrations'
REDSHIFT_CONNECTION_ID_SERVERLESS = 'aip_prod_redshift_serverless_integrations'

EMR_S3_BUCKET = "prdeu-ibr-emr-s3"
S3_BUCKET = "iberia-data-lake"


#####################################################################################

##################################### CONECTIONS ####################################

# SQOOP_STEP = """sqoop import \
#     -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
#     --connect "jdbc:oracle:thin:@//{oracle_host}/{oracle_db}" \
#     --username "{oracle_username}" \
#     --password "{oracle_password}" \
#     --null-non-string '' \
#     --null-string '' -m 1 \
#     --target-dir "{s3_path}" \
#     --fields-terminated-by "{delimiter}" \
#     --query "{query}" -m 1 \
#     --delete-target-dir --verbose
# """

SQOOP_STEP_IBGAT = """sqoop import \
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
    --connect "jdbc:oracle:thin:@//bdgl-scan.ib:1521/ibgat" \
    --username "{oracle_username}" \
    --password "{oracle_password}" \
    --null-non-string '' \
    --null-string '' -m 1 \
    --target-dir "{s3_path}" \
    --fields-terminated-by "{delimiter}" \
    --query "{query}" -m 1 \
    --delete-target-dir --verbose
"""

SQOOP_STEP_IBSISPER = """
    sqoop import  \
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
    --connect "jdbc:oracle:thin:@//bdibel3.ib:1521/ibsisperc_pr" \
    --username "{oracle_username}" \
    --password "{oracle_password}" \
    --null-non-string '' \
    --null-string '' -m 1 \
    --target-dir "{s3_path}" \
    --fields-terminated-by "{delimiter}" \
    --query "{query}" -m 1 \
    --delete-target-dir --verbose
"""

SQOOP_STEP_IBZIB = """
    sqoop import  \
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
    --connect "jdbc:oracle:thin:@//zbdl-scan.ib:1521/ibzib" \
    --username "{oracle_username}" \
    --password "{oracle_password}" \
    --null-non-string '' \
    --null-string '' -m 1 \
    --target-dir "{s3_path}" \
    --fields-terminated-by "{delimiter}" \
    --query "{query}" -m 1 \
    --delete-target-dir --verbose
"""

#---------------TABLE -------------------------------



#####################################################################################

######################################## DAG ########################################
# Definition of DAG configuration parameters
sys.path.insert(0, f"{DAG_FOLDER}")

# EMAIL_CONNECTION = "email_noreply_conn"
# configuration.conf.set("smtp", "smtp_starttls", "True")
# configuration.conf.set("smtp", "smtp_host", EMAIL_CONNECTION.host)
# configuration.conf.set("smtp", "smtp_port", str(EMAIL_CONNECTION.port))
# configuration.conf.set("smtp", "smtp_mail_from", EMAIL_CONNECTION.schema)
# configuration.conf.set("smtp", "smtp_user", EMAIL_CONNECTION.login)
# configuration.conf.set("smtp", "smtp_password", EMAIL_CONNECTION.password)

EMAILS_TO = ["carlos.valiente@nextdigital.es"]
EMAILS_CC = ["iberia.customer@nextdigital.es"]
EMAILS_ON_FAILURE = ["iberia.customer@nextdigital.es", "carlos.valiente@nextdigital.es"]

DEFAULT_ARGS = {
    "owner": OWNER,
    "depends_on_past": False,
    "email": EMAILS_ON_FAILURE,
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 3,
}


docs = """

Documentation can be found in the \[Docs folder.](https://gitlab.com/iberia-data/data-processing/integrations/airflow-dags/-/blob/aip/dags/integrations/hr_ciclope_ingest/docs/)

"""

#############################################################################################

######################################## INIT PARAMS ########################################
# Initial task in which the variables to be used are loaded into xcom
# Uncomment or add those that are necessary
def _populate_variables(**kwargs):
    import re

    d = Variable.get("MY_DICT", deserialize_json=True)
    print(d)

    insert_date_ci_as_datetime = kwargs["data_interval_start"]
    insert_date_ci = insert_date_ci_as_datetime.strftime("%Y-%m-%d")
    prev_insert_date_ci = (insert_date_ci_as_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
    data_interval_end = kwargs["data_interval_end"].strftime("%Y-%m-%d")
    print(kwargs)
    print("insert_date_ci: ", insert_date_ci)
    print("prev_insert_date_ci: ", prev_insert_date_ci)
    print("data_interval_end: ", data_interval_end)
    # next_insert_date_ci = (insert_date_ci_as_datetime + timedelta(days=1)).strftime("%Y-%m-%d")
    # insert_date_ci_no_dash = insert_date_ci_as_datetime.strftime("%Y%m%d")

    kwargs["ti"].xcom_push(key="insert_date_ci", value=insert_date_ci)
    # kwargs["ti"].xcom_push(key="prev_insert_date_ci", value=prev_insert_date_ci)
    # kwargs["ti"].xcom_push(key="next_insert_date_ci", value=next_insert_date_ci)
    # kwargs["ti"].xcom_push(key="insert_date_ci_no_dash", value=insert_date_ci_no_dash)

    previous_month = (insert_date_ci_as_datetime.replace(day=1) - timedelta(days=1)).strftime('%Y%m')
    print(f"{previous_month = }")
    days_before_100 = (insert_date_ci_as_datetime - timedelta(days=100)).strftime('%Y-%m-%D')
    print(f"{days_before_100 = }")

    # Using Basehook instead of template engine because I expect it will allows to call connection once and stores it in python, instead of calling meta db 4 times for each table (user, pwd, host…) and 300 tables.
    ORACLE_CONN_dict = {
        'IBGAT' : BaseHook.get_connection("aip_prod_oracle_ibgat_integrations"),
        'IBSISPER' : BaseHook.get_connection("aip_prod_oracle_ibsisper_integrations"),
        'IBZIB' : BaseHook.get_connection("aip_prod_oracle_ibzib_integrations")
    }
    

    logging.info(f"dag folder: : {DAG_FOLDER}")
    
    # ----------------- Task sqoop ------------------------------------------------
    for db, db_data in d.items():

        s3 = boto3.client("s3")

        
        
        host=db_data["config"]["host"]
        # db=db_data["config"]["db"]
        ORACLE_CONN = ORACLE_CONN_dict[db.upper()]
        SQOOP_STEP = globals()[f"SQOOP_STEP_{db.upper()}"]
        logging.info(f"Getting host: '{ORACLE_CONN.host}' and db name: '{ORACLE_CONN.schema}'")
        

        for schema_name, data in db_data['schemas'].items():

            if not os.path.exists(f"/tmp/integrations_hr_ciclope_sqoop/{db}/{schema_name}/"):
                os.makedirs(f"/tmp/integrations_hr_ciclope_sqoop/{db}/{schema_name}/")

            timeframe=data["config"]["timeframe_for_querying"]
            expected_total=data["config"]["expected_total"]
            # assert expected_total == len(data["tables"])
            len_data_tables = len(data["tables"])
            print(f"{expected_total =}")
            print(f"{len_data_tables =}")
            logging.info(f'Extracting data from {schema_name} schema.')

            for table_name, tbs in data["tables"].items():

                table_without_schema = table_name
                table = tbs["table_name"].upper()

            

                # with open(DAG_PATH + "/sqoop/" + db + "/" + table_without_schema.upper() + ".sql", "r") as f:
                #     SQOOP_QUERY = f.read()
                # TODO: Al poner en producción hacerlo con esto. Y para ello hay que poner todos los .sql en su ruta. Y borrar el repo paralelo
                
                Key=f"code/iberia-data/data-engineering/etls/integrations/sqoop/{db}/{schema_name}/{table_without_schema}.sql"
                print(f"{Key =}")

                Filename=f"/tmp/integrations_hr_ciclope_sqoop/{db}/{schema_name}/{table_without_schema}.sql"
                print(f"{Filename =}")

                s3.download_file(
                    Bucket="prdeu-ibr-emr-s3",
                    Key=Key,
                    Filename=Filename
                )
                with open(f"/tmp/integrations_hr_ciclope_sqoop/{db}/{schema_name}/{table_without_schema}.sql", 'r') as f:
                    SQOOP_QUERY = f.read()
                
                if "date_from" in tbs:
                    date_before = eval(tbs["date_from"])
                else:
                    date_before = insert_date_ci
                
                assert isinstance(date_before, str)
                assert isinstance(data_interval_end, str)
                assert re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date_before)
                assert re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', data_interval_end)

                query=SQOOP_QUERY.format(
                            # snapshot_date = insert_date_ci,
                            date_before= date_before,
                            date_after=data_interval_end,
                        )
                logging.info(f"Query: {query}")
                
                kwargs["ti"].xcom_push(
                    key=f"SQOOP_STEP_{db.upper()}_{table}", # table with schema because there are duplicate table names
                    value=SQOOP_STEP.format(
                        oracle_host= ORACLE_CONN.host,
                        oracle_db= ORACLE_CONN.schema,
                        oracle_username= ORACLE_CONN.login,
                        oracle_password= ORACLE_CONN.password,
                        # table_name=tbs["table_name"],
                        s3_path=tbs["s3_path"].format(insert_date_ci=insert_date_ci), # otra opcion sería poner así: "nsert_date_ci={{ ds_nodash }}"
                        delimiter=tbs["delimiter"],
                        # s3_bucket=S3_BUCKET,
                        query=query,
                        # s3_path=tbs["s3_path"].format(table=table_without_schema, date="{{ ds }}"),
                    ),
                )
            break # PAra que solo hagamos ibgat. Temporal hasta que funcione la conexión a las otras DBs
                
      

with DAG(
    dag_id=DAG_NAME,
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
    doc_md=docs,

) as dag:
    
    init = PythonOperator(
        task_id="init", 
        python_callable=_populate_variables, 
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )




    ######################################################################################
    ######################################## EMR ########################################
    # The EMR operator is initialised with the most commonly used parameters.
    # Bootstrap is commented, if necessary, uncomment it.

    create_emr_cluster = IBEmrCreateJobFlowOperator(
        idle_timeout_seconds=3600,
        task_id="create_emr_cluster",
        process_name=PROJECT, # concatena nombre cluster y proyecto
        costcode= COSTCODE, #Costcodes.INTEGRATIONS.name,
        region_name="eu-west-1",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        keep_job_flow_alive_when_no_steps=True,
        release_label="emr-6.3.0",
        execution_type="standard",
        applications=["Sqoop", "Hive"], # Oracle is invalid
        s3_maximum_connections="10000",
        step_concurrency_level=4, # uno por conexión más o menos
        check_if_exist=True,
        ec2_subnet_ids=["subnet-1214b64a"], # mandatory to extract from IB On Premise
        classification_configurations=[
            "emrfs-site",
            "yarn-site",
            "mapred-site",
        ],
        tags={
            "CostCode": COSTCODE,
            "Project": PROJECT,
            "Responsible": RESPONSIBLE,
            "Owner": OWNER,
        },
    )



    install_oracle_driver = IBEmrAddStepsOperator(
        task_id="install_oracle_driver",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        # action_on_failure="TERMINATE_CLUSTER",
        # command="sudo wget -O /usr/lib/sqoop/lib/ojdbc8.jar https://download.oracle.com/otn-pub/otn_software/jdbc/213/ojdbc8.jar",
        command="sudo aws s3 cp s3://{emr_s3_bucket}/code/iberia-data/data-engineering/etls/bi-comercial/forward/ojdbc8.jar /usr/lib/sqoop/lib/ojdbc8.jar".format(
            emr_s3_bucket=EMR_S3_BUCKET
        ),
        retries=2,
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        trigger_rule=TriggerRule.ALL_DONE
    )



    # -------------------------------------------------------------------------------
    checkpoint_start_scoop_import = DummyOperator(
        task_id="checkpoint_start_scoop_import"
    )
    checkpoint_end_scoop_import = DummyOperator(
        task_id="checkpoint_end_scoop_import"
    )

    checkpoint_start_redshift_copy = DummyOperator(
        task_id="checkpoint_start_redshift_copy"
    )
    checkpoint_end_redshift_copy = DummyOperator(
        task_id="checkpoint_end_redshift_copy"
    )


    # for db, db_data in d.items():

    #     with TaskGroup(group_id=db, ui_color='#D7DBDD') as tg_1:

    #         for schema_name, data in db_data['schemas'].items():

    #             with TaskGroup(group_id=schema_name, parent_group=tg_1, ui_color='#D98880') as tg_2:

    #                 for table_name, tbs in data["tables"].items():
    #                     key=f"SQOOP_STEP_{db.upper()}_{tbs['table_name'].upper()}"
                        
    #                     step_sqoopimport = IBEmrAddStepsOperator(
    #                         task_id=f"extract_{tbs['table_name'].lower()}",
    #                         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    #                         command=f'{{{{ ti.xcom_pull(task_ids="init", key="{key}") }}}}',
    #                         retries=3,
    #                     )

    step_sqoopimport = IBEmrAddStepsOperator(
                            task_id=f"extract_test_bebil",
                            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
                            command=f'{{{{ ti.xcom_pull(task_ids="init", key="SQOOP_STEP_IBZIB_BEBIL.TBILCUP") }}}}',
                            retries=3,
    )
    checkpoint_start_scoop_import >> step_sqoopimport >> checkpoint_end_scoop_import


############################################################################################

######################################## TASK FLOW ########################################
init >> create_emr_cluster >> install_oracle_driver >> checkpoint_start_scoop_import
checkpoint_end_scoop_import >> terminate_cluster #>> checkpoint_start_redshift_copy >> redshift_task >> checkpoint_end_redshift_copy
