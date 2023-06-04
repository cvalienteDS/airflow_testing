import sys

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from typing import TYPE_CHECKING, Any, Optional, Sequence
import boto3
from enum import Enum

sys.path.append("../../../plugins")
from operators.utils import add_to_dictionary, get_object_from_s3

S3_BUCKET = "prdeu-ibr-emr-s3"
EXECUTION_TYPES = [
    "high-memory",
    "high-memory-high-storage",
    "high-cpu",
    "high-parallelism",
    "standard",
    "ifar",
    "test",
]


class Costcodes(Enum):
    COMMERCIAL = "commercial"
    CUSTOMER = "customer"
    OPERATIONS = "operations"
    BI = "bi"
    FINET = "finet"
    INTEGRATIONS = "integrations"
    INFRA = "infra"


def add_auto_scaling_policy(self, cluster_id):
    policy = (
        get_object_from_s3(
            f"instances_fleet/config/scaling_policies/{self.execution_type}.json",
            S3_BUCKET,
        )
        or {}
    )

    client = boto3.client("emr", region_name="eu-west-1")

    client.put_managed_scaling_policy(ClusterId=cluster_id, ManagedScalingPolicy=policy)


class IBEmrCreateJobFlowOperator(BaseOperator):
    template_fields = ["job_flow_overrides"]

    template_ext = ()

    ui_color = "#f9c915"

    def __init__(
        self,
        process_name: str,
        costcode: Costcodes,
        release_label: str,
        execution_type: str,
        testing_sbx_mode: bool = False,  # testing sandbox mode is used to execute in sandbox with prod configuration
        applications: list = ["Spark", "Hive", "Tez"],
        keep_job_flow_alive_when_no_steps: bool = False,
        termination_protected: bool = False,
        visible_to_all_users: bool = True,
        ebs_root_volume_size: int = 100,
        step_concurrency_level: int = 1,
        s3_maximum_connections: int = 100,
        environment: Optional[str] = "production",
        classification_configurations: list = ["hive-site"],
        bootstrap_actions: list = [],  # Path in S3_BUCKET where .sh is
        ec2_subnet_ids: list = [],
        tags: dict = {
            "CostCode": None,
            "Project": None,
            "Responsible": None,
            "Owner": None,
        },
        aws_conn_id: str = "aws_default",
        emr_conn_id: str = "emr_default",
        region_name: str = None,
        check_if_exist: bool = True,
        idle_timeout_seconds: int = 1800,  # Minimum of 60 seconds and a maximum of 604800 seconds
        *args,
        **kwargs,
    ):
        super(IBEmrCreateJobFlowOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.job_flow_overrides = {}
        self.region_name = region_name
        ################################
        self.process_name = process_name.replace("_", "-")
        self.costcode = costcode
        self.release_label = release_label
        self.environment = environment
        self.testing_sbx_mode = testing_sbx_mode
        self.execution_type = execution_type
        self.applications = applications
        self.keep_job_flow_alive_when_no_steps = keep_job_flow_alive_when_no_steps
        self.termination_protected = termination_protected
        self.visible_to_all_users = visible_to_all_users
        self.ebs_root_volume_size = ebs_root_volume_size
        self.step_concurrency_level = step_concurrency_level
        self.s3_maximum_connections = s3_maximum_connections
        self.classification_configurations = classification_configurations
        self.bootstrap_actions = bootstrap_actions
        self.tags = tags
        self.check_if_exist = check_if_exist
        self.idle_timeout_seconds = idle_timeout_seconds
        self.ec2_subnet_ids = ec2_subnet_ids

    def _set_costcode(self, costcode):
        if isinstance(costcode, str):
            self.log.info("costcode is set as str use Costcode Enum")
            self.costcode = costcode
        else:
            self.costcode = costcode.value

    def prepare_params(self, context):
        self.env = "prod" if self.environment == "production" else "sbx"

        self._set_costcode(self.costcode)

        self.cluster_name = "ibdata-aip-{env}-ew1-emr-{costcode}-{process_name}".format(
            env=self.env, costcode=self.costcode, process_name=self.process_name
        )

        self.launch_mode_message = f"Generate EMR Cluster using {self.execution_type} mode"

        # On SANDBOX always use defult mode except testing sandbox mode is True, to execute in sandbox with prod configuration
        if self.environment == "sandbox":
            if not self.testing_sbx_mode:
                self.launch_mode_message = (
                    f"[SET DEVELOP SANDBOX MODE]Because is on sandbox, mode is changed to develop but was requested as {self.execution_type}"
                )
                self.execution_type = "development"
            else:
                self.launch_mode_message = f"[AVOID DEVELOP SANDBOX MODE]Despite being on sandbox, mode keeps as {self.execution_type} because testing_sbx_mode is set to true"

    def execute(self, context):

        self.prepare_params(context)

        # To trace this note in step log box is generated in __input__ but print here
        self.log.info(f"{self.launch_mode_message}")

        emr = EmrHook(
            aws_conn_id=self.aws_conn_id,
            emr_conn_id=self.emr_conn_id,
            region_name=self.region_name,
        )

        ############# Check if there are any clusters running #############

        if self.check_if_exist is True:
            job_flow_id = emr.get_cluster_id_by_name(
                self.cluster_name, ["RUNNING", "WAITING", "STARTING", "BOOTSTRAPPING"]
            )  # self.cluster_states en vez de None
            if job_flow_id is not None:
                return job_flow_id

        ###################################################################

        self.log.info(
            "Creating JobFlow using aws-conn-id: %s, emr-conn-id: %s",
            self.aws_conn_id,
            self.emr_conn_id,
        )

        ######################### First level config #########################

        self.job_flow_overrides["Name"] = self.cluster_name
        self.job_flow_overrides["LogUri"] = f"s3n://{S3_BUCKET}/logs/"
        self.job_flow_overrides["ReleaseLabel"] = self.release_label
        self.job_flow_overrides["VisibleToAllUsers"] = self.visible_to_all_users
        self.job_flow_overrides["EbsRootVolumeSize"] = self.ebs_root_volume_size
        if self.step_concurrency_level > 1:
            self.job_flow_overrides["StepConcurrencyLevel"] = self.step_concurrency_level

        assert self.applications, "No valid value for applications: %s" % str(self.applications)

        self.job_flow_overrides["Applications"] = [{"Name": app.lower().capitalize()} for app in self.applications]

        ######################### Instances #########################
        assert self.execution_type.lower() in EXECUTION_TYPES, "No valid value for execution_type, it must be in %s" % str(EXECUTION_TYPES)

        # Json in S3 with Instance Group or InstanceFleets
        self.log.info("Reading instances configuration")

        instances = (
            get_object_from_s3(
                f"instances_fleet/config/instances/{self.execution_type}.json",
                S3_BUCKET,
            )
            or {}
        )

        assert "InstanceGroups" in instances.keys() or "InstanceFleets" in instances.keys(), (
            "File %s is not valid" % f"s3://{S3_BUCKET}/instances_fleet/config/instances/{self.execution_type}.json"
        )

        self.job_flow_overrides["Instances"] = {}

        self.job_flow_overrides["Instances"] = {
            **{},
            **instances,
        }

        self.job_flow_overrides["Instances"]["TerminationProtected"] = self.termination_protected

        self.job_flow_overrides["Instances"]["KeepJobFlowAliveWhenNoSteps"] = self.keep_job_flow_alive_when_no_steps

        # In this json will be info like roles, key names, networking and temporarily Secret with RDS credentials
        base_config = get_object_from_s3(f"instances_fleet/config/base/{self.environment}.json", S3_BUCKET) or {}

        assert "Ec2KeyName" in base_config, f"Ec2KeyName not found in s3://{S3_BUCKET}/instances_fleet/config/base/{self.environment}.json"

        if self.ec2_subnet_ids:
            self.job_flow_overrides["Instances"]["Ec2SubnetIds"] = self.ec2_subnet_ids
        else:
            self.job_flow_overrides["Instances"]["Ec2SubnetIds"] = base_config["Ec2SubnetIds"]

        self.job_flow_overrides["Instances"]["Ec2KeyName"] = base_config["Ec2KeyName"]

        ######################### More first level config #########################

        assert "JobFlowRole" in base_config, f"JobFlowRole not found in s3://{S3_BUCKET}/instances_fleet/config/base/{self.environment}.json"

        self.job_flow_overrides["JobFlowRole"] = base_config["JobFlowRole"]

        assert "ServiceRole" in base_config, f"ServiceRole not found in s3://{S3_BUCKET}/instances_fleet/config/base/{self.environment}.json"

        self.job_flow_overrides["ServiceRole"] = base_config["ServiceRole"]

        ######################### Configuration #########################

        configurations = []

        # By now it adds 'hive-site' by default to connect to Hive Metastore RDS
        # TODO: migrate to Glue DataCatalog by environment
        if "hive-site" not in self.classification_configurations:
            self.classification_configurations.append("hive-site")

        for classification in self.classification_configurations:
            # Se coge las credenciales de AWS Secrets Manager
            if classification == "hive-site":
                # Si no esta el nombre del secreto en la configuracion basica
                assert "HiveMetastoreAirflowConnection" in base_config, "There is no HiveMetastoreAirflowConnection in base_config"

                secret = BaseHook.get_connection(base_config["HiveMetastoreAirflowConnection"])

                c = {}
                c["Classification"] = classification
                c["Properties"] = {
                    "javax.jdo.option.ConnectionURL": f"jdbc:mysql://{secret.host}/{secret.schema}",
                    "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                    "javax.jdo.option.ConnectionUserName": f"{secret.login}",
                    "javax.jdo.option.ConnectionPassword": f"{secret.password}",
                    "fs.s3a.connection.maximum": str(self.s3_maximum_connections),
                    "hive.support.concurrency": "true",
                    "hive.vectorized.execution.enabled": "false",
                    "hive.exec.dynamic.partition.mode": "nonstrict",
                    "hive.exec.max.dynamic.partitions": "50000",
                    "hive.exec.max.dynamic.partitions.pernode": "500",
                    "hive.txn.manager": "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager",
                    "datanucleus.connectionPool.maxPoolSize": "5",
                    "hive.compactor.initiator.on": "true",
                }

                # Add custom properties by execution type
                try:
                    aux_properties = (
                        get_object_from_s3(
                            f"instances_fleet/config/classifications/{self.execution_type}/{classification}.json",
                            S3_BUCKET,
                        )
                        or {}
                    )

                    # delete classification if exists
                    if not (aux_properties) == False:
                        if "Classification" in aux_properties:
                            del aux_properties["Classification"]
                    # if there are still elements, add them
                    if not (aux_properties) == False:
                        for name, value in aux_properties.items():
                            add_to_dictionary(c, name, value)
                except:
                    None  # if fails, just not add properties
                configurations.append(c)

            else:
                c = (
                    get_object_from_s3(
                        f"instances_fleet/config/classifications/{self.execution_type}/{classification}.json",
                        S3_BUCKET,
                    )
                    or {}
                )

                assert (
                    "Classification" in c or c["Classification"] == classification
                ), f"The json s3://{S3_BUCKET}/instances_fleet/config/classifications/{self.execution_type}/{classification}.json has conflicts or it has not a right structure: \n{str(c)}"

                configurations.append(c)

        self.job_flow_overrides["Configurations"] = configurations

        ######################### Tags #########################

        assert all(self.tags), "CostCode, Project, Responsible, Owner have to be set in tags parameter."

        self.tags["Name"] = self.cluster_name
        self.tags["Environment"] = self.environment
        self.tags["ClusterType"] = self.execution_type

        self.job_flow_overrides["Tags"] = [{"Key": key, "Value": value} for key, value in self.tags.items()]
        self.log.info(str(self.job_flow_overrides["Instances"]))

        if self.bootstrap_actions != []:
            bootstrap_actions = []
            n = 1
            for boot in self.bootstrap_actions:
                b = {
                    "Name": f"Bootstrap - {n} - {self.cluster_name}",
                    "ScriptBootstrapAction": {
                        "Path": f"s3://{S3_BUCKET}/code/{boot}",
                    },
                }

                bootstrap_actions.append(b)
                n = n + 1

            self.job_flow_overrides["BootstrapActions"] = bootstrap_actions

        self.job_flow_overrides["AutoTerminationPolicy"] = {"IdleTimeout": self.idle_timeout_seconds}

        response = emr.create_job_flow(self.job_flow_overrides)

        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException("JobFlow creation failed: %s" % response)
        else:
            self.log.info("JobFlow with id %s created", response["JobFlowId"])
            context["ti"].xcom_push(key="job_flow_id", value=response["JobFlowId"])

        self.log.info("Adding managed scaling policy...")

        add_auto_scaling_policy(self, response["JobFlowId"])

        return response["JobFlowId"]
