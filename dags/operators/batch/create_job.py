#
# Based on https://github.com/apache/airflow/blob/providers-amazon/2.1.0rc2/airflow/providers/amazon/aws/operators/batch.py
#
import sys
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

import boto3
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

sys.path.append("../../../plugins")
from operators.utils import get_object_from_s3

S3_BUCKET = "iberia-data-lake"

EXECUTION_TYPES_CPU = {
    "S": ["S", "M", "L"],
    "M": ["M", "L", "XL"],
    "L": ["L", "XL", "XXL"],
    "XL": ["XL", "XXL"],
    "XXL": ["XXL", "XXXL"],
}


class Costcodes(Enum):
    COMMERCIAL = "commercial"
    CUSTOMER = "customer"
    OPERATIONS = "operations"
    BI = "bi"
    BI_GOVERNANCE = "bi-governance"
    FINET = "finet"
    INTEGRATIONS = "integrations"
    INFRA = "infra"


class IBBatchCreateJobOperator(BaseOperator):
    """
    Execute a job on AWS Batch
    :param job_name: the name for the job that will run on AWS Batch (templated)
    :type job_name: str

    :param costcode: the name of the owning vertical of the system to which the DAG belongs
    :type costcode: str

    :param command: The command that's passed to the container. This parameter maps to Cmd in the Create a container section of the Docker Remote API and the COMMAND parameter to docker run.
    :type command: list

    :param execution_type_cpu: cpu from continer overrides (https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerOverride.html) [S, M, L, XL, XXL]
    :type parameters: str

    :parm execution_type_mem: mem from continer overrides (https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerOverride.html) [S, M, L, XL, XXL, XXXL]
    :type parameters: str

    :param check_if_exists: Flag that checks if the job is running before submitting
    :type parameters: Optional[bool]

    :param parameters: the `parameters` for boto3 (templated)
    :type parameters: Optional[str]

    :param environment: The environment variables to send to the container. You can add new environment variables, which are added to the container at launch, or you can override the existing environment variables from the Docker image or the task definition.Enviroment from continer overrides (https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerOverride.html)
    :type environment: Optional[dict]

    :param env_batch_role: The enviroment of execution. [production, sandbox]
    :type parameters: Optional[str]

    :param timeout: The time duration in seconds (measured from the job attempt's startedAt timestamp) after AWS Batch terminates unfinished jobs. The minimum value for the timeout is 60 seconds.
    :type parameters: Optional[int]

    :param waiters: an :py:class:`.AwsBatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :type waiters: Optional[AwsBatchWaiters]

    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :type max_retries: int

    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :type status_retries: int

    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :type aws_conn_id: str

    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str

    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted
    :type tags: dict

    .. note::
        Any custom waiters must return a waiter for these calls:
        .. code-block:: python
            waiter = waiters.get_waiter("JobExists")
            waiter = waiters.get_waiter("JobRunning")
            waiter = waiters.get_waiter("JobComplete")

    TODO LIST
    # entrypoint vs command vs args => command ~ command & args ~ parameters but command is mandatory¿?
    # use of xcoms => not posible k8s airflow implementation uses sidecar pattern that need a pod with multicontainer capabilities => https://gitlab.com/iberia-data/data-engineering/airflow-dags/-/blob/master/dags/finance_loads/sharepoint_utils/xlsx_functions.py TODO terminate README and CI/CD
    # bash commands uses
    # timeouts ~ The only thing to comment is that the timeout is not very useful if the systems are not stable (e.g. query queued for hours).
    # docs ~ from de original source
    """

    template_fields = [
        "job_flow_overrides",
        "overrides",
        "parameters",
        "command",
        "environment",
    ]

    template_ext = ()

    ui_color = "#5DADE2"

    def __init__(
        self,
        *,
        job_name: str,
        costcode: Costcodes,
        command: Optional[list] = None,
        execution_type_cpu: str = "S",
        execution_type_mem: str = "S",
        check_if_exists: Optional[bool] = False,
        parameters: Optional[dict] = None,
        environment: Optional[list] = None,
        env_batch_role: Optional[str] = "production",
        timeout: Optional[int] = 7200,
        waiters: Optional[Any] = None,
        max_retries: Optional[int] = None,
        status_retries: Optional[int] = None,
        aws_conn_id: str = "aws_default",
        region_name: str = "eu-west-1",
        tags: Optional[dict] = {
            "CostCode": None,
            "Project": None,
            "Responsible": None,
            "Owner": None,
        },
        ecr_image_arn: str,
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)

        self.costcode = costcode
        self.command = command
        self.environment = environment
        self.env_batch_role = env_batch_role.lower()
        self.timeout = timeout
        self.job_flow_overrides = {}
        self.job_name = job_name
        self.ecr_image_arn = ecr_image_arn
        self.execution_type_cpu = execution_type_cpu.upper()
        self.execution_type_mem = execution_type_mem.upper()
        self.check_if_exists = check_if_exists
        self.overrides = {}
        self.array_properties = {}
        self.parameters = parameters or {}
        self.waiters = waiters
        self.tags = tags or {}
        self.execution_role_arn: str = "arn:aws:iam::077156906314:role/ecsTaskExecutionRole"
        self.propagate_tags = True
        self.platform_capabilities = ["FARGATE"]
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.hook = BatchClientHook(
            max_retries=max_retries,
            status_retries=status_retries,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )
        self.job_id = None

    def _set_costcode(self, costcode):
        if isinstance(costcode, str):
            self.log.info("costcode is set as str use Costcode Enum")
            self.costcode = costcode
        else:
            self.costcode = costcode.value

    def _set_command(self, command):
        if isinstance(command, str):
            self.log.info("Using split 'command' as command param to aws_batch job")
            self.command = command.split(" ")
        else:
            self.log.info("command param is not defined, using command_list as command param to aws_batch job")
            self.command = command

    def execute(self, context: "Context"):
        """
        Submit and monitor an AWS Batch job

        :raises: AirflowException
        """

        self.prepare_params()
        self.create_job_definition(context)
        self.submit_job(context)
        self.monitor_job(context)

    def prepare_params(self):
        assert self.env_batch_role in [
            "production",
            "sandbox",
        ], "No valid value for env_batch_role, it must be in %s" % str(["production", "sandbox"])

        if self.env_batch_role == "sandbox":
            self.environment_short = "sbx"
        elif self.env_batch_role == "production":
            self.environment_short = "prod"

        self._set_costcode(self.costcode)
        self._set_command(self.command)

        self.job_name = "ibdata-aip-{env}-ew1-batch-{costcode}-{name}".format(
            costcode=self.costcode,
            name=self.job_name.replace("_", "-"),
            env=self.environment_short,
        )
        self.job_definition = (
            self.ecr_image_arn[self.ecr_image_arn.index("/") + 1 :].split(":")[0].replace("/", "-")
            + "_"
            + self.ecr_image_arn[self.ecr_image_arn.index("/") + 1 :].split(":")[1].replace(".", "-")
            + "-"
            + self.costcode
            + "-"
            + self.environment_short
        )
        self.job_queue = "ibdata-{costcode}-fargate-spot-{env}-queue".format(costcode=self.costcode, env=self.environment_short)

        self.jobRoleArn = "ibdata-aip-" + self.environment_short + "-role-batch-jobs-" + self.costcode

        assert self.execution_type_cpu.upper() in EXECUTION_TYPES_CPU.keys(), "No valid value for execution_type_cpu, it must be in %s" % str(
            EXECUTION_TYPES_CPU.keys()
        )

        resource_requirements_vcpu = get_object_from_s3(f"aws-batch/vcpu/{self.execution_type_cpu}.json", S3_BUCKET) or {}

        assert (
            self.execution_type_mem.upper() in EXECUTION_TYPES_CPU[self.execution_type_cpu.upper()]
        ), "No valid value for execution_type_mem with execution_type_cpu %s, it must be in %s" % (
            str(self.execution_type_cpu.upper()),
            str(EXECUTION_TYPES_CPU[self.execution_type_cpu.upper()]),
        )

        resource_requirements_memory = get_object_from_s3(f"aws-batch/memory/{self.execution_type_mem}.json", S3_BUCKET) or {}

        overrides = {
            "command": self.command,
            "resourceRequirements": [
                resource_requirements_vcpu,
                resource_requirements_memory,
            ],
            "environment": self.environment,
        }

        self.overrides = overrides or {}
        self.resource_requirements = [
            resource_requirements_vcpu,
            resource_requirements_memory,
        ]

    def check_if_job_exists(self, context: "Context"):
        client = boto3.client("batch", region_name="eu-west-1")

        describe_job_definiton_response = client.list_jobs(
            jobQueue=self.job_queue,
            filters=[
                {"name": "JOB_NAME", "values": [self.job_name]},
            ],
        )

        running_jobs = [i for i in describe_job_definiton_response["jobSummaryList"] if i["status"] == "RUNNING"]

        if len(running_jobs) > 0:
            return True
        else:
            return False

    def create_job_definition(self, context: "Context"):
        self.log.info("Creating AWS Job Definition: %s", self.job_definition)

        self.log.info(f"Job parameters: {str(self.parameters)}")

        client = boto3.client("batch", region_name=self.region_name)

        describe_job_definiton_response = client.describe_job_definitions(jobDefinitionName=self.job_definition)

        image_revision_and_role = [
            (
                i["containerProperties"]["image"],
                i["revision"],
                i["containerProperties"]["jobRoleArn"],
            )
            for i in describe_job_definiton_response["jobDefinitions"]
            if i["status"] == "ACTIVE"
        ]

        if (
            len(describe_job_definiton_response["jobDefinitions"]) == 0 or len(image_revision_and_role) == 0
        ):  # Si no existe una definición con ese nombre, se crea
            register_job_definition_response = client.register_job_definition(
                jobDefinitionName=self.job_definition,
                type="container",
                containerProperties={
                    "image": self.ecr_image_arn,
                    "command": [],
                    "executionRoleArn": self.execution_role_arn,
                    "resourceRequirements": self.resource_requirements,
                    "jobRoleArn": self.jobRoleArn,
                    "logConfiguration": {"logDriver": "awslogs"},
                },
                parameters=self.parameters,
                propagateTags=self.propagate_tags,
                timeout={"attemptDurationSeconds": self.timeout},
                tags=self.tags,
                platformCapabilities=self.platform_capabilities,
            )

            self.log.info("Job Definition created!")

        else:
            last_image, last_revision, role = max(image_revision_and_role, key=lambda item: item[1])
            self.log.info(last_image)

            if (last_image == self.ecr_image_arn) and (role == self.jobRoleArn):
                self.log.info("Job Definition alredy exists")
            else:  # Si la última definición activa no es la imagen que se ha pasado, se desactiva, y se crea una nueva revisión con la imagen nueva.
                response = client.deregister_job_definition(jobDefinition=self.job_definition + ":" + str(last_revision))

                register_job_definition_response = client.register_job_definition(
                    jobDefinitionName=self.job_definition,
                    type="container",
                    containerProperties={
                        "image": self.ecr_image_arn,
                        "command": [],
                        "executionRoleArn": self.execution_role_arn,
                        "resourceRequirements": self.resource_requirements,
                        "jobRoleArn": self.jobRoleArn,
                        "logConfiguration": {"logDriver": "awslogs"},
                    },
                    propagateTags=self.propagate_tags,
                    timeout={"attemptDurationSeconds": self.timeout},
                    tags=self.tags,
                    platformCapabilities=self.platform_capabilities,
                )

                self.log.info("Job Definition updated!")

    def on_kill(self):
        # response = self.hook.client.terminate_job(jobId=self.job_id, reason="Task killed by the user")
        # self.log.info("AWS Batch job (%s) terminated: %s", self.job_id, response)
        self.log.info("AWS Batch Task marked as failed")

    def submit_job(self, context: "Context"):
        """
        Submit an AWS Batch job

        :raises: AirflowException
        """
        self.log.info(
            "Running AWS Batch job - job definition: %s - on queue %s",
            self.job_definition,
            self.job_queue,
        )
        self.log.info("AWS Batch job - container overrides: %s", self.overrides)

        try:
            if self.check_if_exists:
                exists, response = self.check_if_job_exists(context)
            else:
                exists = False

            if not exists:
                response = self.hook.client.submit_job(
                    jobName=self.job_name,
                    jobQueue=self.job_queue,
                    jobDefinition=self.job_definition,
                    arrayProperties=self.array_properties,
                    parameters=self.parameters,
                    containerOverrides=self.overrides,
                    timeout={"attemptDurationSeconds": self.timeout},
                    tags=self.tags,
                )

                self.log.info("AWS Batch job (%s) started: %s", self.job_id, response)
            else:
                self.log.info("AWS Batch job (%s) alredy exists.", self.job_id)

            self.job_id = response["jobId"]

            # response_job = self.hook.get_job_description(self.job_id)
            # print(response_job)
            # response_logs = self.hook.client.get_job_awslogs_info(self.job_id)
            # print(response_logs)

        except Exception as e:
            self.log.error("AWS Batch job (%s) failed submission", self.job_id)
            raise AirflowException(e)

    def read_logs(self, context: "Context"):
        client = boto3.client("logs", region_name=self.region_name)

        job = self.hook.get_job_description(self.job_id)
        log_stream_name = job["attempts"][0]["container"]["logStreamName"]
        print("Log Stream Name: ", log_stream_name)

        log_group = "/aws/batch/job"

        response = client.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=log_stream_name,
            descending=True,
            limit=50,
        )

        # print("Streams in Cloudwatch: ", response["logStreams"])
        self.log.info("Looking for Log Group " + log_group + " and Log Stream " + log_stream_name)

        if (len(response["logStreams"]) > 0) and (log_stream_name in response["logStreams"][0]["logStreamName"]):
            response = client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream_name,
                startTime=int((datetime.now() - timedelta(hours=10)).timestamp()) * 1000,
                endTime=int((datetime.now() + timedelta(hours=10)).timestamp()) * 1000,
                limit=10000,
                startFromHead=True,
            )

            [self.log.info(i["message"]) for i in response["events"]]
        else:
            self.log.info("There are no logs!!!")

    def monitor_job(self, context: "Context"):
        """
        Monitor an AWS Batch job
        monitor_job can raise an exception or an AirflowTaskTimeout can be raised if execution_timeout
        is given while creating the task. These exceptions should be handled in taskinstance.py
        instead of here like it was previously done

        :raises: AirflowException
        """
        if not self.job_id:
            raise AirflowException("AWS Batch job - job_id was not found")

        self.log.info(self.job_id)

        if self.waiters:
            self.waiters.wait_for_job(self.job_id)
        else:
            self.hook.wait_for_job(self.job_id)

        job = self.hook.get_job_description(self.job_id)
        job_status = job.get("status")

        if job_status == "SUCCEEDED":
            self.read_logs(context)
            self.log.info("AWS Batch job (%s) succeeded: %s", self.job_id, job)
            return True

        if job_status == "FAILED":
            self.read_logs(context)
            raise AirflowException(f"AWS Batch job ({self.job_id}) failed: {job}")

        if job_status in "RUNNING":
            self.read_logs(context)
            raise AirflowException(f"AWS Batch job ({self.job_id}) is not complete: {job}")

        raise AirflowException(f"AWS Batch job ({self.job_id}) has unknown status: {job}")
