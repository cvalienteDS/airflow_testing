# emr_add_step.py
import time
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from botocore.auth import NoCredentialsError
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError as NoCredentialsErrorBotocore

ACTIONS_ON_FAILURE = ["TERMINATE_CLUSTER", "CANCEL_AND_WAIT", "CONTINUE"]


class IBEmrAddStepsOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :type job_flow_id: str
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :type job_flow_name: str
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :type cluster_states: list
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param steps: boto3 style steps to be added to the jobflow. (templated)
    :type steps: list
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    :type do_xcom_push: bool
    """

    non_terminal_states = {"PENDING", "RUNNING"}
    failed_states = {"FAILED", "CANCELLED", "INTERRUPTED", "CANCEL_PENDING"}

    template_fields = ["job_flow_id", "job_flow_name", "cluster_states", "command"]

    template_ext = ()

    ui_color = "#f9c915"

    # @apply_defaults
    def __init__(
        self,
        job_flow_id=None,
        job_flow_name=None,
        cluster_states=["RUNNING", "WAITING", "STARTING", "BOOTSTRAPPING"],
        aws_conn_id="aws_default",
        action_on_failure: str = "CONTINUE",
        wait_for_completion: bool = True,
        check_interval: int = 30,
        command=None,
        check_if_exist: bool = True,
        retries: int = 5,
        *args,
        **kwargs,
    ):
        if not ((job_flow_id is None) ^ (job_flow_name is None)):
            raise AirflowException("Exactly one of job_flow_id or job_flow_name must be specified.")
        super(IBEmrAddStepsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.job_flow_name = job_flow_name
        self.cluster_states = cluster_states
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.action_on_failure = action_on_failure
        self.retries = retries
        assert command, f"Not valid command {command}"
        assert action_on_failure in ACTIONS_ON_FAILURE, f"action_on_failure must be in {str(ACTIONS_ON_FAILURE)}"
        self.command = command
        self.check_if_exist = check_if_exist
        self.no_credentials_errors = 20
        self.client_errors = 5

    # Returns emr hook and job_flow_id
    def get_cluster(self):
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()
        job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(self.job_flow_name, self.cluster_states)
        if not job_flow_id:
            if self.cluster_states is not None:
                raise AirflowException(f"No cluster found for name: {self.job_flow_name} with this state: {self.cluster_states}")
            raise AirflowException("No cluster found for name: " + self.job_flow_name)

        return emr, job_flow_id

    def get_step(self, emr, job_flow_id, is_first_try):
        # self.log.info(self.retries, self.max_tries, self.try_number)
        w_loop = True
        self.log.info("Trying to get steps from cluster %s.", job_flow_id)
        while w_loop:
            try:
                response = emr.list_steps(ClusterId=job_flow_id)
                w_loop = False
                response_steps = sorted(
                    response.get("Steps", []),
                    key=lambda s: s.get("Status", {}).get("Timeline", {}).get("CreationDateTime"),
                )
                steps = []
                for s in response_steps:
                    if s.get("Name", None) == self.task_id:
                        if s.get("Config", {}).get("Jar", None) == "command-runner.jar":
                            if len(s.get("Config", {}).get("Args", [])) > 2 and s["Config"]["Args"][2] == self.command:
                                if s.get("Status", {}).get("State", None) in self.non_terminal_states:
                                    steps.append(s["Id"])
                                elif s.get("Status", {}).get("State", None) not in self.failed_states and not is_first_try:
                                    steps.append(s["Id"])
            except ClientError:
                self.log.warn("AWS request failed, check logs for more info")
                self.client_errors -= 1
                if self.client_errors >= 0:
                    self.log.warn(f"Sleeping for 3 minutes due to BotoCore Client Error, {self.client_errors} tries remaining")
                    time.sleep(60 * 3)  # Sleep during 3 minutes because Client Errors can be due to API Rate Exceeded
                    continue
                else:
                    self.log.exception("Couldn't get steps for cluster %s.", job_flow_id)
            except NoCredentialsError:
                self.no_credentials_errors -= 1
                self.log.warn("Unable to locate credentials")
                if self.no_credentials_errors >= 0:
                    time.sleep(self.check_interval)
                    continue
                else:
                    self.log.exception("Couldn't get steps for cluster %s.", job_flow_id)
            except NoCredentialsErrorBotocore:
                self.no_credentials_errors -= 1
                self.log.warn("Unable to locate credentials")
                if self.no_credentials_errors >= 0:
                    continue
                else:
                    self.log.exception("Couldn't get steps for cluster %s.", job_flow_id)
            finally:
                self.log.info("Got %s steps for cluster %s.", len(steps), job_flow_id)
                return steps

    def execute(self, context):

        emr, job_flow_id = self.get_cluster()

        if self.do_xcom_push:
            context["ti"].xcom_push(key="job_flow_id", value=job_flow_id)

        self.log.info("Adding steps to %s", job_flow_id)

        steps = [
            {
                "Name": self.task_id,
                "ActionOnFailure": self.action_on_failure,
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    # "Args": self.command.split(),
                    "Args": ["bash", "-c", self.command],
                },
            }
        ]
        step_ids = []
        if self.check_if_exist:
            is_first_try = (context["ti"].max_tries - context["ti"].task.retries + 1) == context["ti"].try_number
            step_ids = self.get_step(emr, job_flow_id, is_first_try)

        w_loop = len(step_ids) == 0

        while w_loop:  # loop adding step if got credentials exception
            try:
                response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)
                w_loop = False
            except KeyError:
                raise AirflowException("Could not add step to the EMR job")
            except ClientError:
                self.log.warn("AWS request failed, check logs for more info")
                self.client_errors -= 1
                if self.client_errors >= 0:
                    self.log.warn(f"Sleeping for 3 minutes due to BotoCore Client Error, {self.client_errors} tries remaining")
                    time.sleep(60 * 3)  # Sleep during 3 minutes because Client Errors can be due to API Rate Exceeded
                    continue
                else:
                    raise AirflowException("You have experienced 5 ClientError")

            except NoCredentialsError:
                self.no_credentials_errors -= 1
                self.log.warn("Unable to locate credentials")
                if self.no_credentials_errors >= 0:
                    time.sleep(self.check_interval)
                    continue
                else:
                    raise AirflowException("You have experienced 10 NoCredentialsError")
            except NoCredentialsErrorBotocore:
                self.no_credentials_errors -= 1
                self.log.warn("Unable to locate credentials")
                if self.no_credentials_errors >= 0:
                    continue
                else:
                    raise AirflowException("You have experienced 20 NoCredentialsError")

        if len(step_ids) == 0 and not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException("Adding steps failed: %s" % response)
        else:
            if len(step_ids) == 0:
                self.log.info("Steps %s added to JobFlow", response["StepIds"])
                # Assumption : ONly a single step is submitted each time.
                step_ids = response["StepIds"]
            else:
                self.log.info("Steps %s found already in cluster", step_ids)
            self.step_id = step_ids[0]
            if self.wait_for_completion:
                self.check_status(
                    job_flow_id,
                    self.step_id,
                    self.describe_step,
                    self.check_interval,
                )
            return step_ids

    def check_status(
        self,
        job_flow_id: str,
        step_id: str,
        describe_function: Callable,
        check_interval: int,
        max_ingestion_time: Optional[int] = None,
        non_terminal_states: Optional[Set] = None,
    ):
        """
        Check status of a EMR Step
        :param job_flow_id: name of the Cluster to check status
        :type job_flow_id: str
        :param step_id: the Step Id
            that points to the Job
        :type step_id: str
        :param describe_function: the function used to retrieve the status
        :type describe_function: python callable
        :param args: the arguments for the function
        :param check_interval: the time interval in seconds which the operator
            will check the status of any EMR job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            EMR jobs that run longer than this will fail. Setting this to
            None implies no timeout for any EMR job.
        :type max_ingestion_time: int
        :param non_terminal_states: the set of nonterminal states
        :type non_terminal_states: set
        :return: response of describe call after job is done
        """
        if not non_terminal_states:
            non_terminal_states = self.non_terminal_states

        sec = 0
        running = True
        self.no_credentials_errors = 20
        self.client_errors = 5
        while running:
            time.sleep(check_interval)
            sec += check_interval

            try:
                response = describe_function(job_flow_id, step_id)
                status = response["Step"]["Status"]["State"]
                self.log.info(
                    "Job still running for %s seconds... " "current status is %s",
                    sec,
                    status,
                )
                self.no_credentials_errors = 20
                self.client_errors = 5
            except KeyError:
                raise AirflowException("Could not get status of the EMR job")
            except ClientError:
                self.log.warn("AWS request failed, check logs for more info")
                self.client_errors -= 1
                if self.client_errors >= 0:
                    self.log.warn(f"Sleeping during 3 minutes due to BotoCore Client Error, there are still {self.client_errors} to fail")
                    time.sleep(60 * 3)  # Sleep during 3 minutes because Client Errors can be due to API Rate Exceeded
                    continue
                else:
                    raise AirflowException("You have experienced 5 ClientError")
            except NoCredentialsError:
                self.no_credentials_errors -= 1
                self.log.warn("Unable to locate credentials")
                if self.no_credentials_errors >= 0:
                    continue
                else:
                    raise AirflowException("You have experienced 20 NoCredentialsError")
            except NoCredentialsErrorBotocore:
                self.no_credentials_errors -= 1
                self.log.warn("Unable to locate credentials")
                if self.no_credentials_errors >= 0:
                    continue
                else:
                    raise AirflowException("You have experienced 20 NoCredentialsError")

            if status in non_terminal_states:
                running = True
            elif status in self.failed_states:
                raise AirflowException("EMR Step failed because %s" % str(response["Step"]["Status"]["FailureDetails"]))
            else:
                running = False

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException(f"EMR job took more than {max_ingestion_time} seconds")

        self.log.info("EMR Job completed")
        # response = describe_function(job_flow_id, step_id)
        return response

    def describe_step(self, clusterid: str, stepid: str) -> dict:
        """
        Return the transform job info associated with the name
        :param clusterid: EMR Cluster ID
        :type stepid: str: StepID
        :return: A dict contains all the transform job info
        """
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()
        return emr.describe_step(ClusterId=clusterid, StepId=stepid)

    # action on time out https://stackoverflow.com/questions/50054777/airflow-task-is-not-stopped-when-execution-timeout-gets-triggered
    def on_kill(self):

        # when time out
        self.log.info(f"Timeout: Cancel step {self.step_id}")
        emr, job_flow_id = self.get_cluster()

        response = emr.cancel_steps(
            ClusterId=job_flow_id,
            StepIds=[self.step_id],  # only cancel one step
            StepCancellationOption="TERMINATE_PROCESS",  # 'SEND_INTERRUPT' available too
        )

        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException(f"Cancel steps failed: {response}")
        else:
            self.log.info(f"Step  {self.step_id} cancel from JobFlow")

        # super.on_kill
