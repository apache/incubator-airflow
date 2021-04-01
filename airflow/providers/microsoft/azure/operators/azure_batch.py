#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from typing import Any, List, Optional

from azure.batch import models as batch_models

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_batch import AzureBatchHook
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes
class AzureBatchOperator(BaseOperator):
    """
    Executes a job on Azure Batch Service

    :param batch_pool_id: A string that uniquely identifies the Pool within the Account.
    :type batch_pool_id: str
    :param batch_pool_vm_size: The size of virtual machines in the Pool
    :type batch_pool_vm_size: str
    :param batch_job_id: A string that uniquely identifies the Job within the Account.
    :type batch_job_id: str
    :param batch_task_command_line: The command line of the Task
    :type batch_task_command_line: str
    :param batch_task_id: A string that uniquely identifies the task within the Job.
    :type batch_task_id: str
    :param batch_pool_display_name: The display name for the Pool.
        The display name need not be unique
    :type batch_pool_display_name: Optional[str]
    :param batch_job_display_name: The display name for the Job.
        The display name need not be unique
    :type batch_job_display_name: Optional[str]
    :param batch_job_manager_task: Details of a Job Manager Task to be launched when the Job is started.
    :type batch_job_manager_task: Optional[batch_models.JobManagerTask]
    :param batch_job_preparation_task: The Job Preparation Task. If set, the Batch service will
        run the Job Preparation Task on a Node before starting any Tasks of that
        Job on that Compute Node. Required if batch_job_release_task is set.
    :type batch_job_preparation_task: Optional[batch_models.JobPreparationTask]
    :param batch_job_release_task: The Job Release Task. Use to undo changes to Compute Nodes
        made by the Job Preparation Task
    :type batch_job_release_task: Optional[batch_models.JobReleaseTask]
    :param batch_task_display_name:  The display name for the task.
        The display name need not be unique
    :type batch_task_display_name: Optional[str]
    :param batch_task_container_settings: The settings for the container under which the Task runs
    :type batch_task_container_settings: Optional[batch_models.TaskContainerSettings]
    :param batch_start_task: A Task specified to run on each Compute Node as it joins the Pool.
        The Task runs when the Compute Node is added to the Pool or
        when the Compute Node is restarted.
    :type batch_start_task: Optional[batch_models.StartTask]
    :param batch_max_retries: The number of times to retry this batch operation before it's
        considered a failed operation. Default is 3
    :type batch_max_retries: int
    :param batch_task_resource_files: A list of files that the Batch service will
        download to the Compute Node before running the command line.
    :type batch_task_resource_files: Optional[List[batch_models.ResourceFile]]
    :param batch_task_output_files: A list of files that the Batch service will upload
        from the Compute Node after running the command line.
    :type batch_task_output_files: Optional[List[batch_models.OutputFile]]
    :param batch_task_user_identity: The user identity under which the Task runs.
        If omitted, the Task runs as a non-administrative user unique to the Task.
    :type batch_task_user_identity: Optional[batch_models.UserIdentity]
    :param target_low_priority_nodes: The desired number of low-priority Compute Nodes in the Pool.
        This property must not be specified if enable_auto_scale is set to true.
    :type target_low_priority_nodes: Optional[int]
    :param target_dedicated_nodes: The desired number of dedicated Compute Nodes in the Pool.
        This property must not be specified if enable_auto_scale is set to true.
    :type target_dedicated_nodes: Optional[int]
    :param enable_auto_scale: Whether the Pool size should automatically adjust over time. Default is false
    :type enable_auto_scale: bool
    :param auto_scale_formula: A formula for the desired number of Compute Nodes in the Pool.
        This property must not be specified if enableAutoScale is set to false.
        It is required if enableAutoScale is set to true.
    :type auto_scale_formula: Optional[str]
    :param azure_batch_conn_id: The connection id of Azure batch service
    :type azure_batch_conn_id: str
    :param use_latest_verified_vm_image_and_sku: Whether to use the latest verified virtual
        machine image and sku in the batch account. Default is false.
    :type use_latest_verified_vm_image_and_sku: bool
    :param vm_publisher: The publisher of the Azure Virtual Machines Marketplace Image.
        For example, Canonical or MicrosoftWindowsServer. Required if
        use_latest_image_and_sku is set to True
    :type vm_publisher: Optional[str]
    :param vm_offer: The offer type of the Azure Virtual Machines Marketplace Image.
        For example, UbuntuServer or WindowsServer. Required if
        use_latest_image_and_sku is set to True
    :type vm_offer: Optional[str]
    :param sku_starts_with: The starting string of the Virtual Machine SKU. Required if
        use_latest_image_and_sku is set to True
    :type sku_starts_with: Optional[str]
    :param vm_sku: The name of the virtual machine sku to use
    :type vm_sku: Optional[str]
    :param vm_version: The version of the virtual machine
    :param vm_version: Optional[str]
    :param vm_node_agent_sku_id: The node agent sku id of the virtual machine
    :type vm_node_agent_sku_id: Optional[str]
    :param os_family: The Azure Guest OS family to be installed on the virtual machines in the Pool.
    :type os_family: Optional[str]
    :param os_version: The OS family version
    :type os_version: Optional[str]
    :param timeout: The amount of time to wait for the job to complete in minutes. Default is 25
    :type timeout: int
    :param should_delete_job: Whether to delete job after execution. Default is False
    :type should_delete_job: bool
    :param should_delete_pool: Whether to delete pool after execution of jobs. Default is False
    :type should_delete_pool: bool
    """

    template_fields = (
        'batch_pool_id',
        'batch_pool_vm_size',
        'batch_job_id',
        'batch_task_id',
        'batch_task_command_line',
    )
    ui_color = '#f0f0e4'

    @apply_defaults
    def __init__(
        self,
        *,  # pylint: disable=too-many-arguments,too-many-locals
        batch_pool_id: str,
        batch_pool_vm_size: str,
        batch_job_id: str,
        batch_task_command_line: str,
        batch_task_id: str,
        vm_publisher: Optional[str] = None,
        vm_offer: Optional[str] = None,
        sku_starts_with: Optional[str] = None,
        vm_sku: Optional[str] = None,
        vm_version: Optional[str] = None,
        vm_node_agent_sku_id: Optional[str] = None,
        os_family: Optional[str] = None,
        os_version: Optional[str] = None,
        batch_pool_display_name: Optional[str] = None,
        batch_job_display_name: Optional[str] = None,
        batch_job_manager_task: Optional[batch_models.JobManagerTask] = None,
        batch_job_preparation_task: Optional[batch_models.JobPreparationTask] = None,
        batch_job_release_task: Optional[batch_models.JobReleaseTask] = None,
        batch_task_display_name: Optional[str] = None,
        batch_task_container_settings: Optional[batch_models.TaskContainerSettings] = None,
        batch_start_task: Optional[batch_models.StartTask] = None,
        batch_max_retries: int = 3,
        batch_task_resource_files: Optional[List[batch_models.ResourceFile]] = None,
        batch_task_output_files: Optional[List[batch_models.OutputFile]] = None,
        batch_task_user_identity: Optional[batch_models.UserIdentity] = None,
        target_low_priority_nodes: Optional[int] = None,
        target_dedicated_nodes: Optional[int] = None,
        enable_auto_scale: bool = False,
        auto_scale_formula: Optional[str] = None,
        azure_batch_conn_id='azure_batch_default',
        use_latest_verified_vm_image_and_sku: bool = False,
        timeout: int = 25,
        should_delete_job: bool = False,
        should_delete_pool: bool = False,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.batch_pool_id = batch_pool_id
        self.batch_pool_vm_size = batch_pool_vm_size
        self.batch_job_id = batch_job_id
        self.batch_task_id = batch_task_id
        self.batch_task_command_line = batch_task_command_line
        self.batch_pool_display_name = batch_pool_display_name
        self.batch_job_display_name = batch_job_display_name
        self.batch_job_manager_task = batch_job_manager_task
        self.batch_job_preparation_task = batch_job_preparation_task
        self.batch_job_release_task = batch_job_release_task
        self.batch_task_display_name = batch_task_display_name
        self.batch_task_container_settings = batch_task_container_settings
        self.batch_start_task = batch_start_task
        self.batch_max_retries = batch_max_retries
        self.batch_task_resource_files = batch_task_resource_files
        self.batch_task_output_files = batch_task_output_files
        self.batch_task_user_identity = batch_task_user_identity
        self.target_low_priority_nodes = target_low_priority_nodes
        self.target_dedicated_nodes = target_dedicated_nodes
        self.enable_auto_scale = enable_auto_scale
        self.auto_scale_formula = auto_scale_formula
        self.azure_batch_conn_id = azure_batch_conn_id
        self.use_latest_image = use_latest_verified_vm_image_and_sku
        self.vm_publisher = vm_publisher
        self.vm_offer = vm_offer
        self.sku_starts_with = sku_starts_with
        self.vm_sku = vm_sku
        self.vm_version = vm_version
        self.vm_node_agent_sku_id = vm_node_agent_sku_id
        self.os_family = os_family
        self.os_version = os_version
        self.timeout = timeout
        self.should_delete_job = should_delete_job
        self.should_delete_pool = should_delete_pool
        self.hook = self.get_hook()

    def _check_inputs(self) -> Any:
        if not self.os_family and not self.vm_publisher:
            raise AirflowException("You must specify either vm_publisher or os_family")
        if self.os_family and self.vm_publisher:
            raise AirflowException(
                "Cloud service configuration and virtual machine configuration "
                "are mutually exclusive. You must specify either of os_family and"
                " vm_publisher"
            )

        if self.use_latest_image:
            if not all(elem for elem in [self.vm_publisher, self.vm_offer]):
                raise AirflowException(
                    "If use_latest_image_and_sku is"
                    " set to True then the parameters vm_publisher, vm_offer, "
                    "must all be set. Found "
                    "vm_publisher={}, vm_offer={}".format(self.vm_publisher, self.vm_offer)
                )
        if self.vm_publisher:
            if not all([self.vm_sku, self.vm_offer, self.vm_node_agent_sku_id]):
                raise AirflowException(
                    "If vm_publisher is set, then the parameters vm_sku, vm_offer,"
                    "vm_node_agent_sku_id must be set. Found "
                    f"vm_publisher={self.vm_publisher}, vm_offer={self.vm_offer} "
                    f"vm_node_agent_sku_id={self.vm_node_agent_sku_id}, "
                    f"vm_version={self.vm_version}"
                )

        if not self.target_dedicated_nodes and not self.enable_auto_scale:
            raise AirflowException(
                "Either target_dedicated_nodes or enable_auto_scale must be set. None was set"
            )
        if self.enable_auto_scale:
            if self.target_dedicated_nodes or self.target_low_priority_nodes:
                raise AirflowException(
                    "If enable_auto_scale is set, then the parameters "
                    "target_dedicated_nodes and target_low_priority_nodes must not "
                    "be set. Found target_dedicated_nodes={},"
                    " target_low_priority_nodes={}".format(
                        self.target_dedicated_nodes, self.target_low_priority_nodes
                    )
                )
            if not self.auto_scale_formula:
                raise AirflowException("The auto_scale_formula is required when enable_auto_scale is set")
        if self.batch_job_release_task and not self.batch_job_preparation_task:
            raise AirflowException(
                "A batch_job_release_task cannot be specified without also "
                " specifying a batch_job_preparation_task for the Job."
            )
        if not all(
            [
                self.batch_pool_id,
                self.batch_job_id,
                self.batch_pool_vm_size,
                self.batch_task_id,
                self.batch_task_command_line,
            ]
        ):
            raise AirflowException(
                "Some required parameters are missing.Please you must set all the required parameters. "
            )

    def execute(self, context: dict) -> None:
        self._check_inputs()
        self.hook.connection.config.retry_policy = self.batch_max_retries

        pool = self.hook.configure_pool(
            pool_id=self.batch_pool_id,
            vm_size=self.batch_pool_vm_size,
            display_name=self.batch_pool_display_name,
            target_dedicated_nodes=self.target_dedicated_nodes,
            use_latest_image_and_sku=self.use_latest_image,
            vm_publisher=self.vm_publisher,
            vm_offer=self.vm_offer,
            sku_starts_with=self.sku_starts_with,
            vm_sku=self.vm_sku,
            vm_version=self.vm_version,
            vm_node_agent_sku_id=self.vm_node_agent_sku_id,
            os_family=self.os_family,
            os_version=self.os_version,
            target_low_priority_nodes=self.target_low_priority_nodes,
            enable_auto_scale=self.enable_auto_scale,
            auto_scale_formula=self.auto_scale_formula,
            start_task=self.batch_start_task,
        )
        self.hook.create_pool(pool)
        # Wait for nodes to reach complete state
        self.hook.wait_for_all_node_state(
            self.batch_pool_id,
            {
                batch_models.ComputeNodeState.start_task_failed,
                batch_models.ComputeNodeState.unusable,
                batch_models.ComputeNodeState.idle,
            },
        )
        # Create job if not already exist
        job = self.hook.configure_job(
            job_id=self.batch_job_id,
            pool_id=self.batch_pool_id,
            display_name=self.batch_job_display_name,
            job_manager_task=self.batch_job_manager_task,
            job_preparation_task=self.batch_job_preparation_task,
            job_release_task=self.batch_job_release_task,
        )
        self.hook.create_job(job)
        # Create task
        task = self.hook.configure_task(
            task_id=self.batch_task_id,
            command_line=self.batch_task_command_line,
            display_name=self.batch_task_display_name,
            container_settings=self.batch_task_container_settings,
            resource_files=self.batch_task_resource_files,
            output_files=self.batch_task_output_files,
            user_identity=self.batch_task_user_identity,
        )
        # Add task to job
        self.hook.add_single_task_to_job(job_id=self.batch_job_id, task=task)
        # Wait for tasks to complete
        self.hook.wait_for_job_tasks_to_complete(job_id=self.batch_job_id, timeout=self.timeout)
        # Clean up
        if self.should_delete_job:
            # delete job first
            self.clean_up(job_id=self.batch_job_id)
        if self.should_delete_pool:
            self.clean_up(self.batch_pool_id)

    def on_kill(self) -> None:
        response = self.hook.connection.job.terminate(
            job_id=self.batch_job_id, terminate_reason='Job killed by user'
        )
        self.log.info("Azure Batch job (%s) terminated: %s", self.batch_job_id, response)

    def get_hook(self) -> AzureBatchHook:
        """Create and return an AzureBatchHook."""
        return AzureBatchHook(azure_batch_conn_id=self.azure_batch_conn_id)

    def clean_up(self, pool_id: Optional[str] = None, job_id: Optional[str] = None) -> None:
        """
        Delete the given pool and job in the batch account

        :param pool_id: The id of the pool to delete
        :type pool_id: str
        :param job_id: The id of the job to delete
        :type job_id: str

        """
        if job_id:
            self.log.info("Deleting job: %s", job_id)
            self.hook.connection.job.delete(job_id)
        if pool_id:
            self.log.info("Deleting pool: %s", pool_id)
            self.hook.connection.pool.delete(pool_id)
