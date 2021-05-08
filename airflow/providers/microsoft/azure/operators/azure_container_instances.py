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

import re
from collections import namedtuple
from time import sleep
from typing import Any, Dict, List, Optional, Sequence, Union

from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerPort,
    EnvironmentVariable,
    IpAddress,
    ResourceRequests,
    ResourceRequirements,
    VolumeMount,
)
from msrestazure.azure_exceptions import CloudError

from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_container_instance import AzureContainerInstanceHook
from airflow.providers.microsoft.azure.hooks.azure_container_registry import AzureContainerRegistryHook
from airflow.providers.microsoft.azure.hooks.azure_container_volume import AzureContainerVolumeHook

Volume = namedtuple(
    'Volume',
    ['conn_id', 'account_name', 'share_name', 'mount_path', 'read_only'],
)


DEFAULT_ENVIRONMENT_VARIABLES: Dict[str, str] = {}
DEFAULT_SECURED_VARIABLES: Sequence[str] = []
DEFAULT_VOLUMES: Sequence[Volume] = []
DEFAULT_MEMORY_IN_GB = 2.0
DEFAULT_CPU = 1.0


# pylint: disable=too-many-instance-attributes
class AzureContainerInstancesOperator(BaseOperator):
    """
    Start a container on Azure Container Instances

    :param ci_conn_id: connection id of a service principal which will be used
        to start the container instance
    :type ci_conn_id: str
    :param registry_conn_id: connection id of a user which can login to a
        private docker registry. For Azure use :ref:`Azure connection id<howto/connection:azure>`
    :type azure_conn_id: str If None, we assume a public registry
    :type registry_conn_id: Optional[str]
    :param resource_group: name of the resource group wherein this container
        instance should be started
    :type resource_group: str
    :param name: name of this container instance. Please note this name has
        to be unique in order to run containers in parallel.
    :type name: str
    :param image: the docker image to be used
    :type image: str
    :param region: the region wherein this container instance should be started
    :type region: str
    :param environment_variables: key,value pairs containing environment
        variables which will be passed to the running container
    :type environment_variables: Optional[dict]
    :param secured_variables: names of environmental variables that should not
        be exposed outside the container (typically passwords).
    :type secured_variables: Optional[str]
    :param volumes: list of ``Volume`` tuples to be mounted to the container.
        Currently only Azure Fileshares are supported.
    :type volumes: list[<conn_id, account_name, share_name, mount_path, read_only>]
    :param memory_in_gb: the amount of memory to allocate to this container
    :type memory_in_gb: double
    :param cpu: the number of cpus to allocate to this container
    :type cpu: double
    :param gpu: GPU Resource for the container.
    :type gpu: azure.mgmt.containerinstance.models.GpuResource
    :param command: the command to run inside the container
    :type command: Optional[List[str]]
    :param container_timeout: max time allowed for the execution of
        the container instance.
    :type container_timeout: datetime.timedelta
    :param tags: azure tags as dict of str:str
    :type tags: Optional[dict[str, str]]
    :param os_type: The operating system type required by the containers
        in the container group. Possible values include: 'Windows', 'Linux'
    :type os_type: str
    :param restart_policy: Restart policy for all containers within the container group.
        Possible values include: 'Always', 'OnFailure', 'Never'
    :type restart_policy: str
    :param ip_address: The IP address type of the container group.
    :type ip_address: IpAddress

    **Example**::

                AzureContainerInstancesOperator(
                    ci_conn_id = "azure_service_principal",
                    registry_conn_id = "azure_registry_user",
                    resource_group = "my-resource-group",
                    name = "my-container-name-{{ ds }}",
                    image = "myprivateregistry.azurecr.io/my_container:latest",
                    region = "westeurope",
                    environment_variables = {"MODEL_PATH":  "my_value",
                     "POSTGRES_LOGIN": "{{ macros.connection('postgres_default').login }}",
                     "POSTGRES_PASSWORD": "{{ macros.connection('postgres_default').password }}",
                     "JOB_GUID": "{{ ti.xcom_pull(task_ids='task1', key='guid') }}" },
                    secured_variables = ['POSTGRES_PASSWORD'],
                    volumes = [("azure_wasb_conn_id",
                            "my_storage_container",
                            "my_fileshare",
                            "/input-data",
                        True),],
                    memory_in_gb=14.0,
                    cpu=4.0,
                    gpu=GpuResource(count=1, sku='K80'),
                    command=["/bin/echo", "world"],
                    task_id="start_container"
                )
    """

    template_fields = ('name', 'image', 'command', 'environment_variables')
    template_fields_renderers = {"command": "bash", "environment_variables": "json"}

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        *,
        ci_conn_id: str,
        registry_conn_id: Optional[str],
        resource_group: str,
        name: str,
        image: str,
        region: str,
        environment_variables: Optional[dict] = None,
        secured_variables: Optional[str] = None,
        volumes: Optional[list] = None,
        memory_in_gb: Optional[Any] = None,
        cpu: Optional[Any] = None,
        gpu: Optional[Any] = None,
        command: Optional[List[str]] = None,
        remove_on_error: bool = True,
        fail_if_exists: bool = True,
        tags: Optional[Dict[str, str]] = None,
        os_type: str = 'Linux',
        restart_policy: str = 'Never',
        ip_address: Optional[IpAddress] = None,
        ports: Optional[List[ContainerPort]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.ci_conn_id = ci_conn_id
        self.resource_group = resource_group
        self.name = self._check_name(name)
        self.image = image
        self.region = region
        self.registry_conn_id = registry_conn_id
        self.environment_variables = environment_variables or DEFAULT_ENVIRONMENT_VARIABLES
        self.secured_variables = secured_variables or DEFAULT_SECURED_VARIABLES
        self.volumes = volumes or DEFAULT_VOLUMES
        self.memory_in_gb = memory_in_gb or DEFAULT_MEMORY_IN_GB
        self.cpu = cpu or DEFAULT_CPU
        self.gpu = gpu
        self.command = command
        self.remove_on_error = remove_on_error
        self.fail_if_exists = fail_if_exists
        self._ci_hook: Any = None
        self.tags = tags
        self.os_type = os_type
        if self.os_type not in ['Linux', 'Windows']:
            raise AirflowException(
                "Invalid value for the os_type argument. "
                "Please set 'Linux' or 'Windows' as the os_type. "
                f"Found `{self.os_type}`."
            )
        self.restart_policy = restart_policy
        if self.restart_policy not in ['Always', 'OnFailure', 'Never']:
            raise AirflowException(
                "Invalid value for the restart_policy argument. "
                "Please set one of 'Always', 'OnFailure','Never' as the restart_policy. "
                f"Found `{self.restart_policy}`"
            )
        self.ip_address = ip_address
        self.ports = ports

    def execute(self, context: dict) -> int:
        # Check name again in case it was templated.
        self._check_name(self.name)

        self._ci_hook = AzureContainerInstanceHook(self.ci_conn_id)

        if self.fail_if_exists:
            self.log.info("Testing if container group already exists")
            if self._ci_hook.exists(self.resource_group, self.name):
                raise AirflowException("Container group exists")

        if self.registry_conn_id:
            registry_hook = AzureContainerRegistryHook(self.registry_conn_id)
            image_registry_credentials: Optional[list] = [
                registry_hook.connection,
            ]
        else:
            image_registry_credentials = None

        environment_variables = []
        for key, value in self.environment_variables.items():
            if key in self.secured_variables:
                e = EnvironmentVariable(name=key, secure_value=value)
            else:
                e = EnvironmentVariable(name=key, value=value)
            environment_variables.append(e)

        volumes: List[Union[Volume, Volume]] = []
        volume_mounts: List[Union[VolumeMount, VolumeMount]] = []
        for conn_id, account_name, share_name, mount_path, read_only in self.volumes:
            hook = AzureContainerVolumeHook(conn_id)

            mount_name = "mount-%d" % len(volumes)
            volumes.append(hook.get_file_volume(mount_name, share_name, account_name, read_only))
            volume_mounts.append(VolumeMount(name=mount_name, mount_path=mount_path, read_only=read_only))

        exit_code = 1
        try:
            self.log.info("Starting container group with %.1f cpu %.1f mem", self.cpu, self.memory_in_gb)
            if self.gpu:
                self.log.info("GPU count: %.1f, GPU SKU: %s", self.gpu.count, self.gpu.sku)

            resources = ResourceRequirements(
                requests=ResourceRequests(memory_in_gb=self.memory_in_gb, cpu=self.cpu, gpu=self.gpu)
            )

            if self.ip_address and not self.ports:
                self.ports = [ContainerPort(port=80)]
                self.log.info("Default port set. Container will listen on port 80")

            container = Container(
                name=self.name,
                image=self.image,
                resources=resources,
                command=self.command,
                environment_variables=environment_variables,
                volume_mounts=volume_mounts,
                ports=self.ports,
            )

            container_group = ContainerGroup(
                location=self.region,
                containers=[
                    container,
                ],
                image_registry_credentials=image_registry_credentials,
                volumes=volumes,
                restart_policy=self.restart_policy,
                os_type=self.os_type,
                tags=self.tags,
                ip_address=self.ip_address,
            )

            self._ci_hook.create_or_update(self.resource_group, self.name, container_group)

            self.log.info("Container group started %s/%s", self.resource_group, self.name)

            exit_code = self._monitor_logging(self.resource_group, self.name)

            self.log.info("Container had exit code: %s", exit_code)
            if exit_code != 0:
                raise AirflowException(f"Container had a non-zero exit code, {exit_code}")
            return exit_code

        except CloudError:
            self.log.exception("Could not start container group")
            raise AirflowException("Could not start container group")

        finally:
            if exit_code == 0 or self.remove_on_error:
                self.on_kill()

    def on_kill(self) -> None:
        if self.remove_on_error:
            self.log.info("Deleting container group")
            try:
                self._ci_hook.delete(self.resource_group, self.name)
            except Exception:  # pylint: disable=broad-except
                self.log.exception("Could not delete container group")

    def _monitor_logging(self, resource_group: str, name: str) -> int:
        last_state = None
        last_message_logged = None
        last_line_logged = None

        # pylint: disable=too-many-nested-blocks
        while True:
            try:
                cg_state = self._ci_hook.get_state(resource_group, name)
                instance_view = cg_state.containers[0].instance_view

                # If there is no instance view, we show the provisioning state
                if instance_view is not None:
                    c_state = instance_view.current_state
                    state, exit_code, detail_status = (
                        c_state.state,
                        c_state.exit_code,
                        c_state.detail_status,
                    )
                else:
                    state = cg_state.provisioning_state
                    exit_code = 0
                    detail_status = "Provisioning"

                if instance_view is not None and instance_view.events is not None:
                    messages = [event.message for event in instance_view.events]
                    last_message_logged = self._log_last(messages, last_message_logged)

                if state != last_state:
                    self.log.info("Container group state changed to %s", state)
                    last_state = state

                if state in ["Running", "Terminated", "Succeeded"]:
                    try:
                        logs = self._ci_hook.get_logs(resource_group, name)
                        last_line_logged = self._log_last(logs, last_line_logged)
                    except CloudError:
                        self.log.exception(
                            "Exception while getting logs from container instance, retrying..."
                        )

                if state in ["Terminated"]:
                    self.log.info("Container exited with detail_status %s", detail_status)
                    return exit_code

                if state == "Failed":
                    self.log.error("Azure provision failure")
                    return 1

            except AirflowTaskTimeout:
                raise
            except CloudError as err:
                if 'ResourceNotFound' in str(err):
                    self.log.warning(
                        "ResourceNotFound, container is probably removed "
                        "by another process "
                        "(make sure that the name is unique)."
                    )
                    return 1
                else:
                    self.log.exception("Exception while getting container groups")
            except Exception:  # pylint: disable=broad-except
                self.log.exception("Exception while getting container groups")

            sleep(1)

    def _log_last(self, logs: Optional[list], last_line_logged: Any) -> Optional[Any]:
        if logs:
            # determine the last line which was logged before
            last_line_index = 0
            for i in range(len(logs) - 1, -1, -1):
                if logs[i] == last_line_logged:
                    # this line is the same, hence print from i+1
                    last_line_index = i + 1
                    break

            # log all new ones
            for line in logs[last_line_index:]:
                self.log.info(line.rstrip())

            return logs[-1]
        return None

    @staticmethod
    def _check_name(name: str) -> str:
        if '{{' in name:
            # Let macros pass as they cannot be checked at construction time
            return name
        regex_check = re.match("[a-z0-9]([-a-z0-9]*[a-z0-9])?", name)
        if regex_check is None or regex_check.group() != name:
            raise AirflowException('ACI name must match regex [a-z0-9]([-a-z0-9]*[a-z0-9])? (like "my-name")')
        if len(name) > 63:
            raise AirflowException('ACI name cannot be longer than 63 characters')
        return name
