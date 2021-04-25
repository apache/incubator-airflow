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
import inspect
from functools import wraps
from typing import Any, Callable, Optional

from azure.core.polling import LROPoller
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    CreateRunResponse,
    Dataset,
    DatasetResource,
    Factory,
    LinkedService,
    LinkedServiceResource,
    PipelineResource,
    PipelineRun,
    Trigger,
    TriggerResource,
)

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


def provide_targeted_factory(func: Callable) -> Callable:
    """
    Provide the targeted factory to the decorated function in case it isn't specified.

    If ``resource_group_name`` or ``factory_name`` is not provided it defaults to the value specified in
    the connection extras.
    """
    signature = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        bound_args = signature.bind(*args, **kwargs)

        def bind_argument(arg, default_key):
            if arg not in bound_args.arguments:
                self = args[0]
                conn = self.get_connection(self.conn_id)
                default_value = conn.extra_dejson.get(default_key)

                if not default_value:
                    raise AirflowException("Could not determine the targeted data factory.")

                bound_args.arguments[arg] = conn.extra_dejson[default_key]

        bind_argument("resource_group_name", "resourceGroup")
        bind_argument("factory_name", "factory")

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


class AzureDataFactoryHook(BaseHook):  # pylint: disable=too-many-public-methods
    """
    A hook to interact with Azure Data Factory.

    :param conn_id: The Azure Data Factory connection id.
    """

    conn_type: str = 'azure_data_factory'
    conn_name_attr: str = 'azure_data_factory_conn_id'
    default_conn_name: str = 'azure_data_factory_default'
    hook_name: str = 'Azure Data Factory'

    def __init__(self, conn_id: Optional[str] = default_conn_name):
        self._conn: DataFactoryManagementClient = None
        self.conn_id = conn_id
        super().__init__()

    def get_conn(self) -> DataFactoryManagementClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)

        self._conn = DataFactoryManagementClient(
            credential=ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=conn.extra_dejson.get("tenantId")
            ),
            subscription_id=conn.extra_dejson.get("subscriptionId"),
        )

        return self._conn

    @provide_targeted_factory
    def get_factory(
        self, resource_group_name: Optional[str] = None, factory_name: Optional[str] = None, **config: Any
    ) -> Factory:
        """
        Get the factory.

        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The factory.
        """
        return self.get_conn().factories.get(resource_group_name, factory_name, **config)

    def _factory_exists(self, resource_group_name, factory_name) -> bool:
        """Return whether or not the factory already exists."""
        factories = {
            factory.name for factory in self.get_conn().factories.list_by_resource_group(resource_group_name)
        }

        return factory_name in factories

    @provide_targeted_factory
    def update_factory(
        self,
        factory: Factory,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> Factory:
        """
        Update the factory.

        :param factory: The factory resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the factory does not exist.
        :return: The factory.
        """
        if not self._factory_exists(resource_group_name, factory_name):
            raise AirflowException(f"Factory {factory!r} does not exist.")

        return self.get_conn().factories.create_or_update(
            resource_group_name, factory_name, factory, **config
        )

    @provide_targeted_factory
    def create_factory(
        self,
        factory: Factory,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> Factory:
        """
        Create the factory.

        :param factory: The factory resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the factory already exists.
        :return: The factory.
        """
        if self._factory_exists(resource_group_name, factory_name):
            raise AirflowException(f"Factory {factory!r} already exists.")

        return self.get_conn().factories.create_or_update(
            resource_group_name, factory_name, factory, **config
        )

    @provide_targeted_factory
    def delete_factory(
        self, resource_group_name: Optional[str] = None, factory_name: Optional[str] = None, **config: Any
    ) -> None:
        """
        Delete the factory.

        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().factories.delete(resource_group_name, factory_name, **config)

    @provide_targeted_factory
    def get_linked_service(
        self,
        linked_service_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LinkedServiceResource:
        """
        Get the linked service.

        :param linked_service_name: The linked service name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The linked service.
        """
        return self.get_conn().linked_services.get(
            resource_group_name, factory_name, linked_service_name, **config
        )

    def _linked_service_exists(self, resource_group_name, factory_name, linked_service_name) -> bool:
        """Return whether or not the linked service already exists."""
        linked_services = {
            linked_service.name
            for linked_service in self.get_conn().linked_services.list_by_factory(
                resource_group_name, factory_name
            )
        }

        return linked_service_name in linked_services

    @provide_targeted_factory
    def update_linked_service(
        self,
        linked_service_name: str,
        linked_service: LinkedService,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LinkedServiceResource:
        """
        Update the linked service.

        :param linked_service_name: The linked service name.
        :param linked_service: The linked service resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the linked service does not exist.
        :return: The linked service.
        """
        if not self._linked_service_exists(resource_group_name, factory_name, linked_service_name):
            raise AirflowException(f"Linked service {linked_service_name!r} does not exist.")

        return self.get_conn().linked_services.create_or_update(
            resource_group_name, factory_name, linked_service_name, linked_service, **config
        )

    @provide_targeted_factory
    def create_linked_service(
        self,
        linked_service_name: str,
        linked_service: LinkedService,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LinkedServiceResource:
        """
        Create the linked service.

        :param linked_service_name: The linked service name.
        :param linked_service: The linked service resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the linked service already exists.
        :return: The linked service.
        """
        if self._linked_service_exists(resource_group_name, factory_name, linked_service_name):
            raise AirflowException(f"Linked service {linked_service_name!r} already exists.")

        return self.get_conn().linked_services.create_or_update(
            resource_group_name, factory_name, linked_service_name, linked_service, **config
        )

    @provide_targeted_factory
    def delete_linked_service(
        self,
        linked_service_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the linked service.

        :param linked_service_name: The linked service name.
        :param resource_group_name: The linked service name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().linked_services.delete(
            resource_group_name, factory_name, linked_service_name, **config
        )

    @provide_targeted_factory
    def get_dataset(
        self,
        dataset_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> DatasetResource:
        """
        Get the dataset.

        :param dataset_name: The dataset name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The dataset.
        """
        return self.get_conn().datasets.get(resource_group_name, factory_name, dataset_name, **config)

    def _dataset_exists(self, resource_group_name, factory_name, dataset_name) -> bool:
        """Return whether or not the dataset already exists."""
        datasets = {
            dataset.name
            for dataset in self.get_conn().datasets.list_by_factory(resource_group_name, factory_name)
        }

        return dataset_name in datasets

    @provide_targeted_factory
    def update_dataset(
        self,
        dataset_name: str,
        dataset: Dataset,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> DatasetResource:
        """
        Update the dataset.

        :param dataset_name: The dataset name.
        :param dataset: The dataset resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the dataset does not exist.
        :return: The dataset.
        """
        if not self._dataset_exists(resource_group_name, factory_name, dataset_name):
            raise AirflowException(f"Dataset {dataset_name!r} does not exist.")

        return self.get_conn().datasets.create_or_update(
            resource_group_name, factory_name, dataset_name, dataset, **config
        )

    @provide_targeted_factory
    def create_dataset(
        self,
        dataset_name: str,
        dataset: Dataset,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> DatasetResource:
        """
        Create the dataset.

        :param dataset_name: The dataset name.
        :param dataset: The dataset resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the dataset already exists.
        :return: The dataset.
        """
        if self._dataset_exists(resource_group_name, factory_name, dataset_name):
            raise AirflowException(f"Dataset {dataset_name!r} already exists.")

        return self.get_conn().datasets.create_or_update(
            resource_group_name, factory_name, dataset_name, dataset, **config
        )

    @provide_targeted_factory
    def delete_dataset(
        self,
        dataset_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the dataset.

        :param dataset_name: The dataset name.
        :param resource_group_name: The dataset name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().datasets.delete(resource_group_name, factory_name, dataset_name, **config)

    @provide_targeted_factory
    def get_pipeline(
        self,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineResource:
        """
        Get the pipeline.

        :param pipeline_name: The pipeline name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline.
        """
        return self.get_conn().pipelines.get(resource_group_name, factory_name, pipeline_name, **config)

    def _pipeline_exists(self, resource_group_name, factory_name, pipeline_name) -> bool:
        """Return whether or not the pipeline already exists."""
        pipelines = {
            pipeline.name
            for pipeline in self.get_conn().pipelines.list_by_factory(resource_group_name, factory_name)
        }

        return pipeline_name in pipelines

    @provide_targeted_factory
    def update_pipeline(
        self,
        pipeline_name: str,
        pipeline: PipelineResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineResource:
        """
        Update the pipeline.

        :param pipeline_name: The pipeline name.
        :param pipeline: The pipeline resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the pipeline does not exist.
        :return: The pipeline.
        """
        if not self._pipeline_exists(resource_group_name, factory_name, pipeline_name):
            raise AirflowException(f"Pipeline {pipeline_name!r} does not exist.")

        return self.get_conn().pipelines.create_or_update(
            resource_group_name, factory_name, pipeline_name, pipeline, **config
        )

    @provide_targeted_factory
    def create_pipeline(
        self,
        pipeline_name: str,
        pipeline: PipelineResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineResource:
        """
        Create the pipeline.

        :param pipeline_name: The pipeline name.
        :param pipeline: The pipeline resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the pipeline already exists.
        :return: The pipeline.
        """
        if self._pipeline_exists(resource_group_name, factory_name, pipeline_name):
            raise AirflowException(f"Pipeline {pipeline_name!r} already exists.")

        return self.get_conn().pipelines.create_or_update(
            resource_group_name, factory_name, pipeline_name, pipeline, **config
        )

    @provide_targeted_factory
    def delete_pipeline(
        self,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the pipeline.

        :param pipeline_name: The pipeline name.
        :param resource_group_name: The pipeline name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().pipelines.delete(resource_group_name, factory_name, pipeline_name, **config)

    @provide_targeted_factory
    def run_pipeline(
        self,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> CreateRunResponse:
        """
        Run a pipeline.

        :param pipeline_name: The pipeline name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline run.
        """
        return self.get_conn().pipelines.create_run(
            resource_group_name, factory_name, pipeline_name, **config
        )

    @provide_targeted_factory
    def get_pipeline_run(
        self,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineRun:
        """
        Get the pipeline run.

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline run.
        """
        return self.get_conn().pipeline_runs.get(resource_group_name, factory_name, run_id, **config)

    @provide_targeted_factory
    def cancel_pipeline_run(
        self,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Cancel the pipeline run.

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().pipeline_runs.cancel(resource_group_name, factory_name, run_id, **config)

    @provide_targeted_factory
    def get_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> TriggerResource:
        """
        Get the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The trigger.
        """
        return self.get_conn().triggers.get(resource_group_name, factory_name, trigger_name, **config)

    def _trigger_exists(self, resource_group_name, factory_name, trigger_name) -> bool:
        """Return whether or not the trigger already exists."""
        triggers = {
            trigger.name
            for trigger in self.get_conn().triggers.list_by_factory(resource_group_name, factory_name)
        }

        return trigger_name in triggers

    @provide_targeted_factory
    def update_trigger(
        self,
        trigger_name: str,
        trigger: Trigger,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> TriggerResource:
        """
        Update the trigger.

        :param trigger_name: The trigger name.
        :param trigger: The trigger resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the trigger does not exist.
        :return: The trigger.
        """
        if not self._trigger_exists(resource_group_name, factory_name, trigger_name):
            raise AirflowException(f"Trigger {trigger_name!r} does not exist.")

        return self.get_conn().triggers.create_or_update(
            resource_group_name, factory_name, trigger_name, trigger, **config
        )

    @provide_targeted_factory
    def create_trigger(
        self,
        trigger_name: str,
        trigger: Trigger,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> TriggerResource:
        """
        Create the trigger.

        :param trigger_name: The trigger name.
        :param trigger: The trigger resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the trigger already exists.
        :return: The trigger.
        """
        if self._trigger_exists(resource_group_name, factory_name, trigger_name):
            raise AirflowException(f"Trigger {trigger_name!r} already exists.")

        return self.get_conn().triggers.create_or_update(
            resource_group_name, factory_name, trigger_name, trigger, **config
        )

    @provide_targeted_factory
    def delete_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().triggers.delete(resource_group_name, factory_name, trigger_name, **config)

    @provide_targeted_factory
    def start_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LROPoller:
        """
        Start the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: An Azure operation poller.
        """
        return self.get_conn().triggers.begin_start(resource_group_name, factory_name, trigger_name, **config)

    @provide_targeted_factory
    def stop_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LROPoller:
        """
        Stop the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: An Azure operation poller.
        """
        return self.get_conn().triggers.begin_stop(resource_group_name, factory_name, trigger_name, **config)

    @provide_targeted_factory
    def rerun_trigger(
        self,
        trigger_name: str,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Rerun the trigger.

        :param trigger_name: The trigger name.
        :param run_id: The trigger run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        return self.get_conn().trigger_runs.rerun(
            resource_group_name, factory_name, trigger_name, run_id, **config
        )

    @provide_targeted_factory
    def cancel_trigger(
        self,
        trigger_name: str,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Cancel the trigger.

        :param trigger_name: The trigger name.
        :param run_id: The trigger run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().trigger_runs.cancel(resource_group_name, factory_name, trigger_name, run_id, **config)
