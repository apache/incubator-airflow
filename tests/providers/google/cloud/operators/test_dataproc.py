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
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, Mock, call

import pytest
from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.retry import Retry

from airflow import AirflowException
from airflow.models import DAG, DagBag, TaskInstance
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocClusterLink,
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteClusterOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocJobLink,
    DataprocScaleClusterOperator,
    DataprocSubmitHadoopJobOperator,
    DataprocSubmitHiveJobOperator,
    DataprocSubmitJobOperator,
    DataprocSubmitPigJobOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocSubmitSparkJobOperator,
    DataprocSubmitSparkSqlJobOperator,
    DataprocUpdateClusterOperator,
)
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.version import version as airflow_version
from tests.test_utils.db import clear_db_runs, clear_db_xcom

cluster_params = inspect.signature(ClusterGenerator.__init__).parameters

AIRFLOW_VERSION = "v" + airflow_version.replace(".", "-").replace("+", "-")

DATAPROC_PATH = "airflow.providers.google.cloud.operators.dataproc.{}"

TASK_ID = "task-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

CLUSTER_NAME = "cluster_name"
CONFIG = {
    "gce_cluster_config": {
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/project_id/zones/zone",
        "metadata": {"metadata": "data"},
        "network_uri": "network_uri",
        "subnetwork_uri": "subnetwork_uri",
        "internal_ip_only": True,
        "tags": ["tags"],
        "service_account": "service_account",
        "service_account_scopes": ["service_account_scopes"],
    },
    "master_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/master_machine_type",
        "disk_config": {"boot_disk_type": "master_disk_type", "boot_disk_size_gb": 128},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/custom_image",
    },
    "secondary_worker_config": {
        "num_instances": 4,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "is_preemptible": True,
    },
    "software_config": {"properties": {"properties": "data"}, "optional_components": ["optional_components"]},
    "lifecycle_config": {
        "idle_delete_ttl": {'seconds': 60},
        "auto_delete_time": "2019-09-12T00:00:00.000000Z",
    },
    "encryption_config": {"gce_pd_kms_key_name": "customer_managed_key"},
    "autoscaling_config": {"policy_uri": "autoscaling_policy"},
    "config_bucket": "storage_bucket",
    "initialization_actions": [
        {"executable_file": "init_actions_uris", "execution_timeout": {'seconds': 600}}
    ],
}

CONFIG_WITH_CUSTOM_IMAGE_FAMILY = {
    "gce_cluster_config": {
        "zone_uri": "https://www.googleapis.com/compute/v1/projects/project_id/zones/zone",
        "metadata": {"metadata": "data"},
        "network_uri": "network_uri",
        "subnetwork_uri": "subnetwork_uri",
        "internal_ip_only": True,
        "tags": ["tags"],
        "service_account": "service_account",
        "service_account_scopes": ["service_account_scopes"],
    },
    "master_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/master_machine_type",
        "disk_config": {"boot_disk_type": "master_disk_type", "boot_disk_size_gb": 128},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/family/custom_image_family",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "image_uri": "https://www.googleapis.com/compute/beta/projects/"
        "custom_image_project_id/global/images/family/custom_image_family",
    },
    "secondary_worker_config": {
        "num_instances": 4,
        "machine_type_uri": "projects/project_id/zones/zone/machineTypes/worker_machine_type",
        "disk_config": {"boot_disk_type": "worker_disk_type", "boot_disk_size_gb": 256},
        "is_preemptible": True,
    },
    "software_config": {"properties": {"properties": "data"}, "optional_components": ["optional_components"]},
    "lifecycle_config": {
        "idle_delete_ttl": {'seconds': 60},
        "auto_delete_time": "2019-09-12T00:00:00.000000Z",
    },
    "encryption_config": {"gce_pd_kms_key_name": "customer_managed_key"},
    "autoscaling_config": {"policy_uri": "autoscaling_policy"},
    "config_bucket": "storage_bucket",
    "initialization_actions": [
        {"executable_file": "init_actions_uris", "execution_timeout": {'seconds': 600}}
    ],
}

LABELS = {"labels": "data", "airflow-version": AIRFLOW_VERSION}

LABELS.update({'airflow-version': 'v' + airflow_version.replace('.', '-').replace('+', '-')})

CLUSTER = {"project_id": "project_id", "cluster_name": CLUSTER_NAME, "config": CONFIG, "labels": LABELS}

UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}

TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]
REQUEST_ID = "request_id_uuid"

WORKFLOW_NAME = "airflow-dataproc-test"
WORKFLOW_TEMPLATE = {
    "id": WORKFLOW_NAME,
    "placement": {
        "managed_cluster": {
            "cluster_name": CLUSTER_NAME,
            "config": CLUSTER,
        }
    },
    "jobs": [{"step_id": "pig_job_1", "pig_job": {}}],
}
TEST_DAG_ID = 'test-dataproc-operators'
DEFAULT_DATE = datetime(2020, 1, 1)
TEST_JOB_ID = "test-job"

DATAPROC_JOB_LINK_EXPECTED = (
    f"https://console.cloud.google.com/dataproc/jobs/{TEST_JOB_ID}?"
    f"region={GCP_LOCATION}&project={GCP_PROJECT}"
)
DATAPROC_CLUSTER_LINK_EXPECTED = (
    f"https://console.cloud.google.com/dataproc/clusters/{CLUSTER_NAME}/monitoring?"
    f"region={GCP_LOCATION}&project={GCP_PROJECT}"
)
DATAPROC_JOB_CONF_EXPECTED = {
    "job_id": TEST_JOB_ID,
    "region": GCP_LOCATION,
    "project_id": GCP_PROJECT,
}
DATAPROC_CLUSTER_CONF_EXPECTED = {
    "cluster_name": CLUSTER_NAME,
    "region": GCP_LOCATION,
    "project_id": GCP_PROJECT,
}


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class DataprocTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder="/dev/null", include_examples=False)
        cls.dag = DAG(TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE})

    def setUp(self):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.extra_links_manager_mock = Mock()
        self.extra_links_manager_mock.attach_mock(self.mock_ti, 'ti')

    def tearDown(self):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.extra_links_manager_mock = Mock()
        self.extra_links_manager_mock.attach_mock(self.mock_ti, 'ti')

    @classmethod
    def tearDownClass(cls):
        clear_db_runs()
        clear_db_xcom()


class DataprocJobTestBase(DataprocTestBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.extra_links_expected_calls = [
            call.ti.xcom_push(execution_date=None, key='job_conf', value=DATAPROC_JOB_CONF_EXPECTED),
            call.hook().wait_for_job(job_id=TEST_JOB_ID, location=GCP_LOCATION, project_id=GCP_PROJECT),
        ]


class DataprocClusterTestBase(DataprocTestBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.extra_links_expected_calls_base = [
            call.ti.xcom_push(execution_date=None, key='cluster_conf', value=DATAPROC_CLUSTER_CONF_EXPECTED)
        ]


class TestsClusterGenerator(unittest.TestCase):
    def test_image_version(self):
        with pytest.raises(ValueError) as ctx:
            ClusterGenerator(
                custom_image="custom_image",
                image_version="image_version",
                project_id=GCP_PROJECT,
                cluster_name=CLUSTER_NAME,
            )
            assert "custom_image and image_version" in str(ctx.value)

    def test_custom_image_family_error_with_image_version(self):
        with pytest.raises(ValueError) as ctx:
            ClusterGenerator(
                image_version="image_version",
                custom_image_family="custom_image_family",
                project_id=GCP_PROJECT,
                cluster_name=CLUSTER_NAME,
            )
            assert "image_version and custom_image_family" in str(ctx.value)

    def test_custom_image_family_error_with_custom_image(self):
        with pytest.raises(ValueError) as ctx:
            ClusterGenerator(
                custom_image="custom_image",
                custom_image_family="custom_image_family",
                project_id=GCP_PROJECT,
                cluster_name=CLUSTER_NAME,
            )
            assert "custom_image and custom_image_family" in str(ctx.value)

    def test_nodes_number(self):
        with pytest.raises(AssertionError) as ctx:
            ClusterGenerator(
                num_workers=0, num_preemptible_workers=0, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
            )
            assert "num_workers == 0 means single" in str(ctx.value)

    def test_build(self):
        generator = ClusterGenerator(
            project_id="project_id",
            num_workers=2,
            zone="zone",
            network_uri="network_uri",
            subnetwork_uri="subnetwork_uri",
            internal_ip_only=True,
            tags=["tags"],
            storage_bucket="storage_bucket",
            init_actions_uris=["init_actions_uris"],
            init_action_timeout="10m",
            metadata={"metadata": "data"},
            custom_image="custom_image",
            custom_image_project_id="custom_image_project_id",
            autoscaling_policy="autoscaling_policy",
            properties={"properties": "data"},
            optional_components=["optional_components"],
            num_masters=2,
            master_machine_type="master_machine_type",
            master_disk_type="master_disk_type",
            master_disk_size=128,
            worker_machine_type="worker_machine_type",
            worker_disk_type="worker_disk_type",
            worker_disk_size=256,
            num_preemptible_workers=4,
            region="region",
            service_account="service_account",
            service_account_scopes=["service_account_scopes"],
            idle_delete_ttl=60,
            auto_delete_time=datetime(2019, 9, 12),
            auto_delete_ttl=250,
            customer_managed_key="customer_managed_key",
        )
        cluster = generator.make()
        assert CONFIG == cluster

    def test_build_with_custom_image_family(self):
        generator = ClusterGenerator(
            project_id="project_id",
            num_workers=2,
            zone="zone",
            network_uri="network_uri",
            subnetwork_uri="subnetwork_uri",
            internal_ip_only=True,
            tags=["tags"],
            storage_bucket="storage_bucket",
            init_actions_uris=["init_actions_uris"],
            init_action_timeout="10m",
            metadata={"metadata": "data"},
            custom_image_family="custom_image_family",
            custom_image_project_id="custom_image_project_id",
            autoscaling_policy="autoscaling_policy",
            properties={"properties": "data"},
            optional_components=["optional_components"],
            num_masters=2,
            master_machine_type="master_machine_type",
            master_disk_type="master_disk_type",
            master_disk_size=128,
            worker_machine_type="worker_machine_type",
            worker_disk_type="worker_disk_type",
            worker_disk_size=256,
            num_preemptible_workers=4,
            region="region",
            service_account="service_account",
            service_account_scopes=["service_account_scopes"],
            idle_delete_ttl=60,
            auto_delete_time=datetime(2019, 9, 12),
            auto_delete_ttl=250,
            customer_managed_key="customer_managed_key",
        )
        cluster = generator.make()
        assert CONFIG_WITH_CUSTOM_IMAGE_FAMILY == cluster


class TestDataprocClusterCreateOperator(DataprocClusterTestBase):
    def test_deprecation_warning(self):
        with pytest.warns(DeprecationWarning) as warnings:
            op = DataprocCreateClusterOperator(
                task_id=TASK_ID,
                region=GCP_LOCATION,
                project_id=GCP_PROJECT,
                cluster_name="cluster_name",
                num_workers=2,
                zone="zone",
            )
        assert_warning("Passing cluster parameters by keywords", warnings)

        assert op.project_id == GCP_PROJECT
        assert op.cluster_name == "cluster_name"
        assert op.cluster_config['worker_config']['num_instances'] == 2
        assert "zones/zone" in op.cluster_config['master_config']["machine_type_uri"]

        with pytest.warns(DeprecationWarning) as warnings:
            op_default_region = DataprocCreateClusterOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                cluster_name="cluster_name",
                cluster_config=op.cluster_config,
            )
        assert_warning("Default region value", warnings)
        assert op_default_region.region == 'global'

    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        self.extra_links_manager_mock.attach_mock(mock_hook, 'hook')
        mock_hook.return_value.create_cluster.result.return_value = None
        create_cluster_args = {
            'region': GCP_LOCATION,
            'project_id': GCP_PROJECT,
            'cluster_name': CLUSTER_NAME,
            'request_id': REQUEST_ID,
            'retry': RETRY,
            'timeout': TIMEOUT,
            'metadata': METADATA,
            'cluster_config': CONFIG,
            'labels': LABELS,
        }
        expected_calls = self.extra_links_expected_calls_base + [
            call.hook().create_cluster(**create_cluster_args),
        ]

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_cluster.assert_called_once_with(**create_cluster_args)

        # Test whether xcom push occurs before create cluster is called
        self.extra_links_manager_mock.assert_has_calls(expected_calls, any_order=False)

        to_dict_mock.assert_called_once_with(mock_hook().create_cluster().result())
        self.mock_ti.xcom_push.assert_called_once_with(
            key="cluster_conf",
            value=DATAPROC_CLUSTER_CONF_EXPECTED,
            execution_date=None,
        )

    @mock.patch(DATAPROC_PATH.format("Cluster.to_dict"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists(self, mock_hook, to_dict_mock):
        mock_hook.return_value.create_cluster.side_effect = [AlreadyExists("test")]
        mock_hook.return_value.get_cluster.return_value.status.state = 0
        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_cluster.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.get_cluster.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        to_dict_mock.assert_called_once_with(mock_hook.return_value.get_cluster.return_value)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists_do_not_use(self, mock_hook):
        mock_hook.return_value.create_cluster.side_effect = [AlreadyExists("test")]
        mock_hook.return_value.get_cluster.return_value.status.state = 0
        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            use_if_exists=False,
        )
        with pytest.raises(AlreadyExists):
            op.execute(context=self.mock_context)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists_in_error_state(self, mock_hook):
        mock_hook.return_value.create_cluster.side_effect = [AlreadyExists("test")]
        cluster_status = mock_hook.return_value.get_cluster.return_value.status
        cluster_status.state = 0
        cluster_status.State.ERROR = 0

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
        )
        with pytest.raises(AirflowException):
            op.execute(context=self.mock_context)

        mock_hook.return_value.diagnose_cluster.assert_called_once_with(
            region=GCP_LOCATION, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
        )
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            region=GCP_LOCATION, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
        )

    @mock.patch(DATAPROC_PATH.format("exponential_sleep_generator"))
    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator._create_cluster"))
    @mock.patch(DATAPROC_PATH.format("DataprocCreateClusterOperator._get_cluster"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_if_cluster_exists_in_deleting_state(
        self, mock_hook, mock_get_cluster, mock_create_cluster, mock_generator
    ):
        cluster = mock.MagicMock()
        cluster.status.state = 0
        cluster.status.State.DELETING = 0  # pylint: disable=no-member

        cluster2 = mock.MagicMock()
        cluster2.status.state = 0
        cluster2.status.State.ERROR = 0  # pylint: disable=no-member

        mock_create_cluster.side_effect = [AlreadyExists("test"), cluster2]
        mock_generator.return_value = [0]
        mock_get_cluster.side_effect = [cluster, NotFound("test")]

        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_config=CONFIG,
            labels=LABELS,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            gcp_conn_id=GCP_CONN_ID,
        )
        with pytest.raises(AirflowException):
            op.execute(context=self.mock_context)

        calls = [mock.call(mock_hook.return_value), mock.call(mock_hook.return_value)]
        mock_get_cluster.assert_has_calls(calls)
        mock_create_cluster.assert_has_calls(calls)
        mock_hook.return_value.diagnose_cluster.assert_called_once_with(
            region=GCP_LOCATION, project_id=GCP_PROJECT, cluster_name=CLUSTER_NAME
        )

    def test_operator_extra_links(self):
        op = DataprocCreateClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            delete_on_error=True,
            gcp_conn_id=GCP_CONN_ID,
            dag=self.dag,
        )

        serialized_dag = SerializedDAG.to_dict(self.dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        # Assert operator links for serialized DAG
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{"airflow.providers.google.cloud.operators.dataproc.DataprocClusterLink": {}}],
        )

        # Assert operator link types are preserved during deserialization
        self.assertIsInstance(deserialized_task.operator_extra_links[0], DataprocClusterLink)

        ti = TaskInstance(task=op, execution_date=DEFAULT_DATE)

        # Assert operator link is empty when no XCom push occurred
        self.assertEqual(op.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name), "")

        # Assert operator link is empty for deserialized task when no XCom push occurred
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            "",
        )

        ti.xcom_push(key="cluster_conf", value=DATAPROC_CLUSTER_CONF_EXPECTED)

        # Assert operator links are preserved in deserialized tasks after execution
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            DATAPROC_CLUSTER_LINK_EXPECTED,
        )

        # Assert operator links after execution
        self.assertEqual(
            op.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            DATAPROC_CLUSTER_LINK_EXPECTED,
        )

        # Check negative case
        self.assertEqual(op.get_extra_links(datetime(2020, 7, 20), DataprocClusterLink.name), "")


class TestDataprocClusterScaleOperator(DataprocClusterTestBase):
    def test_deprecation_warning(self):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocScaleClusterOperator(task_id=TASK_ID, cluster_name=CLUSTER_NAME, project_id=GCP_PROJECT)
        assert_warning("DataprocUpdateClusterOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        self.extra_links_manager_mock.attach_mock(mock_hook, 'hook')
        mock_hook.return_value.update_cluster.result.return_value = None
        cluster_update = {
            "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 4}}
        }
        update_cluster_args = {
            'project_id': GCP_PROJECT,
            'location': GCP_LOCATION,
            'cluster_name': CLUSTER_NAME,
            'cluster': cluster_update,
            'graceful_decommission_timeout': {"seconds": 600},
            'update_mask': UPDATE_MASK,
        }
        expected_calls = self.extra_links_expected_calls_base + [
            call.hook().update_cluster(**update_cluster_args)
        ]

        op = DataprocScaleClusterOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            num_workers=3,
            num_preemptible_workers=4,
            graceful_decommission_timeout="10m",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_cluster.assert_called_once_with(**update_cluster_args)

        # Test whether xcom push occurs before cluster is updated
        self.extra_links_manager_mock.assert_has_calls(expected_calls, any_order=False)

        self.mock_ti.xcom_push.assert_called_once_with(
            key="cluster_conf",
            value=DATAPROC_CLUSTER_CONF_EXPECTED,
            execution_date=None,
        )

    def test_operator_extra_links(self):
        op = DataprocScaleClusterOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            num_workers=3,
            num_preemptible_workers=2,
            graceful_decommission_timeout="2m",
            gcp_conn_id=GCP_CONN_ID,
            dag=self.dag,
        )

        serialized_dag = SerializedDAG.to_dict(self.dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        # Assert operator links for serialized DAG
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{"airflow.providers.google.cloud.operators.dataproc.DataprocClusterLink": {}}],
        )

        # Assert operator link types are preserved during deserialization
        self.assertIsInstance(deserialized_task.operator_extra_links[0], DataprocClusterLink)

        ti = TaskInstance(task=op, execution_date=DEFAULT_DATE)

        # Assert operator link is empty when no XCom push occurred
        self.assertEqual(op.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name), "")

        # Assert operator link is empty for deserialized task when no XCom push occurred
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            "",
        )

        ti.xcom_push(key="cluster_conf", value=DATAPROC_CLUSTER_CONF_EXPECTED)

        # Assert operator links are preserved in deserialized tasks after execution
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            DATAPROC_CLUSTER_LINK_EXPECTED,
        )

        # Assert operator links after execution
        self.assertEqual(
            op.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            DATAPROC_CLUSTER_LINK_EXPECTED,
        )

        # Check negative case
        self.assertEqual(op.get_extra_links(datetime(2020, 7, 20), DataprocClusterLink.name), "")


class TestDataprocClusterDeleteOperator(unittest.TestCase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        op = DataprocDeleteClusterOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            request_id=REQUEST_ID,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_name=CLUSTER_NAME,
            cluster_uuid=None,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataprocSubmitJobOperator(DataprocJobTestBase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        xcom_push_call = call.ti.xcom_push(
            execution_date=None, key='job_conf', value=DATAPROC_JOB_CONF_EXPECTED
        )
        wait_for_job_call = call.hook().wait_for_job(
            job_id=TEST_JOB_ID, location=GCP_LOCATION, project_id=GCP_PROJECT, timeout=None
        )

        job = {}
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID
        self.extra_links_manager_mock.attach_mock(mock_hook, 'hook')

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)

        # Test whether xcom push occurs before polling for job
        self.assertLess(
            self.extra_links_manager_mock.mock_calls.index(xcom_push_call),
            self.extra_links_manager_mock.mock_calls.index(wait_for_job_call),
            msg='Xcom push for Job Link has to be done before polling for job status',
        )

        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            job=job,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=GCP_PROJECT, location=GCP_LOCATION, timeout=None
        )

        self.mock_ti.xcom_push.assert_called_once_with(
            key="job_conf", value=DATAPROC_JOB_CONF_EXPECTED, execution_date=None
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_async(self, mock_hook):
        job = {}
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            asynchronous=True,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            job=job,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.wait_for_job.assert_not_called()

        self.mock_ti.xcom_push.assert_called_once_with(
            key="job_conf", value=DATAPROC_JOB_CONF_EXPECTED, execution_date=None
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_on_kill(self, mock_hook):
        job = {}
        job_id = "job_id"
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = job_id

        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job=job,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            request_id=REQUEST_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cancel_on_kill=False,
        )
        op.execute(context=self.mock_context)

        op.on_kill()
        mock_hook.return_value.cancel_job.assert_not_called()

        op.cancel_on_kill = True
        op.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(
            project_id=GCP_PROJECT, location=GCP_LOCATION, job_id=job_id
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_operator_extra_links(self, mock_hook):
        mock_hook.return_value.project_id = GCP_PROJECT
        op = DataprocSubmitJobOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job={},
            gcp_conn_id=GCP_CONN_ID,
            dag=self.dag,
        )

        serialized_dag = SerializedDAG.to_dict(self.dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        # Assert operator links for serialized_dag
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{"airflow.providers.google.cloud.operators.dataproc.DataprocJobLink": {}}],
        )

        # Assert operator link types are preserved during deserialization
        self.assertIsInstance(deserialized_task.operator_extra_links[0], DataprocJobLink)

        ti = TaskInstance(task=op, execution_date=DEFAULT_DATE)

        # Assert operator link is empty when no XCom push occurred
        self.assertEqual(op.get_extra_links(DEFAULT_DATE, DataprocJobLink.name), "")

        # Assert operator link is empty for deserialized task when no XCom push occurred
        self.assertEqual(deserialized_task.get_extra_links(DEFAULT_DATE, DataprocJobLink.name), "")

        ti.xcom_push(key="job_conf", value=DATAPROC_JOB_CONF_EXPECTED)

        # Assert operator links are preserved in deserialized tasks
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocJobLink.name),
            DATAPROC_JOB_LINK_EXPECTED,
        )
        # Assert operator links after execution
        self.assertEqual(
            op.get_extra_links(DEFAULT_DATE, DataprocJobLink.name),
            DATAPROC_JOB_LINK_EXPECTED,
        )
        # Check for negative case
        self.assertEqual(op.get_extra_links(datetime(2020, 7, 20), DataprocJobLink.name), "")


class TestDataprocUpdateClusterOperator(DataprocClusterTestBase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        self.extra_links_manager_mock.attach_mock(mock_hook, 'hook')
        mock_hook.return_value.update_cluster.result.return_value = None
        cluster_decommission_timeout = {"graceful_decommission_timeout": "600s"}
        update_cluster_args = {
            'location': GCP_LOCATION,
            'project_id': GCP_PROJECT,
            'cluster_name': CLUSTER_NAME,
            'cluster': CLUSTER,
            'update_mask': UPDATE_MASK,
            'graceful_decommission_timeout': cluster_decommission_timeout,
            'request_id': REQUEST_ID,
            'retry': RETRY,
            'timeout': TIMEOUT,
            'metadata': METADATA,
        }
        expected_calls = self.extra_links_expected_calls_base + [
            call.hook().update_cluster(**update_cluster_args)
        ]

        op = DataprocUpdateClusterOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            request_id=REQUEST_ID,
            graceful_decommission_timeout=cluster_decommission_timeout,
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=self.mock_context)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_cluster.assert_called_once_with(**update_cluster_args)

        # Test whether the xcom push happens before updating the cluster
        self.extra_links_manager_mock.assert_has_calls(expected_calls, any_order=False)

        self.mock_ti.xcom_push.assert_called_once_with(
            key="cluster_conf",
            value=DATAPROC_CLUSTER_CONF_EXPECTED,
            execution_date=None,
        )

    def test_operator_extra_links(self):
        op = DataprocUpdateClusterOperator(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            cluster_name=CLUSTER_NAME,
            cluster=CLUSTER,
            update_mask=UPDATE_MASK,
            graceful_decommission_timeout={"graceful_decommission_timeout": "600s"},
            project_id=GCP_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
            dag=self.dag,
        )

        serialized_dag = SerializedDAG.to_dict(self.dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        # Assert operator links for serialized_dag
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{"airflow.providers.google.cloud.operators.dataproc.DataprocClusterLink": {}}],
        )

        # Assert operator link types are preserved during deserialization
        self.assertIsInstance(deserialized_task.operator_extra_links[0], DataprocClusterLink)

        ti = TaskInstance(task=op, execution_date=DEFAULT_DATE)

        # Assert operator link is empty when no XCom push occurred
        self.assertEqual(op.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name), "")

        # Assert operator link is empty for deserialized task when no XCom push occurred
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            "",
        )

        ti.xcom_push(key="cluster_conf", value=DATAPROC_CLUSTER_CONF_EXPECTED)

        # Assert operator links are preserved in deserialized tasks
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            DATAPROC_CLUSTER_LINK_EXPECTED,
        )
        # Assert operator links after execution
        self.assertEqual(
            op.get_extra_links(DEFAULT_DATE, DataprocClusterLink.name),
            DATAPROC_CLUSTER_LINK_EXPECTED,
        )
        # Check for negative case
        self.assertEqual(op.get_extra_links(datetime(2020, 7, 20), DataprocClusterLink.name), "")


class TestDataprocWorkflowTemplateInstantiateOperator(unittest.TestCase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        template_id = "template_id"
        version = 6
        parameters = {}

        op = DataprocInstantiateWorkflowTemplateOperator(
            task_id=TASK_ID,
            template_id=template_id,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            version=version,
            parameters=parameters,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_workflow_template.assert_called_once_with(
            template_name=template_id,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            version=version,
            parameters=parameters,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataprocWorkflowTemplateInstantiateInlineOperator(unittest.TestCase):
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        template = {}

        op = DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id=TASK_ID,
            template=template,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.instantiate_inline_workflow_template.assert_called_once_with(
            template=template,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestDataProcHiveOperator(unittest.TestCase):
    query = "define sin HiveUDF('sin');"
    variables = {"key": "value"}
    job_id = "uuid_id"
    job = {
        "reference": {"project_id": GCP_PROJECT, "job_id": "{{task.task_id}}_{{ds_nodash}}_" + job_id},
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "hive_job": {"query_list": {"queries": [query]}, "script_variables": variables},
    }

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_deprecation_warning(self, mock_hook):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocSubmitHiveJobOperator(task_id=TASK_ID, region=GCP_LOCATION, query="query")
        assert_warning("DataprocSubmitJobOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_uuid):
        mock_uuid.return_value = self.job_id
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = self.job_id

        op = DataprocSubmitHiveJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT, job=self.job, location=GCP_LOCATION
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_id=self.job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_builder(self, mock_hook, mock_uuid):
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_uuid.return_value = self.job_id

        op = DataprocSubmitHiveJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
        )
        job = op.generate_job()
        assert self.job == job


class TestDataProcPigOperator(unittest.TestCase):
    query = "define sin HiveUDF('sin');"
    variables = {"key": "value"}
    job_id = "uuid_id"
    job = {
        "reference": {"project_id": GCP_PROJECT, "job_id": "{{task.task_id}}_{{ds_nodash}}_" + job_id},
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "pig_job": {"query_list": {"queries": [query]}, "script_variables": variables},
    }

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_deprecation_warning(self, mock_hook):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocSubmitPigJobOperator(task_id=TASK_ID, region=GCP_LOCATION, query="query")
        assert_warning("DataprocSubmitJobOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_uuid):
        mock_uuid.return_value = self.job_id
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = self.job_id

        op = DataprocSubmitPigJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT, job=self.job, location=GCP_LOCATION
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_id=self.job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_builder(self, mock_hook, mock_uuid):
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_uuid.return_value = self.job_id

        op = DataprocSubmitPigJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
        )
        job = op.generate_job()
        assert self.job == job


class TestDataProcSparkSqlOperator(unittest.TestCase):
    query = "SHOW DATABASES;"
    variables = {"key": "value"}
    job_id = "uuid_id"
    job = {
        "reference": {"project_id": GCP_PROJECT, "job_id": "{{task.task_id}}_{{ds_nodash}}_" + job_id},
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "spark_sql_job": {"query_list": {"queries": [query]}, "script_variables": variables},
    }
    other_project_job = {
        "reference": {"project_id": "other-project", "job_id": "{{task.task_id}}_{{ds_nodash}}_" + job_id},
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "spark_sql_job": {"query_list": {"queries": [query]}, "script_variables": variables},
    }

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_deprecation_warning(self, mock_hook):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocSubmitSparkSqlJobOperator(task_id=TASK_ID, region=GCP_LOCATION, query="query")
        assert_warning("DataprocSubmitJobOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_uuid):
        mock_uuid.return_value = self.job_id
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = self.job_id

        op = DataprocSubmitSparkSqlJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id=GCP_PROJECT, job=self.job, location=GCP_LOCATION
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_id=self.job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute_override_project_id(self, mock_hook, mock_uuid):
        mock_uuid.return_value = self.job_id
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_hook.return_value.wait_for_job.return_value = None
        mock_hook.return_value.submit_job.return_value.reference.job_id = self.job_id

        op = DataprocSubmitSparkSqlJobOperator(
            project_id="other-project",
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_job.assert_called_once_with(
            project_id="other-project", job=self.other_project_job, location=GCP_LOCATION
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_id=self.job_id, location=GCP_LOCATION, project_id="other-project"
        )

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_builder(self, mock_hook, mock_uuid):
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_uuid.return_value = self.job_id

        op = DataprocSubmitSparkSqlJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            query=self.query,
            variables=self.variables,
        )
        job = op.generate_job()
        assert self.job == job


class TestDataProcSparkOperator(DataprocJobTestBase):
    main_class = "org.apache.spark.examples.SparkPi"
    jars = ["file:///usr/lib/spark/examples/jars/spark-examples.jar"]
    job = {
        "reference": {
            "project_id": GCP_PROJECT,
            "job_id": "{{task.task_id}}_{{ds_nodash}}_" + TEST_JOB_ID,
        },
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "spark_job": {"jar_file_uris": jars, "main_class": main_class},
    }

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_deprecation_warning(self, mock_hook):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocSubmitSparkJobOperator(
                task_id=TASK_ID, region=GCP_LOCATION, main_class=self.main_class, dataproc_jars=self.jars
            )
        assert_warning("DataprocSubmitJobOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_uuid):
        mock_uuid.return_value = TEST_JOB_ID
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_uuid.return_value = TEST_JOB_ID
        mock_hook.return_value.submit_job.return_value.reference.job_id = TEST_JOB_ID
        self.extra_links_manager_mock.attach_mock(mock_hook, 'hook')

        op = DataprocSubmitSparkJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            main_class=self.main_class,
            dataproc_jars=self.jars,
        )
        job = op.generate_job()
        assert self.job == job

        op.execute(context=self.mock_context)
        self.mock_ti.xcom_push.assert_called_once_with(
            key="job_conf", value=DATAPROC_JOB_CONF_EXPECTED, execution_date=None
        )

        # Test whether xcom push occurs before polling for job
        self.extra_links_manager_mock.assert_has_calls(self.extra_links_expected_calls, any_order=False)

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_operator_extra_links(self, mock_hook):
        mock_hook.return_value.project_id = GCP_PROJECT

        op = DataprocSubmitSparkJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            main_class=self.main_class,
            dataproc_jars=self.jars,
            dag=self.dag,
        )

        serialized_dag = SerializedDAG.to_dict(self.dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_task = deserialized_dag.task_dict[TASK_ID]

        # Assert operator links for serialized DAG
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{"airflow.providers.google.cloud.operators.dataproc.DataprocJobLink": {}}],
        )

        # Assert operator link types are preserved during deserialization
        self.assertIsInstance(deserialized_task.operator_extra_links[0], DataprocJobLink)

        ti = TaskInstance(task=op, execution_date=DEFAULT_DATE)

        # Assert operator link is empty when no XCom push occurred
        self.assertEqual(op.get_extra_links(DEFAULT_DATE, DataprocJobLink.name), "")

        # Assert operator link is empty for deserialized task when no XCom push occurred
        self.assertEqual(deserialized_task.get_extra_links(DEFAULT_DATE, DataprocJobLink.name), "")

        ti.xcom_push(key="job_conf", value=DATAPROC_JOB_CONF_EXPECTED)

        # Assert operator links after task execution
        self.assertEqual(
            op.get_extra_links(DEFAULT_DATE, DataprocJobLink.name),
            DATAPROC_JOB_LINK_EXPECTED,
        )

        # Assert operator links are preserved in deserialized tasks
        self.assertEqual(
            deserialized_task.get_extra_links(DEFAULT_DATE, DataprocJobLink.name),
            DATAPROC_JOB_LINK_EXPECTED,
        )

        # Assert for negative case
        self.assertEqual(
            deserialized_task.get_extra_links(datetime(2020, 7, 20), DataprocJobLink.name),
            "",
        )


class TestDataProcHadoopOperator(unittest.TestCase):
    args = ["wordcount", "gs://pub/shakespeare/rose.txt"]
    jar = "file:///usr/lib/spark/examples/jars/spark-examples.jar"
    job_id = "uuid_id"
    job = {
        "reference": {"project_id": GCP_PROJECT, "job_id": "{{task.task_id}}_{{ds_nodash}}_" + job_id},
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "hadoop_job": {"main_jar_file_uri": jar, "args": args},
    }

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_deprecation_warning(self, mock_hook):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocSubmitHadoopJobOperator(
                task_id=TASK_ID, region=GCP_LOCATION, main_jar=self.jar, arguments=self.args
            )
        assert_warning("DataprocSubmitJobOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_uuid):
        mock_uuid.return_value = self.job_id
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_uuid.return_value = self.job_id

        op = DataprocSubmitHadoopJobOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            main_jar=self.jar,
            arguments=self.args,
        )
        job = op.generate_job()
        assert self.job == job


class TestDataProcPySparkOperator(unittest.TestCase):
    uri = "gs://{}/{}"
    job_id = "uuid_id"
    job = {
        "reference": {"project_id": GCP_PROJECT, "job_id": "{{task.task_id}}_{{ds_nodash}}_" + job_id},
        "placement": {"cluster_name": "cluster-1"},
        "labels": {"airflow-version": AIRFLOW_VERSION},
        "pyspark_job": {"main_python_file_uri": uri},
    }

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_deprecation_warning(self, mock_hook):
        with pytest.warns(DeprecationWarning) as warnings:
            DataprocSubmitPySparkJobOperator(task_id=TASK_ID, region=GCP_LOCATION, main=self.uri)
        assert_warning("DataprocSubmitJobOperator", warnings)

    @mock.patch(DATAPROC_PATH.format("uuid.uuid4"))
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook, mock_uuid):
        mock_hook.return_value.project_id = GCP_PROJECT
        mock_uuid.return_value = self.job_id

        op = DataprocSubmitPySparkJobOperator(
            task_id=TASK_ID, region=GCP_LOCATION, gcp_conn_id=GCP_CONN_ID, main=self.uri
        )
        job = op.generate_job()
        assert self.job == job


class TestDataprocCreateWorkflowTemplateOperator:
    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_execute(self, mock_hook):
        op = DataprocCreateWorkflowTemplateOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            template=WORKFLOW_TEMPLATE,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_workflow_template.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            template=WORKFLOW_TEMPLATE,
        )
