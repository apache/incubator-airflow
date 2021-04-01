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
import json
import unittest
from unittest import mock

from airflow.models import Connection
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook

TEST_PROJECT_ID = "test-project-id"

TEST_INSTANCE_NAME = "test-instance"
TEST_ZONE = "test-zone-42"
INTERNAL_IP = "192.9.9.9"
EXTERNAL_IP = "192.3.3.3"
TEST_PUB_KEY = "root:NAME AYZ root"
TEST_PUB_KEY2 = "root:NAME MNJ root"


class TestComputeEngineHookWithPassedProjectId(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    def test_get_conn_default_configuration(
        self, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook
    ):
        mock_paramiko.SSHException = Exception
        mock_paramiko.RSAKey.generate.return_value.get_name.return_value = "NAME"
        mock_paramiko.RSAKey.generate.return_value.get_base64.return_value = "AYZ"

        mock_compute_hook.return_value.project_id = TEST_PROJECT_ID
        mock_compute_hook.return_value.get_instance_address.return_value = EXTERNAL_IP

        mock_os_login_hook.return_value._get_credentials_email.return_value = "test-example@example.org"
        mock_os_login_hook.return_value.import_ssh_public_key.return_value.login_profile.posix_accounts = [
            mock.MagicMock(username="test-username")
        ]

        hook = ComputeEngineSSHHook(instance_name=TEST_INSTANCE_NAME, zone=TEST_ZONE)
        result = hook.get_conn()
        assert mock_ssh_client.return_value == result

        mock_paramiko.RSAKey.generate.assert_called_once_with(2048)
        mock_compute_hook.assert_has_calls(
            [
                mock.call(delegate_to=None, gcp_conn_id="google_cloud_default"),
                mock.call().get_instance_address(
                    project_id=TEST_PROJECT_ID,
                    resource_id=TEST_INSTANCE_NAME,
                    use_internal_ip=False,
                    zone=TEST_ZONE,
                ),
            ]
        )
        mock_os_login_hook.assert_has_calls(
            [
                mock.call(delegate_to=None, gcp_conn_id="google_cloud_default"),
                mock.call()._get_credentials_email(),
                mock.call().import_ssh_public_key(
                    ssh_public_key={"key": "NAME AYZ root", "expiration_time_usec": mock.ANY},
                    project_id="test-project-id",
                    user=mock_os_login_hook.return_value._get_credentials_email.return_value,
                ),
            ]
        )
        mock_ssh_client.assert_has_calls(
            [
                mock.call(mock_compute_hook.return_value),
                mock.call().set_missing_host_key_policy(mock_paramiko.AutoAddPolicy.return_value),
                mock.call().connect(
                    hostname=EXTERNAL_IP,
                    look_for_keys=False,
                    pkey=mock_paramiko.RSAKey.generate.return_value,
                    sock=None,
                    username="test-username",
                ),
            ]
        )

        mock_compute_hook.return_value.set_instance_metadata.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    def test_get_conn_authorize_using_instance_metadata(
        self, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook
    ):
        mock_paramiko.SSHException = Exception
        mock_paramiko.RSAKey.generate.return_value.get_name.return_value = "NAME"
        mock_paramiko.RSAKey.generate.return_value.get_base64.return_value = "AYZ"

        mock_compute_hook.return_value.project_id = TEST_PROJECT_ID
        mock_compute_hook.return_value.get_instance_address.return_value = EXTERNAL_IP

        mock_compute_hook.return_value.get_instance_info.return_value = {"metadata": {}}

        hook = ComputeEngineSSHHook(instance_name=TEST_INSTANCE_NAME, zone=TEST_ZONE, use_oslogin=False)
        result = hook.get_conn()
        assert mock_ssh_client.return_value == result

        mock_paramiko.RSAKey.generate.assert_called_once_with(2048)
        mock_compute_hook.assert_has_calls(
            [
                mock.call(delegate_to=None, gcp_conn_id="google_cloud_default"),
                mock.call().get_instance_address(
                    project_id=TEST_PROJECT_ID,
                    resource_id=TEST_INSTANCE_NAME,
                    use_internal_ip=False,
                    zone=TEST_ZONE,
                ),
                mock.call().get_instance_info(
                    project_id=TEST_PROJECT_ID, resource_id=TEST_INSTANCE_NAME, zone=TEST_ZONE
                ),
                mock.call().set_instance_metadata(
                    metadata={"items": [{"key": "ssh-keys", "value": f"{TEST_PUB_KEY}\n"}]},
                    project_id=TEST_PROJECT_ID,
                    resource_id=TEST_INSTANCE_NAME,
                    zone=TEST_ZONE,
                ),
            ]
        )

        mock_ssh_client.assert_has_calls(
            [
                mock.call(mock_compute_hook.return_value),
                mock.call().set_missing_host_key_policy(mock_paramiko.AutoAddPolicy.return_value),
                mock.call().connect(
                    hostname=EXTERNAL_IP,
                    look_for_keys=False,
                    pkey=mock_paramiko.RSAKey.generate.return_value,
                    sock=None,
                    username="root",
                ),
            ]
        )

        mock_os_login_hook.return_value.import_ssh_public_key.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    def test_get_conn_authorize_using_instance_metadata_append_ssh_keys(
        self, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook
    ):
        del mock_os_login_hook
        mock_paramiko.SSHException = Exception
        mock_paramiko.RSAKey.generate.return_value.get_name.return_value = "NAME"
        mock_paramiko.RSAKey.generate.return_value.get_base64.return_value = "AYZ"

        mock_compute_hook.return_value.project_id = TEST_PROJECT_ID
        mock_compute_hook.return_value.get_instance_address.return_value = EXTERNAL_IP

        mock_compute_hook.return_value.get_instance_info.return_value = {
            "metadata": {"items": [{"key": "ssh-keys", "value": f"{TEST_PUB_KEY2}\n"}]}
        }

        hook = ComputeEngineSSHHook(instance_name=TEST_INSTANCE_NAME, zone=TEST_ZONE, use_oslogin=False)
        result = hook.get_conn()
        assert mock_ssh_client.return_value == result

        mock_compute_hook.return_value.set_instance_metadata.assert_called_once_with(
            metadata={"items": [{"key": "ssh-keys", "value": f"{TEST_PUB_KEY}\n{TEST_PUB_KEY2}\n"}]},
            project_id=TEST_PROJECT_ID,
            resource_id=TEST_INSTANCE_NAME,
            zone=TEST_ZONE,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    def test_get_conn_private_ip(self, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook):
        del mock_os_login_hook
        mock_paramiko.SSHException = Exception
        mock_paramiko.RSAKey.generate.return_value.get_name.return_value = "NAME"
        mock_paramiko.RSAKey.generate.return_value.get_base64.return_value = "AYZ"

        mock_compute_hook.return_value.project_id = TEST_PROJECT_ID
        mock_compute_hook.return_value.get_instance_address.return_value = INTERNAL_IP

        mock_compute_hook.return_value.get_instance_info.return_value = {"metadata": {}}

        hook = ComputeEngineSSHHook(
            instance_name=TEST_INSTANCE_NAME, zone=TEST_ZONE, use_oslogin=False, use_internal_ip=True
        )
        result = hook.get_conn()
        assert mock_ssh_client.return_value == result

        mock_compute_hook.return_value.get_instance_address.assert_called_once_with(
            project_id=TEST_PROJECT_ID, resource_id=TEST_INSTANCE_NAME, use_internal_ip=True, zone=TEST_ZONE
        )
        mock_ssh_client.return_value.connect.assert_called_once_with(
            hostname=INTERNAL_IP, look_for_keys=mock.ANY, pkey=mock.ANY, sock=mock.ANY, username=mock.ANY
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    def test_get_conn_custom_hostname(
        self, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook
    ):
        del mock_os_login_hook
        mock_paramiko.SSHException = Exception

        hook = ComputeEngineSSHHook(
            instance_name=TEST_INSTANCE_NAME,
            zone=TEST_ZONE,
            use_oslogin=False,
            hostname="custom-hostname",
        )
        result = hook.get_conn()
        assert mock_ssh_client.return_value == result

        mock_compute_hook.return_value.get_instance_address.assert_not_called()
        mock_ssh_client.return_value.connect.assert_called_once_with(
            hostname="custom-hostname",
            look_for_keys=mock.ANY,
            pkey=mock.ANY,
            sock=mock.ANY,
            username=mock.ANY,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    def test_get_conn_iap_tunnel(self, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook):
        del mock_os_login_hook
        mock_paramiko.SSHException = Exception

        mock_compute_hook.return_value.project_id = TEST_PROJECT_ID

        hook = ComputeEngineSSHHook(
            instance_name=TEST_INSTANCE_NAME, zone=TEST_ZONE, use_oslogin=False, use_iap_tunnel=True
        )
        result = hook.get_conn()
        assert mock_ssh_client.return_value == result

        mock_ssh_client.return_value.connect.assert_called_once_with(
            hostname=mock.ANY,
            look_for_keys=mock.ANY,
            pkey=mock.ANY,
            sock=mock_paramiko.ProxyCommand.return_value,
            username=mock.ANY,
        )
        mock_paramiko.ProxyCommand.assert_called_once_with(
            f"gcloud compute start-iap-tunnel {TEST_INSTANCE_NAME} 22 "
            f"--listen-on-stdin --project={TEST_PROJECT_ID} "
            f"--zone={TEST_ZONE} --verbosity=warning"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.OSLoginHook")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.paramiko")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh._GCloudAuthorizedSSHClient")
    @mock.patch("airflow.providers.google.cloud.hooks.compute_ssh.time.sleep")
    def test_get_conn_retry_on_connection_error(
        self, mock_time, mock_ssh_client, mock_paramiko, mock_os_login_hook, mock_compute_hook
    ):
        del mock_os_login_hook
        del mock_compute_hook

        class CustomException(Exception):
            pass

        mock_paramiko.SSHException = CustomException
        mock_ssh_client.return_value.connect.side_effect = [CustomException, CustomException, True]
        hook = ComputeEngineSSHHook(instance_name=TEST_INSTANCE_NAME, zone=TEST_ZONE)
        hook.get_conn()

        assert 3 == mock_ssh_client.return_value.connect.call_count

    def test_read_configuration_from_connection(self):
        conn = Connection(
            conn_type="gcpssh",
            login="conn-user",
            host="conn-host",
            extra=json.dumps(
                {
                    "extra__google_cloud_platform__instance_name": "conn-instance-name",
                    "extra__google_cloud_platform__zone": "zone",
                    "extra__google_cloud_platform__use_internal_ip": True,
                    "extra__google_cloud_platform__use_iap_tunnel": True,
                    "extra__google_cloud_platform__use_oslogin": False,
                    "extra__google_cloud_platform__expire_time": 4242,
                }
            ),
        )
        conn_uri = conn.get_uri()
        with mock.patch.dict("os.environ", AIRFLOW_CONN_GCPSSH=conn_uri):
            hook = ComputeEngineSSHHook(gcp_conn_id="gcpssh")
            hook._load_connection_config()
        assert "conn-instance-name" == hook.instance_name
        assert "conn-host" == hook.hostname
        assert "conn-user" == hook.user
        assert hook.use_internal_ip is True
        assert isinstance(hook.use_internal_ip, bool)
        assert hook.use_iap_tunnel is True
        assert isinstance(hook.use_iap_tunnel, bool)
        assert hook.use_oslogin is False
        assert isinstance(hook.use_oslogin, bool)
        assert 4242 == hook.expire_time
        assert isinstance(hook.expire_time, int)

    def test_read_configuration_from_connection_empty_config(self):
        conn = Connection(
            conn_type="gcpssh",
            extra=json.dumps({}),
        )
        conn_uri = conn.get_uri()
        with mock.patch.dict("os.environ", AIRFLOW_CONN_GCPSSH=conn_uri):
            hook = ComputeEngineSSHHook(gcp_conn_id="gcpssh")
            hook._load_connection_config()
        assert None is hook.instance_name
        assert None is hook.hostname
        assert "root" == hook.user
        assert False is hook.use_internal_ip
        assert isinstance(hook.use_internal_ip, bool)
        assert False is hook.use_iap_tunnel
        assert isinstance(hook.use_iap_tunnel, bool)
        assert False is hook.use_oslogin
        assert isinstance(hook.use_oslogin, bool)
        assert 300 == hook.expire_time
        assert isinstance(hook.expire_time, int)
