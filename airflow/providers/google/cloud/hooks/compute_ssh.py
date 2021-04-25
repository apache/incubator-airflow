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
import shlex
import time
from io import StringIO
from typing import Any, Dict, Optional

import paramiko

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.retry import exponential_sleep_generator

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.hooks.os_login import OSLoginHook
from airflow.providers.ssh.hooks.ssh import SSHHook


class _GCloudAuthorizedSSHClient(paramiko.SSHClient):
    """SSH Client that maintains the context for gcloud authorization during the connection"""

    def __init__(self, google_hook, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ssh_client = paramiko.SSHClient()
        self.google_hook = google_hook
        self.decorator = None

    def connect(self, *args, **kwargs):  # pylint: disable=signature-differs
        self.decorator = self.google_hook.provide_authorized_gcloud()
        self.decorator.__enter__()
        return super().connect(*args, **kwargs)

    def close(self):
        if self.decorator:
            self.decorator.__exit__(None, None, None)
        self.decorator = None
        return super().close()

    def __exit__(self, type_, value, traceback):
        if self.decorator:
            self.decorator.__exit__(type_, value, traceback)
        self.decorator = None
        return super().__exit__(type_, value, traceback)


class ComputeEngineSSHHook(SSHHook):
    """
    Hook to connect to a remote instance in compute engine

    :param instance_name: The name of the Compute Engine instance
    :type instance_name: str
    :param zone: The zone of the Compute Engine instance
    :type zone: str
    :param user: The name of the user on which the login attempt will be made
    :type user: str
    :param project_id: The project ID of the remote instance
    :type project_id: str
    :param gcp_conn_id: The connection id to use when fetching connection info
    :type gcp_conn_id: str
    :param hostname: The hostname of the target instance. If it is not passed, it will be detected
        automatically.
    :type hostname: str
    :param use_iap_tunnel: Whether to connect through IAP tunnel
    :type use_iap_tunnel: bool
    :param use_internal_ip: Whether to connect using internal IP
    :type use_internal_ip: bool
    :param use_oslogin: Whether to manage keys using OsLogin API. If false,
        keys are managed using instance metadata
    :type use_oslogin: bool
    :param expire_time: The maximum amount of time in seconds before the private key expires
    :type expire_time: int
    :param gcp_conn_id: The connection id to use when fetching connection information
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    conn_name_attr = 'gcp_conn_id'
    default_conn_name = 'google_cloud_default'
    conn_type = 'gcpssh'
    hook_name = 'Google Cloud SSH'

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ['host', 'schema', 'login', 'password', 'port', 'extra'],
            "relabeling": {},
        }

    def __init__(  # pylint: disable=too-many-arguments
        self,
        gcp_conn_id: str = 'google_cloud_default',
        instance_name: Optional[str] = None,
        zone: Optional[str] = None,
        user: Optional[str] = 'root',
        project_id: Optional[str] = None,
        hostname: Optional[str] = None,
        use_internal_ip: bool = False,
        use_iap_tunnel: bool = False,
        use_oslogin: bool = True,
        expire_time: int = 300,
        delegate_to: Optional[str] = None,
    ) -> None:
        # Ignore original constructor
        # super().__init__()  # pylint: disable=super-init-not-called
        self.instance_name = instance_name
        self.zone = zone
        self.user = user
        self.project_id = project_id
        self.hostname = hostname
        self.use_internal_ip = use_internal_ip
        self.use_iap_tunnel = use_iap_tunnel
        self.use_oslogin = use_oslogin
        self.expire_time = expire_time
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self._conn: Optional[Any] = None

    @cached_property
    def _oslogin_hook(self) -> OSLoginHook:
        return OSLoginHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

    @cached_property
    def _compute_hook(self) -> ComputeEngineHook:
        return ComputeEngineHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

    def _load_connection_config(self):
        def _boolify(value):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                if value.lower() == 'false':
                    return False
                elif value.lower() == 'true':
                    return True
            return False

        def intify(key, value, default):
            if value is None:
                return default
            if isinstance(value, str) and value.strip() == '':
                return default
            try:
                return int(value)
            except ValueError:
                raise AirflowException(
                    f"The {key} field should be a integer. "
                    f"Current value: \"{value}\" (type: {type(value)}). "
                    f"Please check the connection configuration."
                )

        conn = self.get_connection(self.gcp_conn_id)
        if conn and conn.conn_type == "gcpssh":
            self.instance_name = self._compute_hook._get_field(  # pylint: disable=protected-access
                "instance_name", self.instance_name
            )
            self.zone = self._compute_hook._get_field("zone", self.zone)  # pylint: disable=protected-access
            self.user = conn.login if conn.login else self.user
            # self.project_id is skipped intentionally
            self.hostname = conn.host if conn.host else self.hostname
            self.use_internal_ip = _boolify(
                self._compute_hook._get_field("use_internal_ip")  # pylint: disable=protected-access
            )
            self.use_iap_tunnel = _boolify(
                self._compute_hook._get_field("use_iap_tunnel")  # pylint: disable=protected-access
            )
            self.use_oslogin = _boolify(
                self._compute_hook._get_field("use_oslogin")  # pylint: disable=protected-access
            )
            self.expire_time = intify(
                "expire_time",
                self._compute_hook._get_field("expire_time"),  # pylint: disable=protected-access
                self.expire_time,
            )

    def get_conn(self) -> paramiko.SSHClient:
        """Return SSH connection."""
        self._load_connection_config()
        if not self.project_id:
            self.project_id = self._compute_hook.project_id

        missing_fields = [k for k in ["instance_name", "zone", "project_id"] if not getattr(self, k)]
        if not self.instance_name or not self.zone or not self.project_id:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters be passed either as "
                "keyword parameter or as extra field in Airfow connection definition. Both are not set!"
            )

        self.log.info(
            "Connecting to instance: instance_name=%s, user=%s, zone=%s, "
            "use_internal_ip=%s, use_iap_tunnel=%s, use_os_login=%s",
            self.instance_name,
            self.user,
            self.zone,
            self.use_internal_ip,
            self.use_iap_tunnel,
            self.use_oslogin,
        )
        if not self.hostname:
            hostname = self._compute_hook.get_instance_address(
                zone=self.zone,
                resource_id=self.instance_name,
                project_id=self.project_id,
                use_internal_ip=self.use_internal_ip or self.use_iap_tunnel,
            )
        else:
            hostname = self.hostname

        privkey, pubkey = self._generate_ssh_key(self.user)
        if self.use_oslogin:
            user = self._authorize_os_login(pubkey)
        else:
            user = self.user
            self._authorize_compute_engine_instance_metadata(pubkey)

        proxy_command = None
        if self.use_iap_tunnel:
            proxy_command_args = [
                'gcloud',
                'compute',
                'start-iap-tunnel',
                str(self.instance_name),
                '22',
                '--listen-on-stdin',
                f'--project={self.project_id}',
                f'--zone={self.zone}',
                '--verbosity=warning',
            ]
            proxy_command = " ".join(shlex.quote(arg) for arg in proxy_command_args)

        sshclient = self._connect_to_instance(user, hostname, privkey, proxy_command)
        return sshclient

    def _connect_to_instance(self, user, hostname, pkey, proxy_command) -> paramiko.SSHClient:
        self.log.info("Opening remote connection to host: username=%s, hostname=%s", user, hostname)
        max_time_to_wait = 10
        for time_to_wait in exponential_sleep_generator(initial=1, maximum=max_time_to_wait):
            try:
                client = _GCloudAuthorizedSSHClient(self._compute_hook)
                # Default is RejectPolicy
                # No known host checking since we are not storing privatekey
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                client.connect(
                    hostname=hostname,
                    username=user,
                    pkey=pkey,
                    sock=paramiko.ProxyCommand(proxy_command) if proxy_command else None,
                    look_for_keys=False,
                )
                return client
            except paramiko.SSHException:
                # exponential_sleep_generator is an infinite generator, so we need to
                # check the end condition.
                if time_to_wait == max_time_to_wait:
                    raise
            self.log.info("Failed to connect. Waiting %ds to retry", time_to_wait)
            time.sleep(time_to_wait)
        raise AirflowException("Caa not connect to instance")

    def _authorize_compute_engine_instance_metadata(self, pubkey):
        self.log.info("Appending SSH public key to instance metadata")
        instance_info = self._compute_hook.get_instance_info(
            zone=self.zone, resource_id=self.instance_name, project_id=self.project_id
        )

        keys = self.user + ":" + pubkey + "\n"
        metadata = instance_info['metadata']
        items = metadata.get("items", [])
        for item in items:
            if item.get("key") == "ssh-keys":
                keys += item["value"]
                item['value'] = keys
                break
        else:
            new_dict = dict(key='ssh-keys', value=keys)
            metadata['items'] = [new_dict]

        self._compute_hook.set_instance_metadata(
            zone=self.zone, resource_id=self.instance_name, metadata=metadata, project_id=self.project_id
        )

    def _authorize_os_login(self, pubkey):
        username = self._oslogin_hook._get_credentials_email()  # pylint: disable=protected-access
        self.log.info("Importing SSH public key using OSLogin: user=%s", username)
        expiration = int((time.time() + self.expire_time) * 1000000)
        ssh_public_key = {"key": pubkey, "expiration_time_usec": expiration}
        response = self._oslogin_hook.import_ssh_public_key(
            user=username, ssh_public_key=ssh_public_key, project_id=self.project_id
        )
        profile = response.login_profile
        account = profile.posix_accounts[0]
        user = account.username
        return user

    def _generate_ssh_key(self, user):
        try:
            self.log.info("Generating ssh keys...")
            pkey_file = StringIO()
            pkey_obj = paramiko.RSAKey.generate(2048)
            pkey_obj.write_private_key(pkey_file)
            pubkey = f"{pkey_obj.get_name()} {pkey_obj.get_base64()} {user}"
            return pkey_obj, pubkey
        except (OSError, paramiko.SSHException) as err:
            raise AirflowException(f"Error encountered creating ssh keys, {err}")
