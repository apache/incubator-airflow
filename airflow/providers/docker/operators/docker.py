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
"""Implements Docker operator"""
import ast
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Union

from docker import APIClient, tls

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.docker.hooks.docker import DockerHook
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes
class DockerOperator(BaseOperator):
    """
    Execute a command inside a docker container.

    A temporary directory is created on the host and
    mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container.
    The path to the mounted directory can be accessed
    via the environment variable ``AIRFLOW_TMP_DIR``.

    If a login to a private registry is required prior to pulling the image, a
    Docker connection needs to be configured in Airflow and the connection ID
    be provided with the parameter ``docker_conn_id``.

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used.
    :type image: str
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :type api_version: str
    :param command: Command to be run in the container. (templated)
    :type command: str or list
    :param container_name: Name of the container. Optional (templated)
    :type container_name: str or None
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
    :param docker_url: URL of the host running the docker daemon.
        Default is unix://var/run/docker.sock
    :type docker_url: str
    :param environment: Environment variables to set in the container. (templated)
    :type environment: dict
    :param private_environment: Private environment variables to set in the container.
        These are not templated, and hidden from the website.
    :type private_environment: dict
    :param force_pull: Pull the docker image on every run. Default is False.
    :type force_pull: bool
    :param mem_limit: Maximum amount of memory the container can use.
        Either a float value, which represents the limit in bytes,
        or a string like ``128m`` or ``1g``.
    :type mem_limit: float or str
    :param host_tmp_dir: Specify the location of the temporary directory on the host which will
        be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
    :type host_tmp_dir: str
    :param network_mode: Network mode for the container.
    :type network_mode: str
    :param tls_ca_cert: Path to a PEM-encoded certificate authority
        to secure the docker connection.
    :type tls_ca_cert: str
    :param tls_client_cert: Path to the PEM-encoded certificate
        used to authenticate docker client.
    :type tls_client_cert: str
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
    :type tls_client_key: str
    :param tls_hostname: Hostname to match against
        the docker server certificate or False to disable the check.
    :type tls_hostname: str or bool
    :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
    :type tls_ssl_version: str
    :param tmp_dir: Mount point inside the container to
        a temporary directory created on the host by the operator.
        The path is also made available via the environment variable
        ``AIRFLOW_TMP_DIR`` inside the container.
    :type tmp_dir: str
    :param user: Default user inside the docker container.
    :type user: int or str
    :param volumes: List of volumes to mount into the container, e.g.
        ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
    :type volumes: list
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client)
    :type working_dir: str
    :param xcom_all: Push all the stdout or just the last line.
        The default is False (last line).
    :type xcom_all: bool
    :param docker_conn_id: ID of the Airflow connection to use
    :type docker_conn_id: str
    :param dns: Docker custom DNS servers
    :type dns: list[str]
    :param dns_search: Docker custom DNS search domain
    :type dns_search: list[str]
    :param auto_remove: Auto-removal of the container on daemon side when the
        container's process exits.
        The default is False.
    :type auto_remove: bool
    :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
        greater than 0. If omitted uses system default.
    :type shm_size: int
    :param tty: Allocate pseudo-TTY to the container
        This needs to be set see logs of the Docker container.
    :type tty: bool
    :param privileged: Give extended privileges to this container.
    :type privileged: bool
    :param cap_add: Include container capabilities
    :type cap_add: list[str]
    """

    template_fields = ('command', 'environment', 'container_name')
    template_ext = (
        '.sh',
        '.bash',
    )

    # pylint: disable=too-many-arguments,too-many-locals
    @apply_defaults
    def __init__(
        self,
        *,
        image: str,
        api_version: Optional[str] = None,
        command: Optional[Union[str, List[str]]] = None,
        container_name: Optional[str] = None,
        cpus: float = 1.0,
        docker_url: str = 'unix://var/run/docker.sock',
        environment: Optional[Dict] = None,
        private_environment: Optional[Dict] = None,
        force_pull: bool = False,
        mem_limit: Optional[Union[float, str]] = None,
        host_tmp_dir: Optional[str] = None,
        network_mode: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_hostname: Optional[Union[str, bool]] = None,
        tls_ssl_version: Optional[str] = None,
        tmp_dir: str = '/tmp/airflow',
        user: Optional[Union[str, int]] = None,
        volumes: Optional[List[str]] = None,
        working_dir: Optional[str] = None,
        xcom_all: bool = False,
        docker_conn_id: Optional[str] = None,
        dns: Optional[List[str]] = None,
        dns_search: Optional[List[str]] = None,
        auto_remove: bool = False,
        shm_size: Optional[int] = None,
        tty: bool = False,
        privileged: bool = False,
        cap_add: Optional[Iterable[str]] = None,
        extra_hosts: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.api_version = api_version
        self.auto_remove = auto_remove
        self.command = command
        self.container_name = container_name
        self.cpus = cpus
        self.dns = dns
        self.dns_search = dns_search
        self.docker_url = docker_url
        self.environment = environment or {}
        self._private_environment = private_environment or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.host_tmp_dir = host_tmp_dir
        self.network_mode = network_mode
        self.tls_ca_cert = tls_ca_cert
        self.tls_client_cert = tls_client_cert
        self.tls_client_key = tls_client_key
        self.tls_hostname = tls_hostname
        self.tls_ssl_version = tls_ssl_version
        self.tmp_dir = tmp_dir
        self.user = user
        self.volumes = volumes or []
        self.working_dir = working_dir
        self.xcom_all = xcom_all
        self.docker_conn_id = docker_conn_id
        self.shm_size = shm_size
        self.tty = tty
        self.privileged = privileged
        self.cap_add = cap_add
        self.extra_hosts = extra_hosts
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

        self.cli = None
        self.container = None

    def get_hook(self) -> DockerHook:
        """
        Retrieves hook for the operator.

        :return: The Docker Hook
        """
        return DockerHook(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=self.__get_tls_config(),
        )

    def _run_image(self) -> Optional[str]:
        """Run a Docker container with the provided image"""
        self.log.info('Starting docker container from image %s', self.image)

        with TemporaryDirectory(prefix='airflowtmp', dir=self.host_tmp_dir) as host_tmp_dir:
            self.volumes.append(f'{host_tmp_dir}:{self.tmp_dir}')

            if not self.cli:
                raise Exception("The 'cli' should be initialized before!")
            self.container = self.cli.create_container(
                command=self.get_command(),
                name=self.container_name,
                environment={**self.environment, **self._private_environment},
                host_config=self.cli.create_host_config(
                    auto_remove=False,
                    binds=self.volumes,
                    network_mode=self.network_mode,
                    shm_size=self.shm_size,
                    dns=self.dns,
                    dns_search=self.dns_search,
                    cpu_shares=int(round(self.cpus * 1024)),
                    mem_limit=self.mem_limit,
                    cap_add=self.cap_add,
                    extra_hosts=self.extra_hosts,
                    privileged=self.privileged,
                ),
                image=self.image,
                user=self.user,
                working_dir=self.working_dir,
                tty=self.tty,
            )

            lines = self.cli.attach(container=self.container['Id'], stdout=True, stderr=True, stream=True)

            self.cli.start(self.container['Id'])

            line = ''
            for line in lines:
                line = line.strip()
                if hasattr(line, 'decode'):
                    # Note that lines returned can also be byte sequences so we have to handle decode here
                    line = line.decode('utf-8')
                self.log.info(line)

            result = self.cli.wait(self.container['Id'])
            if result['StatusCode'] != 0:
                if self.auto_remove:
                    self.cli.remove_container(self.container['Id'])
                raise AirflowException('docker container failed: ' + repr(result))

            # duplicated conditional logic because of expensive operation
            ret = None
            if self.do_xcom_push:
                ret = self.cli.logs(container=self.container['Id']) if self.xcom_all else line.encode('utf-8')

            if self.auto_remove:
                self.cli.remove_container(self.container['Id'])

            return ret

    def execute(self, context) -> Optional[str]:
        self.cli = self._get_cli()
        if not self.cli:
            raise Exception("The 'cli' should be initialized before!")

        # Pull the docker image if `force_pull` is set or image does not exist locally
        # pylint: disable=too-many-nested-blocks
        if self.force_pull or not self.cli.images(name=self.image):
            self.log.info('Pulling docker image %s', self.image)
            latest_status = {}
            for output in self.cli.pull(self.image, stream=True, decode=True):
                if isinstance(output, str):
                    self.log.info("%s", output)
                    continue
                if isinstance(output, dict) and 'status' in output:
                    output_status = output["status"]
                    if 'id' not in output:
                        self.log.info("%s", output_status)
                        continue

                    output_id = output["id"]
                    if latest_status.get(output_id) != output_status:
                        self.log.info("%s: %s", output_id, output_status)
                        latest_status[output_id] = output_status

        self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
        return self._run_image()

    def _get_cli(self) -> APIClient:
        if self.docker_conn_id:
            return self.get_hook().get_conn()
        else:
            tls_config = self.__get_tls_config()
            return APIClient(base_url=self.docker_url, version=self.api_version, tls=tls_config)

    def get_command(self) -> Union[List[str], str]:
        """
        Retrieve command(s). if command string starts with [, it returns the command list)

        :return: the command (or commands)
        :rtype: str | List[str]
        """
        if isinstance(self.command, str) and self.command.strip().find('[') == 0:
            commands = ast.literal_eval(self.command)
        else:
            commands = self.command
        return commands

    def on_kill(self) -> None:
        if self.cli is not None:
            self.log.info('Stopping docker container')
            self.cli.stop(self.container['Id'])

    def __get_tls_config(self) -> Optional[tls.TLSConfig]:
        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            # Ignore type error on SSL version here - it is deprecated and type annotation is wrong
            # it should be string
            tls_config = tls.TLSConfig(
                ca_cert=self.tls_ca_cert,
                client_cert=(self.tls_client_cert, self.tls_client_key),
                verify=True,
                ssl_version=self.tls_ssl_version,  # noqa
                assert_hostname=self.tls_hostname,
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')
        return tls_config
