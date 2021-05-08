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

from typing import Callable, Dict, Iterable, List, Optional, Union

from airflow.decorators.python import python_task
from airflow.decorators.python_virtualenv import _virtualenv_task
from airflow.decorators.task_group import task_group  # noqa # pylint: disable=unused-import
from airflow.models.dag import dag  # noqa # pylint: disable=unused-import


class _TaskDecorator:
    def __call__(
        self, python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
    ):
        """
        Python operator decorator. Wraps a function into an Airflow operator.
        Accepts kwargs for operator kwarg. This decorator can be reused in a single DAG.

        :param python_callable: Function to decorate
        :type python_callable: Optional[Callable]
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        """
        return self.python(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)

    @staticmethod
    def python(python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs):
        """
        Python operator decorator. Wraps a function into an Airflow operator.
        Accepts kwargs for operator kwarg. This decorator can be reused in a single DAG.

        :param python_callable: Function to decorate
        :type python_callable: Optional[Callable]
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        """
        return python_task(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)

    @staticmethod
    def virtualenv(
        python_callable: Optional[Callable] = None,
        multiple_outputs: Optional[bool] = None,
        requirements: Optional[Iterable[str]] = None,
        python_version: Optional[Union[str, int, float]] = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        string_args: Optional[Iterable[str]] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Allows one to run a function in a virtualenv that is
        created and destroyed automatically (with certain caveats).

        The function must be defined using def, and not be
        part of a class. All imports must happen inside the function
        and no variables outside of the scope may be referenced. A global scope
        variable named virtualenv_string_args will be available (populated by
        string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
        can use a return value.
        Note that if your virtualenv runs in a different Python major version than Airflow,
        you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
        Airflow through plugins. You can use string_args though.

        .. seealso::
            For more information on how to use this operator, take a look at the guide:
            :ref:`howto/operator:PythonVirtualenvOperator`

        :param python_callable: A python function with no references to outside variables,
            defined with def, which will be run in a virtualenv
        :type python_callable: function
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param requirements: A list of requirements as specified in a pip install command
        :type requirements: list[str]
        :param python_version: The Python version to run the virtualenv with. Note that
            both 2 and 2.7 are acceptable forms.
        :type python_version: Optional[Union[str, int, float]]
        :param use_dill: Whether to use dill to serialize
            the args and result (pickle is default). This allow more complex types
            but requires you to include dill in your requirements.
        :type use_dill: bool
        :param system_site_packages: Whether to include
            system_site_packages in your virtualenv.
            See virtualenv documentation for more information.
        :type system_site_packages: bool
        :param op_args: A list of positional arguments to pass to python_callable.
        :type op_args: list
        :param op_kwargs: A dict of keyword arguments to pass to python_callable.
        :type op_kwargs: dict
        :param string_args: Strings that are present in the global var virtualenv_string_args,
            available to python_callable at runtime as a list[str]. Note that args are split
            by newline.
        :type string_args: list[str]
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied
        :type templates_dict: dict of str
        :param templates_exts: a list of file extensions to resolve while
            processing templated fields, for examples ``['.sql', '.hql']``
        :type templates_exts: list[str]
        """
        return _virtualenv_task(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            requirements=requirements,
            python_version=python_version,
            use_dill=use_dill,
            system_site_packages=system_site_packages,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )

    @staticmethod
    def docker(  # pylint: disable=too-many-arguments,too-many-locals
        python_callable: Optional[Callable] = None,
        multiple_outputs: Optional[bool] = None,
        image: str = "",
        api_version: Optional[str] = None,
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
    ):
        """
        :param python_callable: A python function with no references to outside variables,
            defined with def, which will be run in a virtualenv
        :type python_callable: function
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param image: Docker image from which to create the container.
            If image tag is omitted, "latest" will be used.
        :type image: str
        :param api_version: Remote API version. Set to ``auto`` to automatically
            detect the server's version.
        :type api_version: str
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
        from airflow.providers.docker.decorators.docker import _docker_task

        return _docker_task(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            image=image,
            api_version=api_version,
            container_name=container_name,
            cpus=cpus,
            docker_url=docker_url,
            environment=environment,
            private_environment=private_environment,
            force_pull=force_pull,
            mem_limit=mem_limit,
            host_tmp_dir=host_tmp_dir,
            network_mode=network_mode,
            tls_ca_cert=tls_ca_cert,
            tls_client_cert=tls_client_cert,
            tls_client_key=tls_client_key,
            tls_hostname=tls_hostname,
            tls_ssl_version=tls_ssl_version,
            tmp_dir=tmp_dir,
            user=user,
            volumes=volumes,
            working_dir=working_dir,
            xcom_all=xcom_all,
            docker_conn_id=docker_conn_id,
            dns=dns,
            dns_search=dns_search,
            auto_remove=auto_remove,
            shm_size=shm_size,
            tty=tty,
            privileged=privileged,
            cap_add=cap_add,
            extra_hosts=extra_hosts,
            **kwargs,
        )


task = _TaskDecorator()
