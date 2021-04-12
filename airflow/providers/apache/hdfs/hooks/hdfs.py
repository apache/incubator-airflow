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
"""Hook for HDFS operations"""
from typing import Any, Optional

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

try:
    from snakebite.client import AutoConfigClient, Client, HAClient, Namenode  # pylint: disable=syntax-error

    snakebite_loaded = True
except ImportError:
    snakebite_loaded = False


class HDFSHookException(AirflowException):
    """Exception specific for HDFS"""


class HDFSHook(BaseHook):
    """
    Interact with HDFS. This class is a wrapper around the snakebite library.

    :param hdfs_conn_id: Connection id to fetch connection info
    :type hdfs_conn_id: str
    :param proxy_user: effective user for HDFS operations
    :type proxy_user: str
    :param autoconfig: use snakebite's automatically configured client
    :type autoconfig: bool
    """

    conn_name_attr = 'hdfs_conn_id'
    default_conn_name = 'hdfs_default'
    conn_type = 'hdfs'
    hook_name = 'HDFS'

    def __init__(
        self, hdfs_conn_id: str = 'hdfs_default', proxy_user: Optional[str] = None, autoconfig: bool = False
    ):
        super().__init__()
        if not snakebite_loaded:
            raise ImportError(
                'This HDFSHook implementation requires snakebite, but '
                'snakebite is not compatible with Python 3 '
                '(as of August 2015). Please help by submitting a PR!'
            )
        self.hdfs_conn_id = hdfs_conn_id
        self.proxy_user = proxy_user
        self.autoconfig = autoconfig

    def get_conn(self) -> Any:
        """Returns a snakebite HDFSClient object."""
        # When using HAClient, proxy_user must be the same, so is ok to always
        # take the first.
        effective_user = self.proxy_user
        autoconfig = self.autoconfig
        use_sasl = conf.get('core', 'security') == 'kerberos'

        try:
            connections = self.get_connections(self.hdfs_conn_id)

            if not effective_user:
                effective_user = connections[0].login
            if not autoconfig:
                autoconfig = connections[0].extra_dejson.get('autoconfig', False)
            hdfs_namenode_principal = connections[0].extra_dejson.get('hdfs_namenode_principal')
        except AirflowException:
            if not autoconfig:
                raise

        if autoconfig:
            # will read config info from $HADOOP_HOME conf files
            client = AutoConfigClient(effective_user=effective_user, use_sasl=use_sasl)
        elif len(connections) == 1:
            client = Client(
                connections[0].host,
                connections[0].port,
                effective_user=effective_user,
                use_sasl=use_sasl,
                hdfs_namenode_principal=hdfs_namenode_principal,
            )
        elif len(connections) > 1:
            name_node = [Namenode(conn.host, conn.port) for conn in connections]
            client = HAClient(
                name_node,
                effective_user=effective_user,
                use_sasl=use_sasl,
                hdfs_namenode_principal=hdfs_namenode_principal,
            )
        else:
            raise HDFSHookException("conn_id doesn't exist in the repository and autoconfig is not specified")

        return client
