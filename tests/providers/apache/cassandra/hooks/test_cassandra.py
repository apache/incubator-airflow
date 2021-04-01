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

import unittest
from unittest import mock

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    RoundRobinPolicy,
    TokenAwarePolicy,
    WhiteListRoundRobinPolicy,
)

from airflow.models import Connection
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.utils import db


@pytest.mark.integration("cassandra")
class TestCassandraHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='cassandra_test',
                conn_type='cassandra',
                host='host-1,host-2',
                port='9042',
                schema='test_keyspace',
                extra='{"load_balancing_policy":"TokenAwarePolicy","protocol_version":4}',
            )
        )
        db.merge_conn(
            Connection(
                conn_id='cassandra_default_with_schema',
                conn_type='cassandra',
                host='cassandra',
                port='9042',
                schema='s',
            )
        )

        hook = CassandraHook("cassandra_default")
        session = hook.get_conn()
        cqls = [
            "DROP SCHEMA IF EXISTS s",
            """
                CREATE SCHEMA s WITH REPLICATION =
                    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """,
        ]
        for cql in cqls:
            session.execute(cql)

        session.shutdown()
        hook.shutdown_cluster()

    def test_get_conn(self):
        with mock.patch.object(Cluster, "__init__") as mock_cluster_ctor:
            mock_cluster_ctor.return_value = None
            CassandraHook(cassandra_conn_id='cassandra_test')
            mock_cluster_ctor.assert_called_once_with(
                contact_points=['host-1', 'host-2'],
                port=9042,
                protocol_version=4,
                load_balancing_policy=mock.ANY,
            )

            assert isinstance(mock_cluster_ctor.call_args[1]['load_balancing_policy'], TokenAwarePolicy)

    def test_get_lb_policy_with_no_args(self):
        # test LB policies with no args
        self._assert_get_lb_policy('RoundRobinPolicy', {}, RoundRobinPolicy)
        self._assert_get_lb_policy('DCAwareRoundRobinPolicy', {}, DCAwareRoundRobinPolicy)
        self._assert_get_lb_policy(
            'TokenAwarePolicy', {}, TokenAwarePolicy, expected_child_policy_type=RoundRobinPolicy
        )

    def test_get_lb_policy_with_args(self):
        # test DCAwareRoundRobinPolicy with args
        self._assert_get_lb_policy(
            'DCAwareRoundRobinPolicy',
            {'local_dc': 'foo', 'used_hosts_per_remote_dc': '3'},
            DCAwareRoundRobinPolicy,
        )

        # test WhiteListRoundRobinPolicy with args
        fake_addr_info = [
            ['family', 'sockettype', 'proto', 'canonname', ('2606:2800:220:1:248:1893:25c8:1946', 80, 0, 0)]
        ]
        with mock.patch('socket.getaddrinfo', return_value=fake_addr_info):
            self._assert_get_lb_policy(
                'WhiteListRoundRobinPolicy', {'hosts': ['host1', 'host2']}, WhiteListRoundRobinPolicy
            )

        # test TokenAwarePolicy with args
        with mock.patch('socket.getaddrinfo', return_value=fake_addr_info):
            self._assert_get_lb_policy(
                'TokenAwarePolicy',
                {
                    'child_load_balancing_policy': 'WhiteListRoundRobinPolicy',
                    'child_load_balancing_policy_args': {'hosts': ['host-1', 'host-2']},
                },
                TokenAwarePolicy,
                expected_child_policy_type=WhiteListRoundRobinPolicy,
            )

    def test_get_lb_policy_invalid_policy(self):
        # test invalid policy name should default to RoundRobinPolicy
        self._assert_get_lb_policy('DoesNotExistPolicy', {}, RoundRobinPolicy)

        # test invalid child policy name should default child policy to RoundRobinPolicy
        self._assert_get_lb_policy(
            'TokenAwarePolicy', {}, TokenAwarePolicy, expected_child_policy_type=RoundRobinPolicy
        )
        self._assert_get_lb_policy(
            'TokenAwarePolicy',
            {'child_load_balancing_policy': 'DoesNotExistPolicy'},
            TokenAwarePolicy,
            expected_child_policy_type=RoundRobinPolicy,
        )

    def test_get_lb_policy_no_host_for_allow_list(self):
        # test host not specified for WhiteListRoundRobinPolicy should throw exception
        self._assert_get_lb_policy(
            'WhiteListRoundRobinPolicy', {}, WhiteListRoundRobinPolicy, should_throw=True
        )
        self._assert_get_lb_policy(
            'TokenAwarePolicy',
            {'child_load_balancing_policy': 'WhiteListRoundRobinPolicy'},
            TokenAwarePolicy,
            expected_child_policy_type=RoundRobinPolicy,
            should_throw=True,
        )

    def _assert_get_lb_policy(
        self,
        policy_name,
        policy_args,
        expected_policy_type,
        expected_child_policy_type=None,
        should_throw=False,
    ):
        thrown = False
        try:
            policy = CassandraHook.get_lb_policy(policy_name, policy_args)
            assert isinstance(policy, expected_policy_type)
            if expected_child_policy_type:
                assert isinstance(policy._child_policy, expected_child_policy_type)
        except Exception:  # pylint: disable=broad-except
            thrown = True
        assert should_throw == thrown

    def test_record_exists_with_keyspace_from_cql(self):
        hook = CassandraHook("cassandra_default")
        session = hook.get_conn()
        cqls = [
            "DROP TABLE IF EXISTS s.t",
            "CREATE TABLE s.t (pk1 text, pk2 text, c text, PRIMARY KEY (pk1, pk2))",
            "INSERT INTO s.t (pk1, pk2, c) VALUES ('foo', 'bar', 'baz')",
        ]
        for cql in cqls:
            session.execute(cql)

        assert hook.record_exists("s.t", {"pk1": "foo", "pk2": "bar"})
        assert not hook.record_exists("s.t", {"pk1": "foo", "pk2": "baz"})

        session.shutdown()
        hook.shutdown_cluster()

    def test_record_exists_with_keyspace_from_session(self):
        hook = CassandraHook("cassandra_default_with_schema")
        session = hook.get_conn()
        cqls = [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (pk1 text, pk2 text, c text, PRIMARY KEY (pk1, pk2))",
            "INSERT INTO t (pk1, pk2, c) VALUES ('foo', 'bar', 'baz')",
        ]
        for cql in cqls:
            session.execute(cql)

        assert hook.record_exists("t", {"pk1": "foo", "pk2": "bar"})
        assert not hook.record_exists("t", {"pk1": "foo", "pk2": "baz"})

        session.shutdown()
        hook.shutdown_cluster()

    def test_table_exists_with_keyspace_from_cql(self):
        hook = CassandraHook("cassandra_default")
        session = hook.get_conn()
        cqls = [
            "DROP TABLE IF EXISTS s.t",
            "CREATE TABLE s.t (pk1 text PRIMARY KEY)",
        ]
        for cql in cqls:
            session.execute(cql)

        assert hook.table_exists("s.t")
        assert not hook.table_exists("s.u")

        session.shutdown()
        hook.shutdown_cluster()

    def test_table_exists_with_keyspace_from_session(self):
        hook = CassandraHook("cassandra_default_with_schema")
        session = hook.get_conn()
        cqls = [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (pk1 text PRIMARY KEY)",
        ]
        for cql in cqls:
            session.execute(cql)

        assert hook.table_exists("t")
        assert not hook.table_exists("u")

        session.shutdown()
        hook.shutdown_cluster()
