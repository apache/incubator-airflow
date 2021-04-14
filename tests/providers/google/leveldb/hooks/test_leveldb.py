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

from airflow.providers.google.leveldb.hooks.leveldb import LevelDBHook, LevelDBHookException


class TestLevelDBHook(unittest.TestCase):
    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_get_conn_db_is_not_none(self):
        """Test get_conn method of hook"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        assert hook.db is not None, "Check existence of DB object in connection creation"
        hook.close_conn()

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_run(self):
        """Test run method of hook"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        assert hook.run('get', b'test_key0') is None, "Initially, this key in LevelDB is empty"
        hook.run('put', b'test_key0', b'test_value0')
        assert (
            hook.run('get', b'test_key0') == b'test_value0'
        ), 'Connection to LevelDB with PUT and GET works.'
        hook.run('delete', b'test_key0')
        assert hook.run('get', b'test_key0') is None, 'Connection to LevelDB with DELETE works.'
        hook.close_conn()

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_get(self):
        """Test get method of hook"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        db = hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        db.put(b'test_key', b'test_value')
        assert hook.get(b'test_key') == b'test_value'
        hook.close_conn()

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_put(self):
        """Test put method of hook"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        db = hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        hook.put(b'test_key2', b'test_value2')
        assert db.get(b'test_key2') == b'test_value2'
        hook.close_conn()

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_delete(self):
        """Test delete method of hook"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        db = hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        db.put(b'test_key3', b'test_value3')
        hook.delete(b'test_key3')
        assert db.get(b'test_key3') is None
        hook.close_conn()

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_write_batch(self):
        """Test write batch method of hook"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        db = hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        keys = [b'key', b'another-key']
        values = [b'value', b'another-value']
        hook.write_batch(keys, values)
        assert db.get(b'key') == b'value'
        assert db.get(b'another-key') == b'another-value'
        hook.close_conn()

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_exception(self):
        """Test raising exception of hook in run method if we have unknown command in input"""
        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        hook.get_conn(name='/tmp/testdb/', create_if_missing=True)
        with pytest.raises(LevelDBHookException):
            hook.run(command='other_command', key=b'key', value=b'value')

    @mock.patch.dict('os.environ', AIRFLOW_CONN_LEVELDB_DEFAULT="test")
    def test_comparator(self):
        """Test comparator"""

        def comparator(a, b):
            a = a.lower()
            b = b.lower()

            if a < b:
                # a sorts before b
                return -1

            if a > b:
                # a sorts after b
                return 1

            # a and b are equal
            return 0

        hook = LevelDBHook(leveldb_conn_id='leveldb_default')
        hook.get_conn(
            name='/tmp/testdb2/',
            create_if_missing=True,
            comparator=comparator,
            comparator_name=b'CaseInsensitiveComparator',
        )
        assert hook.db is not None, "Check existence of DB object(with comparator) in connection creation"
        hook.close_conn()
