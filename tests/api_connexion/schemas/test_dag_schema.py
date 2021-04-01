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
from datetime import datetime

from itsdangerous import URLSafeSerializer

from airflow import DAG
from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection,
    DAGCollectionSchema,
    DAGDetailSchema,
    DAGSchema,
)
from airflow.configuration import conf
from airflow.models import DagModel, DagTag

SERIALIZER = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))


class TestDagSchema(unittest.TestCase):
    def test_serialize(self):
        dag_model = DagModel(
            dag_id="test_dag_id",
            root_dag_id="test_root_dag_id",
            is_paused=True,
            is_subdag=False,
            fileloc="/root/airflow/dags/my_dag.py",
            owners="airflow1,airflow2",
            description="The description",
            schedule_interval="5 4 * * *",
            tags=[DagTag(name="tag-1"), DagTag(name="tag-2")],
        )
        serialized_dag = DAGSchema().dump(dag_model)
        assert {
            "dag_id": "test_dag_id",
            "description": "The description",
            "fileloc": "/root/airflow/dags/my_dag.py",
            "file_token": SERIALIZER.dumps("/root/airflow/dags/my_dag.py"),
            "is_paused": True,
            "is_subdag": False,
            "owners": ["airflow1", "airflow2"],
            "root_dag_id": "test_root_dag_id",
            "schedule_interval": {"__type": "CronExpression", "value": "5 4 * * *"},
            "tags": [{"name": "tag-1"}, {"name": "tag-2"}],
        } == serialized_dag


class TestDAGCollectionSchema(unittest.TestCase):
    def test_serialize(self):
        dag_model_a = DagModel(dag_id="test_dag_id_a", fileloc="/tmp/a.py")
        dag_model_b = DagModel(dag_id="test_dag_id_b", fileloc="/tmp/a.py")
        schema = DAGCollectionSchema()
        instance = DAGCollection(dags=[dag_model_a, dag_model_b], total_entries=2)
        assert {
            "dags": [
                {
                    "dag_id": "test_dag_id_a",
                    "description": None,
                    "fileloc": "/tmp/a.py",
                    "file_token": SERIALIZER.dumps("/tmp/a.py"),
                    "is_paused": None,
                    "is_subdag": None,
                    "owners": [],
                    "root_dag_id": None,
                    "schedule_interval": None,
                    "tags": [],
                },
                {
                    "dag_id": "test_dag_id_b",
                    "description": None,
                    "fileloc": "/tmp/a.py",
                    "file_token": SERIALIZER.dumps("/tmp/a.py"),
                    "is_paused": None,
                    "is_subdag": None,
                    "owners": [],
                    "root_dag_id": None,
                    "schedule_interval": None,
                    "tags": [],
                },
            ],
            "total_entries": 2,
        } == schema.dump(instance)


class TestDAGDetailSchema:
    def test_serialize(self):
        dag = DAG(
            dag_id="test_dag",
            start_date=datetime(2020, 6, 19),
            doc_md="docs",
            orientation="LR",
            default_view="duration",
            params={"foo": 1},
            tags=['example1', 'example2'],
        )
        schema = DAGDetailSchema()
        expected = {
            'catchup': True,
            'concurrency': 16,
            'dag_id': 'test_dag',
            'dag_run_timeout': None,
            'default_view': 'duration',
            'description': None,
            'doc_md': 'docs',
            'fileloc': __file__,
            "file_token": SERIALIZER.dumps(__file__),
            'is_paused': None,
            'is_subdag': False,
            'orientation': 'LR',
            'owners': [],
            'params': {'foo': 1},
            'schedule_interval': {'__type': 'TimeDelta', 'days': 1, 'seconds': 0, 'microseconds': 0},
            'start_date': '2020-06-19T00:00:00+00:00',
            'tags': [{'name': "example1"}, {'name': "example2"}],
            'timezone': "Timezone('UTC')",
        }
        assert schema.dump(dag) == expected
