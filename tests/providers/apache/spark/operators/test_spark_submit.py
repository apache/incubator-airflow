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
#

import unittest
from datetime import timedelta

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSparkSubmitOperator(unittest.TestCase):

    _config = {
        'conf': {'parquet.compression': 'SNAPPY'},
        'files': 'hive-site.xml',
        'py_files': 'sample_library.py',
        'archives': 'sample_archive.zip#SAMPLE',
        'driver_class_path': 'parquet.jar',
        'jars': 'parquet.jar',
        'packages': 'com.databricks:spark-avro_2.11:3.2.0',
        'exclude_packages': 'org.bad.dependency:1.0.0',
        'repositories': 'http://myrepo.org',
        'total_executor_cores': 4,
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'principal': 'user/spark@airflow.org',
        'proxy_user': 'sample_user',
        'name': '{{ task_instance.task_id }}',
        'num_executors': 10,
        'status_poll_interval': 30,
        'verbose': True,
        'application': 'test_application.py',
        'driver_memory': '3g',
        'java_class': 'com.foo.bar.AppMain',
        'application_args': [
            '-f',
            'foo',
            '--bar',
            'bar',
            '--start',
            '{{ macros.ds_add(ds, -1)}}',
            '--end',
            '{{ ds }}',
            '--with-spaces',
            'args should keep embedded spaces',
        ],
    }

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self):

        # Given / When
        conn_id = 'spark_default'
        operator = SparkSubmitOperator(
            task_id='spark_submit_job', spark_binary="sparky", dag=self.dag, **self._config
        )

        # Then expected results
        expected_dict = {
            'conf': {'parquet.compression': 'SNAPPY'},
            'files': 'hive-site.xml',
            'py_files': 'sample_library.py',
            'archives': 'sample_archive.zip#SAMPLE',
            'driver_class_path': 'parquet.jar',
            'jars': 'parquet.jar',
            'packages': 'com.databricks:spark-avro_2.11:3.2.0',
            'exclude_packages': 'org.bad.dependency:1.0.0',
            'repositories': 'http://myrepo.org',
            'total_executor_cores': 4,
            'executor_cores': 4,
            'executor_memory': '22g',
            'keytab': 'privileged_user.keytab',
            'principal': 'user/spark@airflow.org',
            'proxy_user': 'sample_user',
            'name': '{{ task_instance.task_id }}',
            'num_executors': 10,
            'status_poll_interval': 30,
            'verbose': True,
            'application': 'test_application.py',
            'driver_memory': '3g',
            'java_class': 'com.foo.bar.AppMain',
            'application_args': [
                '-f',
                'foo',
                '--bar',
                'bar',
                '--start',
                '{{ macros.ds_add(ds, -1)}}',
                '--end',
                '{{ ds }}',
                '--with-spaces',
                'args should keep embedded spaces',
            ],
            'spark_binary': 'sparky',
        }

        assert conn_id == operator._conn_id
        assert expected_dict['application'] == operator._application
        assert expected_dict['conf'] == operator._conf
        assert expected_dict['files'] == operator._files
        assert expected_dict['py_files'] == operator._py_files
        assert expected_dict['archives'] == operator._archives
        assert expected_dict['driver_class_path'] == operator._driver_class_path
        assert expected_dict['jars'] == operator._jars
        assert expected_dict['packages'] == operator._packages
        assert expected_dict['exclude_packages'] == operator._exclude_packages
        assert expected_dict['repositories'] == operator._repositories
        assert expected_dict['total_executor_cores'] == operator._total_executor_cores
        assert expected_dict['executor_cores'] == operator._executor_cores
        assert expected_dict['executor_memory'] == operator._executor_memory
        assert expected_dict['keytab'] == operator._keytab
        assert expected_dict['principal'] == operator._principal
        assert expected_dict['proxy_user'] == operator._proxy_user
        assert expected_dict['name'] == operator._name
        assert expected_dict['num_executors'] == operator._num_executors
        assert expected_dict['status_poll_interval'] == operator._status_poll_interval
        assert expected_dict['verbose'] == operator._verbose
        assert expected_dict['java_class'] == operator._java_class
        assert expected_dict['driver_memory'] == operator._driver_memory
        assert expected_dict['application_args'] == operator._application_args
        assert expected_dict['spark_binary'] == operator._spark_binary

    def test_render_template(self):
        # Given
        operator = SparkSubmitOperator(task_id='spark_submit_job', dag=self.dag, **self._config)
        ti = TaskInstance(operator, DEFAULT_DATE)

        # When
        ti.render_templates()

        # Then
        expected_application_args = [
            '-f',
            'foo',
            '--bar',
            'bar',
            '--start',
            (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
            '--end',
            DEFAULT_DATE.strftime("%Y-%m-%d"),
            '--with-spaces',
            'args should keep embedded spaces',
        ]
        expected_name = 'spark_submit_job'
        assert expected_application_args == getattr(operator, '_application_args')
        assert expected_name == getattr(operator, '_name')
