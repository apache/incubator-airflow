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

import logging
import os
import shutil
import unittest
from unittest import mock
from urllib.parse import quote

import elasticsearch
import pendulum
from parameterized import parameterized

from airflow.configuration import conf
from airflow.models import DAG, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.timezone import datetime

from .elasticmock import elasticmock


class TestElasticsearchTaskHandler(unittest.TestCase):
    DAG_ID = 'dag_for_testing_file_task_handler'
    TASK_ID = 'task_for_testing_file_log_handler'
    EXECUTION_DATE = datetime(2016, 1, 1)
    LOG_ID = f'{DAG_ID}-{TASK_ID}-2016-01-01T00:00:00+00:00-1'

    @elasticmock
    def setUp(self):
        super().setUp()
        self.local_log_location = 'local/log/location'
        self.filename_template = '{try_number}.log'
        self.log_id_template = '{dag_id}-{task_id}-{execution_date}-{try_number}'
        self.end_of_log_mark = 'end_of_log\n'
        self.write_stdout = False
        self.json_format = False
        self.json_fields = 'asctime,filename,lineno,levelname,message'
        self.es_task_handler = ElasticsearchTaskHandler(
            self.local_log_location,
            self.filename_template,
            self.log_id_template,
            self.end_of_log_mark,
            self.write_stdout,
            self.json_format,
            self.json_fields,
        )

        self.es = elasticsearch.Elasticsearch(  # pylint: disable=invalid-name
            hosts=[{'host': 'localhost', 'port': 9200}]
        )
        self.index_name = 'test_index'
        self.doc_type = 'log'
        self.test_message = 'some random stuff'
        self.body = {'message': self.test_message, 'log_id': self.LOG_ID, 'offset': 1}

        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=1)

        self.dag = DAG(self.DAG_ID, start_date=self.EXECUTION_DATE)
        task = DummyOperator(task_id=self.TASK_ID, dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=self.EXECUTION_DATE)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)

    def tearDown(self):
        shutil.rmtree(self.local_log_location.split(os.path.sep)[0], ignore_errors=True)

    def test_client(self):
        assert isinstance(self.es_task_handler.client, elasticsearch.Elasticsearch)

    def test_client_with_config(self):
        es_conf = dict(conf.getsection("elasticsearch_configs"))
        expected_dict = {
            "use_ssl": False,
            "verify_certs": True,
        }
        assert es_conf == expected_dict
        # ensure creating with configs does not fail
        ElasticsearchTaskHandler(
            self.local_log_location,
            self.filename_template,
            self.log_id_template,
            self.end_of_log_mark,
            self.write_stdout,
            self.json_format,
            self.json_fields,
            es_conf,
        )

    def test_read(self):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            self.ti, 1, {'offset': 0, 'last_log_timestamp': str(ts), 'end_of_log': False}
        )

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]['end_of_log']
        assert '1' == metadatas[0]['offset']
        assert timezone.parse(metadatas[0]['last_log_timestamp']) > ts

    def test_read_with_match_phrase_query(self):
        similar_log_id = '{task_id}-{dag_id}-2016-01-01T00:00:00+00:00-1'.format(
            dag_id=TestElasticsearchTaskHandler.DAG_ID, task_id=TestElasticsearchTaskHandler.TASK_ID
        )
        another_test_message = 'another message'

        another_body = {'message': another_test_message, 'log_id': similar_log_id, 'offset': 1}
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=another_body, id=1)

        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            self.ti, 1, {'offset': '0', 'last_log_timestamp': str(ts), 'end_of_log': False, 'max_offset': 2}
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert self.test_message == logs[0][0][-1]
        assert another_test_message != logs[0]

        assert not metadatas[0]['end_of_log']
        assert '1' == metadatas[0]['offset']
        assert timezone.parse(metadatas[0]['last_log_timestamp']) > ts

    def test_read_with_none_metadata(self):
        logs, metadatas = self.es_task_handler.read(self.ti, 1)
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]['end_of_log']
        assert '1' == metadatas[0]['offset']
        assert timezone.parse(metadatas[0]['last_log_timestamp']) < pendulum.now()

    def test_read_nonexistent_log(self):
        ts = pendulum.now()
        # In ElasticMock, search is going to return all documents with matching index
        # and doc_type regardless of match filters, so we delete the log entry instead
        # of making a new TaskInstance to query.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(
            self.ti, 1, {'offset': 0, 'last_log_timestamp': str(ts), 'end_of_log': False}
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]['end_of_log']
        assert '0' == metadatas[0]['offset']
        # last_log_timestamp won't change if no log lines read.
        assert timezone.parse(metadatas[0]['last_log_timestamp']) == ts

    def test_read_with_empty_metadata(self):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(self.ti, 1, {})
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]['end_of_log']
        # offset should be initialized to 0 if not provided.
        assert '1' == metadatas[0]['offset']
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        assert timezone.parse(metadatas[0]['last_log_timestamp']) > ts

        # case where offset is missing but metadata not empty.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(self.ti, 1, {'end_of_log': False})
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]['end_of_log']
        # offset should be initialized to 0 if not provided.
        assert '0' == metadatas[0]['offset']
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        assert timezone.parse(metadatas[0]['last_log_timestamp']) > ts

    def test_read_timeout(self):
        ts = pendulum.now().subtract(minutes=5)

        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(
            self.ti, 1, {'offset': 0, 'last_log_timestamp': str(ts), 'end_of_log': False}
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert metadatas[0]['end_of_log']
        # offset should be initialized to 0 if not provided.
        assert '0' == metadatas[0]['offset']
        assert timezone.parse(metadatas[0]['last_log_timestamp']) == ts

    def test_read_as_download_logs(self):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            self.ti,
            1,
            {'offset': 0, 'last_log_timestamp': str(ts), 'download_logs': True, 'end_of_log': False},
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]['end_of_log']
        assert metadatas[0]['download_logs']
        assert '1' == metadatas[0]['offset']
        assert timezone.parse(metadatas[0]['last_log_timestamp']) > ts

    def test_read_raises(self):
        with mock.patch.object(self.es_task_handler.log, 'exception') as mock_exception:
            with mock.patch("elasticsearch_dsl.Search.execute") as mock_execute:
                mock_execute.side_effect = Exception('Failed to read')
                logs, metadatas = self.es_task_handler.read(self.ti, 1)
            assert mock_exception.call_count == 1
            args, kwargs = mock_exception.call_args
            assert "Could not read log with log_id:" in args[0]

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]['end_of_log']
        assert '0' == metadatas[0]['offset']

    def test_set_context(self):
        self.es_task_handler.set_context(self.ti)
        assert self.es_task_handler.mark_end_on_close

    def test_set_context_w_json_format_and_write_stdout(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.es_task_handler.formatter = formatter
        self.es_task_handler.write_stdout = True
        self.es_task_handler.json_format = True
        self.es_task_handler.set_context(self.ti)

    def test_read_with_json_format(self):
        ts = pendulum.now()
        formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
        self.es_task_handler.formatter = formatter
        self.es_task_handler.json_format = True

        self.body = {
            'message': self.test_message,
            'log_id': f'{self.DAG_ID}-{self.TASK_ID}-2016_01_01T00_00_00_000000-1',
            'offset': 1,
            'asctime': '2020-12-24 19:25:00,962',
            'filename': 'taskinstance.py',
            'lineno': 851,
            'levelname': 'INFO',
        }
        self.es_task_handler.set_context(self.ti)
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=id)

        logs, _ = self.es_task_handler.read(
            self.ti, 1, {'offset': 0, 'last_log_timestamp': str(ts), 'end_of_log': False}
        )
        assert "[2020-12-24 19:25:00,962] {taskinstance.py:851} INFO - some random stuff" == logs[0][0][1]

    def test_close(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.es_task_handler.formatter = formatter

        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.filename_template.format(try_number=1))
        ) as log_file:
            # end_of_log_mark may contain characters like '\n' which is needed to
            # have the log uploaded but will not be stored in elasticsearch.
            # so apply the strip() to log_file.read()
            log_line = log_file.read().strip()
            assert self.end_of_log_mark.strip() == log_line
        assert self.es_task_handler.closed

    def test_close_no_mark_end(self):
        self.ti.raw = True
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.filename_template.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark not in log_file.read()
        assert self.es_task_handler.closed

    def test_close_closed(self):
        self.es_task_handler.closed = True
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.filename_template.format(try_number=1))
        ) as log_file:
            assert 0 == len(log_file.read())

    def test_close_with_no_handler(self):
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.handler = None
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.filename_template.format(try_number=1))
        ) as log_file:
            assert 0 == len(log_file.read())
        assert self.es_task_handler.closed

    def test_close_with_no_stream(self):
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.handler.stream = None
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.filename_template.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark in log_file.read()
        assert self.es_task_handler.closed

        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.handler.stream.close()
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.filename_template.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark in log_file.read()
        assert self.es_task_handler.closed

    def test_render_log_id(self):
        expected_log_id = (
            'dag_for_testing_file_task_handler-'
            'task_for_testing_file_log_handler-2016-01-01T00:00:00+00:00-1'
        )
        log_id = self.es_task_handler._render_log_id(self.ti, 1)
        assert expected_log_id == log_id

        # Switch to use jinja template.
        self.es_task_handler = ElasticsearchTaskHandler(
            self.local_log_location,
            self.filename_template,
            '{{ ti.dag_id }}-{{ ti.task_id }}-{{ ts }}-{{ try_number }}',
            self.end_of_log_mark,
            self.write_stdout,
            self.json_format,
            self.json_fields,
        )
        log_id = self.es_task_handler._render_log_id(self.ti, 1)
        assert expected_log_id == log_id

    def test_clean_execution_date(self):
        clean_execution_date = self.es_task_handler._clean_execution_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert '2016_07_08T09_10_11_000012' == clean_execution_date

    @parameterized.expand(
        [
            # Common case
            ('localhost:5601/{log_id}', 'https://localhost:5601/' + quote(LOG_ID.replace('T', ' '))),
            # Ignore template if "{log_id}"" is missing in the URL
            ('localhost:5601', 'https://localhost:5601'),
        ]
    )
    def test_get_external_log_url(self, es_frontend, expected_url):
        es_task_handler = ElasticsearchTaskHandler(
            self.local_log_location,
            self.filename_template,
            self.log_id_template,
            self.end_of_log_mark,
            self.write_stdout,
            self.json_format,
            self.json_fields,
            frontend=es_frontend,
        )
        url = es_task_handler.get_external_log_url(self.ti, self.ti.try_number)
        assert expected_url == url
