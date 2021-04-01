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

import json
import unittest

import pytest

from airflow.models import Connection
from airflow.providers.dingding.hooks.dingding import DingdingHook
from airflow.utils import db


class TestDingdingHook(unittest.TestCase):
    conn_id = 'dingding_conn_id_test'

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id=self.conn_id,
                conn_type='http',
                host='https://oapi.dingtalk.com',
                password='you_token_here',
            )
        )

    def test_get_endpoint_conn_id(self):
        hook = DingdingHook(dingding_conn_id=self.conn_id)
        endpoint = hook._get_endpoint()
        assert 'robot/send?access_token=you_token_here' == endpoint

    def test_build_text_message_not_remind(self):
        config = {
            'dingding_conn_id': self.conn_id,
            'message_type': 'text',
            'message': 'Airflow dingding text message remind no one',
            'at_mobiles': False,
            'at_all': False,
        }
        expect = {
            'msgtype': 'text',
            'text': {'content': 'Airflow dingding text message remind no one'},
            'at': {'atMobiles': False, 'isAtAll': False},
        }
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_text_message_remind_specific(self):
        config = {
            'dingding_conn_id': self.conn_id,
            'message_type': 'text',
            'message': 'Airflow dingding text message remind specific users',
            'at_mobiles': ['1234', '5768'],
            'at_all': False,
        }
        expect = {
            'msgtype': 'text',
            'text': {'content': 'Airflow dingding text message remind specific users'},
            'at': {'atMobiles': ['1234', '5768'], 'isAtAll': False},
        }
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_text_message_remind_all(self):
        config = {
            'dingding_conn_id': self.conn_id,
            'message_type': 'text',
            'message': 'Airflow dingding text message remind all user in group',
            'at_all': True,
        }
        expect = {
            'msgtype': 'text',
            'text': {'content': 'Airflow dingding text message remind all user in group'},
            'at': {'atMobiles': None, 'isAtAll': True},
        }
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_markdown_message_remind_specific(self):
        msg = {
            'title': 'Airflow dingding markdown message',
            'text': '# Markdown message title\ncontent content .. \n### sub-title\n'
            '![logo](https://airflow.apache.org/_images/pin_large.png)',
        }
        config = {
            'dingding_conn_id': self.conn_id,
            'message_type': 'markdown',
            'message': msg,
            'at_mobiles': ['1234', '5678'],
            'at_all': False,
        }
        expect = {
            'msgtype': 'markdown',
            'markdown': msg,
            'at': {'atMobiles': ['1234', '5678'], 'isAtAll': False},
        }
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_markdown_message_remind_all(self):
        msg = {
            'title': 'Airflow dingding markdown message',
            'text': '# Markdown message title\ncontent content .. \n### sub-title\n'
            '![logo](https://airflow.apache.org/_images/pin_large.png)',
        }
        config = {
            'dingding_conn_id': self.conn_id,
            'message_type': 'markdown',
            'message': msg,
            'at_all': True,
        }
        expect = {'msgtype': 'markdown', 'markdown': msg, 'at': {'atMobiles': None, 'isAtAll': True}}
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_link_message(self):
        msg = {
            'title': 'Airflow dingding link message',
            'text': 'Airflow official documentation link',
            'messageUrl': 'https://airflow.apache.org',
            'picURL': 'https://airflow.apache.org/_images/pin_large.png',
        }
        config = {'dingding_conn_id': self.conn_id, 'message_type': 'link', 'message': msg}
        expect = {'msgtype': 'link', 'link': msg}
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_single_action_card_message(self):
        msg = {
            'title': 'Airflow dingding single actionCard message',
            'text': 'Airflow dingding single actionCard message\n'
            '![logo](https://airflow.apache.org/_images/pin_large.png)\n'
            'This is a official logo in Airflow website.',
            'hideAvatar': '0',
            'btnOrientation': '0',
            'singleTitle': 'read more',
            'singleURL': 'https://airflow.apache.org',
        }
        config = {'dingding_conn_id': self.conn_id, 'message_type': 'actionCard', 'message': msg}
        expect = {'msgtype': 'actionCard', 'actionCard': msg}
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_multi_action_card_message(self):
        msg = {
            'title': 'Airflow dingding multi actionCard message',
            'text': 'Airflow dingding multi actionCard message\n'
            '![logo](https://airflow.apache.org/_images/pin_large.png)\n'
            'Airflow documentation and GitHub',
            'hideAvatar': '0',
            'btnOrientation': '0',
            'btns': [
                {'title': 'Airflow Documentation', 'actionURL': 'https://airflow.apache.org'},
                {'title': 'Airflow GitHub', 'actionURL': 'https://github.com/apache/airflow'},
            ],
        }
        config = {'dingding_conn_id': self.conn_id, 'message_type': 'actionCard', 'message': msg}
        expect = {'msgtype': 'actionCard', 'actionCard': msg}
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_build_feed_card_message(self):
        msg = {
            "links": [
                {
                    "title": "Airflow DAG feed card",
                    "messageURL": "https://airflow.apache.org/docs/apache-airflow/stable/ui.html",
                    "picURL": "https://airflow.apache.org/_images/dags.png",
                },
                {
                    "title": "Airflow tree feed card",
                    "messageURL": "https://airflow.apache.org/docs/apache-airflow/stable/ui.html",
                    "picURL": "https://airflow.apache.org/_images/tree.png",
                },
                {
                    "title": "Airflow graph feed card",
                    "messageURL": "https://airflow.apache.org/docs/apache-airflow/stable/ui.html",
                    "picURL": "https://airflow.apache.org/_images/graph.png",
                },
            ]
        }
        config = {'dingding_conn_id': self.conn_id, 'message_type': 'feedCard', 'message': msg}
        expect = {'msgtype': 'feedCard', 'feedCard': msg}
        hook = DingdingHook(**config)
        message = hook._build_message()
        assert json.dumps(expect) == message

    def test_send_not_support_type(self):
        config = {
            'dingding_conn_id': self.conn_id,
            'message_type': 'not_support_type',
            'message': 'Airflow dingding text message remind no one',
        }
        hook = DingdingHook(**config)
        with pytest.raises(ValueError):
            hook.send()
