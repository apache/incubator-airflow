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
import telegram

import airflow
from airflow.models import Connection
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils import db

TELEGRAM_TOKEN = "xxx:xxx"


class TestTelegramOperator(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='telegram_default',
                conn_type='http',
                password=TELEGRAM_TOKEN,
            )
        )
        db.merge_conn(
            Connection(
                conn_id='telegram_default-with-chat-id',
                conn_type='http',
                password=TELEGRAM_TOKEN,
                host="-420913222",
            )
        )

    @mock.patch('airflow.providers.telegram.operators.telegram.TelegramHook')
    def test_should_send_message_when_all_parameters_are_provided(self, mock_telegram_hook):
        mock_telegram_hook.return_value = mock.Mock()
        mock_telegram_hook.return_value.send_message.return_value = True

        hook = TelegramOperator(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            task_id='telegram',
            text="some non empty text",
        )
        hook.execute()

        mock_telegram_hook.assert_called_once_with(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            token=None,
        )
        mock_telegram_hook.return_value.send_message.assert_called_once_with(
            {'text': 'some non empty text'},
        )

    def test_should_throw_exception_if_connection_id_is_none(self):
        with pytest.raises(airflow.exceptions.AirflowException) as ctx:
            TelegramOperator(task_id="telegram", telegram_conn_id=None)

        assert "No valid Telegram connection id supplied." == str(ctx.value)

    @mock.patch('airflow.providers.telegram.operators.telegram.TelegramHook')
    def test_should_throw_exception_if_telegram_hook_throws_any_exception(self, mock_telegram_hook):
        def side_effect(*args, **kwargs):
            raise telegram.error.TelegramError("cosmic rays caused bit flips")

        mock_telegram_hook.return_value = mock.Mock()
        mock_telegram_hook.return_value.send_message.side_effect = side_effect

        with pytest.raises(telegram.error.TelegramError) as ctx:
            hook = TelegramOperator(
                telegram_conn_id='telegram_default',
                task_id='telegram',
                text="some non empty text",
            )
            hook.execute()

        assert "cosmic rays caused bit flips" == str(ctx.value)

    @mock.patch('airflow.providers.telegram.operators.telegram.TelegramHook')
    def test_should_forward_all_args_to_telegram(self, mock_telegram_hook):
        mock_telegram_hook.return_value = mock.Mock()
        mock_telegram_hook.return_value.send_message.return_value = True

        hook = TelegramOperator(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            task_id='telegram',
            text="some non empty text",
            telegram_kwargs={"custom_arg": "value"},
        )
        hook.execute()

        mock_telegram_hook.assert_called_once_with(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            token=None,
        )
        mock_telegram_hook.return_value.send_message.assert_called_once_with(
            {'custom_arg': 'value', 'text': 'some non empty text'},
        )

    @mock.patch('airflow.providers.telegram.operators.telegram.TelegramHook')
    def test_should_give_precedence_to_text_passed_in_constructor(self, mock_telegram_hook):
        mock_telegram_hook.return_value = mock.Mock()
        mock_telegram_hook.return_value.send_message.return_value = True

        hook = TelegramOperator(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            task_id='telegram',
            text="some non empty text - higher precedence",
            telegram_kwargs={"custom_arg": "value", "text": "some text, that will be ignored"},
        )
        hook.execute()

        mock_telegram_hook.assert_called_once_with(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            token=None,
        )
        mock_telegram_hook.return_value.send_message.assert_called_once_with(
            {'custom_arg': 'value', 'text': 'some non empty text - higher precedence'},
        )

    def test_should_return_template_fields(self):
        hook = TelegramOperator(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            task_id='telegram',
            text="some non empty text - higher precedence",
            telegram_kwargs={"custom_arg": "value", "text": "some text, that will be ignored"},
        )
        assert ('text', 'chat_id') == hook.template_fields

    @mock.patch('airflow.providers.telegram.operators.telegram.TelegramHook')
    def test_should_return_templatized_text_field(self, mock_hook):
        operator = TelegramOperator(
            telegram_conn_id='telegram_default',
            chat_id='-420913222',
            task_id='telegram',
            text="execution date is {{ ds }}",
            telegram_kwargs={"custom_arg": "value", "text": "should be ignored"},
        )
        operator.render_template_fields({"ds": "2021-02-04"})
        operator.execute()
        assert operator.text == "execution date is 2021-02-04"
        assert 'text' in operator.telegram_kwargs
        assert operator.telegram_kwargs['text'] == "execution date is 2021-02-04"

    def test_should_return_templatized_chat_id_field(self):
        operator = TelegramOperator(
            telegram_conn_id='telegram_default',
            chat_id='{{ chat_id }}',
            task_id='telegram',
            text="text",
            telegram_kwargs={"custom_arg": "value", "text": "should be ignored"},
        )
        operator.render_template_fields({"chat_id": "1234567"})
        assert operator.chat_id == "1234567"
