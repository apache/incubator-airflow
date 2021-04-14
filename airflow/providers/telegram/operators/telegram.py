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
"""Operator for Telegram"""
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.utils.decorators import apply_defaults


class TelegramOperator(BaseOperator):
    """
    This operator allows you to post messages to Telegram using Telegram Bot API.
    Takes both Telegram Bot API token directly or connection that has Telegram token in password field.
    If both supplied, token parameter will be given precedence.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TelegramOperator`

    :param telegram_conn_id: Telegram connection ID which its password is Telegram API token
    :type telegram_conn_id: str
    :param token: Telegram API Token
    :type token: str
    :param chat_id: Telegram chat ID for a chat/channel/group
    :type chat_id: str
    :param text: Message to be sent on telegram
    :type text: str
    :param telegram_kwargs: Extra args to be passed to telegram client
    :type telegram_kwargs: dict
    """

    template_fields = ('text', 'chat_id')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(
        self,
        *,
        telegram_conn_id: str = "telegram_default",
        token: Optional[str] = None,
        chat_id: Optional[str] = None,
        text: str = "No message has been set.",
        telegram_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        self.chat_id = chat_id
        self.token = token
        self.telegram_kwargs = telegram_kwargs or {}
        self.text = text

        if telegram_conn_id is None:
            raise AirflowException("No valid Telegram connection id supplied.")

        self.telegram_conn_id = telegram_conn_id

        super().__init__(**kwargs)

    def execute(self, **kwargs) -> None:
        """Calls the TelegramHook to post the provided Telegram message"""
        if self.text:
            self.telegram_kwargs['text'] = self.text

        telegram_hook = TelegramHook(
            telegram_conn_id=self.telegram_conn_id,
            token=self.token,
            chat_id=self.chat_id,
        )
        telegram_hook.send_message(self.telegram_kwargs)
