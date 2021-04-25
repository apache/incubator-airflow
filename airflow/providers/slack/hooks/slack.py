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
"""Hook for Slack"""
from typing import Any, Optional

from slack_sdk import WebClient

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class SlackHook(BaseHook):  # noqa
    """
    Creates a Slack connection, to be used for calls. Takes both Slack API token directly and
    connection that has Slack API token. If both supplied, Slack API token will be used.
    Exposes also the rest of slack.WebClient args
    Examples:

    .. code-block:: python

        # Create hook
        slack_hook = SlackHook(token="xxx")  # or slack_hook = SlackHook(slack_conn_id="slack")

        # Call generic API with parameters (errors are handled by hook)
        #  For more details check https://api.slack.com/methods/chat.postMessage
        slack_hook.call("chat.postMessage", json={"channel": "#random", "text": "Hello world!"})

        # Call method from Slack SDK (you have to handle errors yourself)
        #  For more details check https://slack.dev/python-slack-sdk/web/index.html#messaging
        slack_hook.client.chat_postMessage(channel="#random", text="Hello world!")

    :param token: Slack API token
    :type token: str
    :param slack_conn_id: connection that has Slack API token in the password field
    :type slack_conn_id: str
    :param use_session: A boolean specifying if the client should take advantage of
        connection pooling. Default is True.
    :type use_session: bool
    :param base_url: A string representing the Slack API base URL. Default is
        ``https://www.slack.com/api/``
    :type base_url: str
    :param timeout: The maximum number of seconds the client will wait
        to connect and receive a response from Slack. Default is 30 seconds.
    :type timeout: int
    """

    def __init__(
        self,
        token: Optional[str] = None,
        slack_conn_id: Optional[str] = None,
        **client_args: Any,
    ) -> None:
        super().__init__()
        self.token = self.__get_token(token, slack_conn_id)
        self.client = WebClient(self.token, **client_args)

    def __get_token(self, token: Any, slack_conn_id: Any) -> str:
        if token is not None:
            return token

        if slack_conn_id is not None:
            conn = self.get_connection(slack_conn_id)

            if not getattr(conn, 'password', None):
                raise AirflowException('Missing token(password) in Slack connection')
            return conn.password

        raise AirflowException('Cannot get token: No valid Slack token nor slack_conn_id supplied.')

    def call(self, api_method: str, **kwargs) -> None:
        """
        Calls Slack WebClient `WebClient.api_call` with given arguments.

        :param api_method: The target Slack API method. e.g. 'chat.postMessage'. Required.
        :type api_method: str
        :param http_verb: HTTP Verb. Optional (defaults to 'POST')
        :type http_verb: str
        :param files: Files to multipart upload. e.g. {imageORfile: file_objectORfile_path}
        :type files: dict
        :param data: The body to attach to the request. If a dictionary is provided,
            form-encoding will take place. Optional.
        :type data: dict or aiohttp.FormData
        :param params: The URL parameters to append to the URL. Optional.
        :type params: dict
        :param json: JSON for the body to attach to the request. Optional.
        :type json: dict
        """
        self.client.api_call(api_method, **kwargs)
