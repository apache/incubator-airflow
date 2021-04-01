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
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.utils.decorators import apply_defaults


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well.
    Only one of `slack_conn_id` and `token` is required.

    :param slack_conn_id: Slack connection ID which its password is Slack API token. Optional
    :type slack_conn_id: str
    :param token: Slack API token (https://api.slack.com/web). Optional
    :type token: str
    :param method: The Slack API Method to Call (https://api.slack.com/methods). Optional
    :type method: str
    :param api_params: API Method call parameters (https://api.slack.com/methods). Optional
    :type api_params: dict
    :param client_args: Slack Hook parameters. Optional. Check airflow.providers.slack.hooks.SlackHook
    :type client_args: dict
    """

    @apply_defaults
    def __init__(
        self,
        *,
        slack_conn_id: Optional[str] = None,
        token: Optional[str] = None,
        method: Optional[str] = None,
        api_params: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.token = token  # type: Optional[str]
        self.slack_conn_id = slack_conn_id  # type: Optional[str]

        self.method = method
        self.api_params = api_params

    def construct_api_call_params(self) -> Any:
        """
        Used by the execute function. Allows templating on the source fields
        of the api_call_params dict before construction

        Override in child classes.
        Each SlackAPIOperator child class is responsible for
        having a construct_api_call_params function
        which sets self.api_call_params with a dict of
        API call parameters (https://api.slack.com/methods)
        """
        raise NotImplementedError(
            "SlackAPIOperator should not be used directly. Chose one of the subclasses instead"
        )

    def execute(self, **kwargs):  # noqa: D403
        """
        SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not self.api_params:
            self.construct_api_call_params()
        slack = SlackHook(token=self.token, slack_conn_id=self.slack_conn_id)
        slack.call(self.method, json=self.api_params)


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel
    Examples:

    .. code-block:: python

        slack = SlackAPIPostOperator(
            task_id="post_hello",
            dag=dag,
            token="XXX",
            text="hello there!",
            channel="#random",
        )

    :param channel: channel in which to post message on slack name (#general) or
        ID (C12318391). (templated)
    :type channel: str
    :param username: Username that airflow will be posting to Slack as. (templated)
    :type username: str
    :param text: message to send to slack. (templated)
    :type text: str
    :param icon_url: url to icon used for this message
    :type icon_url: str
    :param attachments: extra formatting details. (templated)
        - see https://api.slack.com/docs/attachments.
    :type attachments: list of hashes
    :param blocks: extra block layouts. (templated)
        - see https://api.slack.com/reference/block-kit/blocks.
    :type blocks: list of hashes
    """

    template_fields = ('username', 'text', 'attachments', 'blocks', 'channel')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(
        self,
        channel: str = '#general',
        username: str = 'Airflow',
        text: str = 'No message has been set.\n'
        'Here is a cat video instead\n'
        'https://www.youtube.com/watch?v=J---aiyznGQ',
        icon_url: str = 'https://raw.githubusercontent.com/apache/'
        'airflow/master/airflow/www/static/pin_100.png',
        attachments: Optional[List] = None,
        blocks: Optional[List] = None,
        **kwargs,
    ) -> None:
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments or []
        self.blocks = blocks or []
        super().__init__(method=self.method, **kwargs)

    def construct_api_call_params(self) -> Any:
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
            'blocks': json.dumps(self.blocks),
        }


class SlackAPIFileOperator(SlackAPIOperator):
    """
    Send a file to a slack channel
    Examples:

    .. code-block:: python

        slack = SlackAPIFileOperator(
            task_id="slack_file_upload",
            dag=dag,
            slack_conn_id="slack",
            channel="#general",
            initial_comment="Hello World!",
            filename="hello_world.csv",
            filetype="csv",
            content="hello,world,csv,file",
        )

    :param channel: channel in which to sent file on slack name (templated)
    :type channel: str
    :param initial_comment: message to send to slack. (templated)
    :type initial_comment: str
    :param filename: name of the file (templated)
    :type filename: str
    :param filetype: slack filetype. (templated)
        - see https://api.slack.com/types/file
    :type filetype: str
    :param content: file content. (templated)
    :type content: str
    """

    template_fields = ('channel', 'initial_comment', 'filename', 'filetype', 'content')
    ui_color = '#44BEDF'

    @apply_defaults
    def __init__(
        self,
        channel: str = '#general',
        initial_comment: str = 'No message has been set!',
        filename: str = 'default_name.csv',
        filetype: str = 'csv',
        content: str = 'default,content,csv,file',
        **kwargs,
    ) -> None:
        self.method = 'files.upload'
        self.channel = channel
        self.initial_comment = initial_comment
        self.filename = filename
        self.filetype = filetype
        self.content = content
        super().__init__(method=self.method, **kwargs)

    def construct_api_call_params(self) -> Any:
        self.api_params = {
            'channels': self.channel,
            'content': self.content,
            'filename': self.filename,
            'filetype': self.filetype,
            'initial_comment': self.initial_comment,
        }
