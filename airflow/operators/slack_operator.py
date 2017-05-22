# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from slackclient import SlackClient
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.slack_webhook_hook import SlackWebHookHook
import json
import logging



class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well

    :param token: Slack API token (https://api.slack.com/web)
    :type token: string
    :param method: The Slack API Method to Call (https://api.slack.com/methods)
    :type method: string
    :param api_params: API Method call parameters (https://api.slack.com/methods)
    :type api_params: dict
    """

    @apply_defaults
    def __init__(self,
                 token='unset',
                 method='unset',
                 api_params=None,
                 *args, **kwargs):
        super(SlackAPIOperator, self).__init__(*args, **kwargs)
        self.token = token
        self.method = method
        self.api_params = api_params

    def construct_api_call_params(self):
        """
        Used by the execute function. Allows templating on the source fields of the api_call_params dict before construction

        Override in child classes.
        Each SlackAPIOperator child class is responsible for having a construct_api_call_params function
        which sets self.api_call_params with a dict of API call parameters (https://api.slack.com/methods)
        """

        pass

    def execute(self, **kwargs):
        """
        SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """

        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        rc = sc.api_call(self.method, **self.api_params)
        if not rc['ok']:
            logging.error("Slack API call failed ({})".format(rc['error']))
            raise AirflowException("Slack API call failed: ({})".format(rc['error']))


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel

    :param channel: channel in which to post message on slack name (#general) or ID (C12318391)
    :type channel: string
    :param username: Username that airflow will be posting to Slack as
    :type username: string
    :param text: message to send to slack
    :type text: string
    :param icon_url: url to icon used for this message
    :type icon_url: string
    :param attachments: extra formatting details - see https://api.slack.com/docs/attachments
    :type attachments: array of hashes
    """

    template_fields = ('username', 'text', 'attachments')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 username='Airflow',
                 text='No message has been set.\n'
                      'Here is a cat video instead\n'
                      'https://www.youtube.com/watch?v=J---aiyznGQ',
                 icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
                 attachments=None,
                 *args, **kwargs):
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments
        super(SlackAPIPostOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
        }


class SlackPostOperator(BaseOperator):
    """
    Posts messages to a slack channel using the Slack Webhook API. Same functionality as
    :class:`.SlackAPIPostOperator`, but uses Connection info from Airflow, and does not rely on a
    deprecated API.

    :param conn_id: Connection ID (type HTTP) to use as Slack WebHook URL. Configure under Connections
                    in the Airflow administration interface, and add the Slack WebHook URL as 'extra'
    :param channel: channel in which to post message on slack name (#general) or ID (C12318391)
    :type channel: string
    :param username: Username that airflow will be posting to Slack as
    :type username: string
    :param text: message to send to slack
    :type text: string
    :param icon_url: url to icon used for this message
    :type icon_url: string
    :param attachments: extra formatting details - see https://api.slack.com/docs/attachments
    :type attachments: array of hashes
    """

    template_fields = ('username', 'text', 'attachments')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 conn_id='default_slack_webhook',
                 channel='#general',
                 username='Airflow',
                 text='No message has been set.\n'
                      'Here is a video of the Royal Guards in Stockholm, Sweden instead\n'
                      'https://www.youtube.com/watch?v=qGeZd6SovFI',
                 icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
                 attachments=None,
                 *args, **kwargs):
        self.conn_id = conn_id
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments
        super(SlackPostOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = SlackWebHookHook(self.conn_id)
        hook.post({'text': self.text,
                   'channel': self.channel,
                   'username': self.username,
                   'icon': self.icon_url,
                   'attachments': self.attachments,
                   })
