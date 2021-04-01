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
from typing import Iterable

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.utils.decorators import apply_defaults


class SageMakerBaseOperator(BaseOperator):
    """
    This is the base operator for all SageMaker operators.

    :param config: The configuration necessary to start a training job (templated)
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    """

    template_fields = ['config']
    template_ext = ()
    ui_color = '#ededed'

    integer_fields = []  # type: Iterable[Iterable[str]]

    @apply_defaults
    def __init__(self, *, config: dict, aws_conn_id: str = 'aws_default', **kwargs):
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config

    def parse_integer(self, config, field):
        """Recursive method for parsing string fields holding integer values to integers."""
        if len(field) == 1:
            if isinstance(config, list):
                for sub_config in config:
                    self.parse_integer(sub_config, field)
                return
            head = field[0]
            if head in config:
                config[head] = int(config[head])
            return

        if isinstance(config, list):
            for sub_config in config:
                self.parse_integer(sub_config, field)
            return

        head, tail = field[0], field[1:]
        if head in config:
            self.parse_integer(config[head], tail)
        return

    def parse_config_integers(self):
        """
        Parse the integer fields of training config to integers in case the config is rendered by Jinja and
        all fields are str.
        """
        for field in self.integer_fields:
            self.parse_integer(self.config, field)

    def expand_role(self):  # noqa: D402
        """Placeholder for calling boto3's expand_role(), which expands an IAM role name into an ARN."""

    def preprocess_config(self):
        """Process the config into a usable form."""
        self.log.info('Preprocessing the config and doing required s3_operations')

        self.hook.configure_s3_resources(self.config)
        self.parse_config_integers()
        self.expand_role()

        self.log.info(
            "After preprocessing the config is:\n %s",
            json.dumps(self.config, sort_keys=True, indent=4, separators=(",", ": ")),
        )

    def execute(self, context):
        raise NotImplementedError('Please implement execute() in sub class!')

    @cached_property
    def hook(self):
        """Return SageMakerHook"""
        return SageMakerHook(aws_conn_id=self.aws_conn_id)
