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

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook
from airflow.utils.decorators import apply_defaults


class AwsGlueCrawlerOperator(BaseOperator):
    """
    Creates, updates and triggers an AWS Glue Crawler. AWS Glue Crawler is a serverless
    service that manages a catalog of metadata tables that contain the inferred
    schema, format and data types of data stores within the AWS cloud.

    :param config: Configurations for the AWS Glue crawler
    :type config: dict
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: Optional[str]
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :type poll_interval: Optional[int]
    """

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        config,
        aws_conn_id='aws_default',
        poll_interval: int = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.config = config

    @cached_property
    def hook(self) -> AwsGlueCrawlerHook:
        """Create and return an AwsGlueCrawlerHook."""
        return AwsGlueCrawlerHook(self.aws_conn_id)

    def execute(self, context):
        """
        Executes AWS Glue Crawler from Airflow

        :return: the name of the current glue crawler.
        """
        crawler_name = self.config['Name']
        if self.hook.has_crawler(crawler_name):
            self.hook.update_crawler(**self.config)
        else:
            self.hook.create_crawler(**self.config)

        self.log.info("Triggering AWS Glue Crawler")
        self.hook.start_crawler(crawler_name)
        self.log.info("Waiting for AWS Glue Crawler")
        self.hook.wait_for_crawler_completion(crawler_name=crawler_name, poll_interval=self.poll_interval)

        return crawler_name
