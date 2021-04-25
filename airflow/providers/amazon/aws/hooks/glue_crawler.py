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

from time import sleep

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsGlueCrawlerHook(AwsBaseHook):
    """
    Interacts with AWS Glue Crawler.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        kwargs['client_type'] = 'glue'
        super().__init__(*args, **kwargs)

    @cached_property
    def glue_client(self):
        """:return: AWS Glue client"""
        return self.get_conn()

    def has_crawler(self, crawler_name) -> bool:
        """
        Checks if the crawler already exists

        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: Returns True if the crawler already exists and False if not.
        """
        self.log.info("Checking if crawler already exists: %s", crawler_name)

        try:
            self.get_crawler(crawler_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            return False

    def get_crawler(self, crawler_name: str) -> dict:
        """
        Gets crawler configurations

        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: Nested dictionary of crawler configurations
        """
        return self.glue_client.get_crawler(Name=crawler_name)['Crawler']

    def update_crawler(self, **crawler_kwargs) -> str:
        """
        Updates crawler configurations

        :param crawler_kwargs: Keyword args that define the configurations used for the crawler
        :type crawler_kwargs: any
        :return: True if crawler was updated and false otherwise
        """
        crawler_name = crawler_kwargs['Name']
        current_crawler = self.get_crawler(crawler_name)

        update_config = {
            key: value for key, value in crawler_kwargs.items() if current_crawler[key] != crawler_kwargs[key]
        }
        if update_config != {}:
            self.log.info("Updating crawler: %s", crawler_name)
            self.glue_client.update_crawler(**crawler_kwargs)
            self.log.info("Updated configurations: %s", update_config)
            return True
        else:
            return False

    def create_crawler(self, **crawler_kwargs) -> str:
        """
        Creates an AWS Glue Crawler

        :param crawler_kwargs: Keyword args that define the configurations used to create the crawler
        :type crawler_kwargs: any
        :return: Name of the crawler
        """
        crawler_name = crawler_kwargs['Name']
        self.log.info("Creating crawler: %s", crawler_name)
        return self.glue_client.create_crawler(**crawler_kwargs)['Crawler']['Name']

    def start_crawler(self, crawler_name: str) -> dict:
        """
        Triggers the AWS Glue crawler

        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :return: Empty dictionary
        """
        self.log.info("Starting crawler %s", crawler_name)
        crawler = self.glue_client.start_crawler(Name=crawler_name)
        return crawler

    def wait_for_crawler_completion(self, crawler_name: str, poll_interval: int = 5) -> str:
        """
        Waits until Glue crawler completes and
        returns the status of the latest crawl run.
        Raises AirflowException if the crawler fails or is cancelled.

        :param crawler_name: unique crawler name per AWS account
        :type crawler_name: str
        :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
        :type poll_interval: int
        :return: Crawler's status
        """
        failed_status = ['FAILED', 'CANCELLED']

        while True:
            crawler = self.get_crawler(crawler_name)
            crawler_state = crawler['State']
            if crawler_state == 'READY':
                self.log.info("State: %s", crawler_state)
                self.log.info("crawler_config: %s", crawler)
                crawler_status = crawler['LastCrawl']['Status']
                if crawler_status in failed_status:
                    raise AirflowException(
                        f"Status: {crawler_status}"
                    )  # pylint: disable=raising-format-tuple
                else:
                    metrics = self.glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                        'CrawlerMetricsList'
                    ][0]
                    self.log.info("Status: %s", crawler_status)
                    self.log.info("Last Runtime Duration (seconds): %s", metrics['LastRuntimeSeconds'])
                    self.log.info("Median Runtime Duration (seconds): %s", metrics['MedianRuntimeSeconds'])
                    self.log.info("Tables Created: %s", metrics['TablesCreated'])
                    self.log.info("Tables Updated: %s", metrics['TablesUpdated'])
                    self.log.info("Tables Deleted: %s", metrics['TablesDeleted'])

                    return crawler_status

            else:
                self.log.info("Polling for AWS Glue crawler: %s ", crawler_name)
                self.log.info("State: %s", crawler_state)

                metrics = self.glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                    'CrawlerMetricsList'
                ][0]
                time_left = int(metrics['TimeLeftSeconds'])

                if time_left > 0:
                    self.log.info("Estimated Time Left (seconds): %s", time_left)
                else:
                    self.log.info("Crawler should finish soon")

                sleep(poll_interval)
