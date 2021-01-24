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
#

import time

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EC2Hook(AwsBaseHook):
    """
    Interact with AWS EC2 Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "ec2"
        super().__init__(*args, **kwargs)

    def stop_instances(self, instance_ids: list) -> dict:
        """
        Stop instances with given ids

        :param instance_ids: List of instance ids to stop
        :return: Dict with key `StoppingInstances` and value as list of instances being stopped
        """
        self.log.info("Stopping instances: %s", instance_ids)

        return self.conn.stop_instances(InstanceIds=instance_ids)

    def start_instances(self, instance_ids: list) -> dict:
        """
        Start instances with given ids

        :param instance_ids: List of instance ids to start
        :return: Dict with key `StartingInstances` and value as list of instances being started
        """
        self.log.info("Starting instances: %s", instance_ids)

        return self.conn.start_instances(InstanceIds=instance_ids)

    def terminate_instances(self, instance_ids: list) -> dict:
        """
        Terminate instances with given ids

        :param instance_ids: List of instance ids to terminate
        :return: Dict with key `TerminatingInstances` and value as list of instances being terminated
        """
        self.log.info("Terminating instances: %s", instance_ids)

        return self.conn.terminate_instances(InstanceIds=instance_ids)

    def describe_instances(self, filters: list = None, instance_ids: list = None):
        """
        Describe EC2 instances, optionally applying filters and selective instance ids

        :param filters: List of filters to specify instances to describe
        :param instance_ids: List of instance IDs to describe
        :return: Response from EC2 describe_instances API
        """
        filters = filters or []
        instance_ids = instance_ids or []

        self.log.info("Filters provided: %s", filters)
        self.log.info("Instance ids provided: %s", instance_ids)

        return self.conn.describe_instances(Filters=filters, InstanceIds=instance_ids)

    def get_instances(self, filters: list = None, instance_ids: list = None) -> list:
        """
        Get list of instance details, optionally applying filters and selective instance ids

        :param instance_ids: List of ids to get instances for
        :param filters: List of filters to specify instances to get
        :return: List of instances
        """
        description = self.describe_instances(filters=filters, instance_ids=instance_ids)

        return [
            instance for reservation in description['Reservations'] for instance in reservation['Instances']
        ]

    def get_instance_ids(self, filters: list = None) -> list:
        """
        Get list of instance ids, optionally applying filters to fetch selective instances

        :param filters: List of filters to specify instances to get
        :return: List of instance ids
        """
        return [instance['InstanceId'] for instance in self.get_instances(filters=filters)]

    def get_instance_state(self, instance_id: str) -> str:
        """
        Get EC2 instance state by id and return it.

        :param instance_id: id of the AWS EC2 instance
        :type instance_id: str
        :return: current state of the instance
        :rtype: str
        """
        return self.get_instances(instance_ids=[instance_id])[0]['State']['Name']

    def wait_for_state(self, instance_id: str, target_state: str, check_interval: float) -> None:
        """
        Wait EC2 instance until its state is equal to the target_state.

        :param instance_id: id of the AWS EC2 instance
        :type instance_id: str
        :param target_state: target state of instance
        :type target_state: str
        :param check_interval: time in seconds that the job should wait in
            between each instance state checks until operation is completed
        :type check_interval: float
        :return: None
        :rtype: None
        """
        instance_state = self.get_instance_state(instance_id=instance_id)

        self.log.info(
            "instance state: %s. Same as target: %s", instance_state, instance_state == target_state
        )

        while instance_state != target_state:
            time.sleep(check_interval)
            instance_state = self.get_instance_state(instance_id=instance_id)

            self.log.info(
                "instance state: %s. Same as target: %s", instance_state, instance_state == target_state
            )
