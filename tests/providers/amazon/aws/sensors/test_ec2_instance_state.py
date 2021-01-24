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

import unittest

import pytest
from moto import mock_ec2

from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.providers.amazon.aws.sensors.ec2_instance_state import EC2InstanceStateSensor


class TestEC2InstanceStateSensor(unittest.TestCase):
    def test_init(self):
        ec2_operator = EC2InstanceStateSensor(
            task_id="task_test",
            target_state="stopped",
            instance_id="i-123abc",
            aws_conn_id="aws_conn_test",
            region_name="region-test",
        )
        assert ec2_operator.task_id == "task_test"
        assert ec2_operator.target_state == "stopped"
        assert ec2_operator.instance_id == "i-123abc"
        assert ec2_operator.aws_conn_id == "aws_conn_test"
        assert ec2_operator.region_name == "region-test"

    def test_init_invalid_target_state(self):
        invalid_target_state = "target_state_test"
        with pytest.raises(ValueError) as ctx:
            EC2InstanceStateSensor(
                task_id="task_test",
                target_state=invalid_target_state,
                instance_id="i-123abc",
            )
        msg = f"Invalid target_state: {invalid_target_state}"
        assert str(ctx.value) == msg

    @mock_ec2
    def test_running(self):
        # create instance
        ec2_hook = EC2Hook()
        instances = ec2_hook.conn.run_instances(
            MaxCount=1,
            MinCount=1,
        )
        instance_id = instances['Instances'][0]['InstanceId']
        # stop instance
        ec2_hook.stop_instances(instance_ids=[instance_id])

        # start sensor, waits until ec2 instance state became running
        start_sensor = EC2InstanceStateSensor(
            task_id="start_sensor",
            target_state="running",
            instance_id=instance_id,
        )
        # assert instance state is not running
        assert not start_sensor.poke(None)
        # start instance
        ec2_hook.start_instances(instance_ids=[instance_id])
        # assert instance state is running
        assert start_sensor.poke(None)

    @mock_ec2
    def test_stopped(self):
        # create instance
        ec2_hook = EC2Hook()
        instances = ec2_hook.conn.run_instances(
            MaxCount=1,
            MinCount=1,
        )
        instance_id = instances['Instances'][0]['InstanceId']
        # start instance
        ec2_hook.start_instances(instance_ids=[instance_id])

        # stop sensor, waits until ec2 instance state became stopped
        stop_sensor = EC2InstanceStateSensor(
            task_id="stop_sensor",
            target_state="stopped",
            instance_id=instance_id,
        )
        # assert instance state is not stopped
        assert not stop_sensor.poke(None)
        # stop instance
        ec2_hook.stop_instances(instance_ids=[instance_id])
        # assert instance state is stopped
        assert stop_sensor.poke(None)

    @mock_ec2
    def test_terminated(self):
        # create instance
        ec2_hook = EC2Hook()
        instances = ec2_hook.conn.run_instances(
            MaxCount=1,
            MinCount=1,
        )
        instance_id = instances['Instances'][0]['InstanceId']
        # start instance
        ec2_hook.start_instances(instance_ids=[instance_id])

        # stop sensor, waits until ec2 instance state became terminated
        stop_sensor = EC2InstanceStateSensor(
            task_id="stop_sensor",
            target_state="terminated",
            instance_id=instance_id,
        )
        # assert instance state is not terminated
        assert not stop_sensor.poke(None)
        # terminate instance
        ec2_hook.terminate_instances(instance_ids=[instance_id])
        # assert instance state is terminated
        assert stop_sensor.poke(None)
