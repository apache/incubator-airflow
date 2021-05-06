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
from unittest import mock

import boto3
import pytest
from moto import mock_datasync

from airflow.exceptions import AirflowTaskTimeout
from airflow.providers.amazon.aws.hooks.datasync import AWSDataSyncHook


@mock_datasync
class TestAwsDataSyncHook(unittest.TestCase):
    def test_get_conn(self):
        hook = AWSDataSyncHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None


# Explanation of: @mock.patch.object(AWSDataSyncHook, 'get_conn')
# base_aws.py fiddles with config files and changes the region
# If you have any ~/.credentials then aws_hook uses it for the region
# This region might not match us-east-1 used for the mocked self.client

# Once patched, the AWSDataSyncHook.get_conn method is mocked and passed to the test as
# mock_get_conn. We then override it to just return the locally created self.client instead of
# the one created by the AWS self.hook.

# Unfortunately this means we can't test the get_conn method - which is why we have it in a
# separate class above


@mock_datasync
@mock.patch.object(AWSDataSyncHook, "get_conn")
class TestAWSDataSyncHookMocked(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_server_hostname = "host"
        self.source_subdirectory = "somewhere"
        self.destination_bucket_name = "my_bucket"
        self.destination_bucket_dir = "dir"

        self.client = None
        self.hook = None
        self.source_location_arn = None
        self.destination_location_arn = None
        self.task_arn = None

    def setUp(self):
        self.client = boto3.client("datasync", region_name="us-east-1")
        self.hook = AWSDataSyncHook(aws_conn_id="aws_default", wait_interval_seconds=0)

        # Create default locations and tasks
        self.source_location_arn = self.client.create_location_smb(
            ServerHostname=self.source_server_hostname,
            Subdirectory=self.source_subdirectory,
            User="",
            Password="",
            AgentArns=["stuff"],
        )["LocationArn"]
        self.destination_location_arn = self.client.create_location_s3(
            S3BucketArn=f"arn:aws:s3:::{self.destination_bucket_name}",
            Subdirectory=self.destination_bucket_dir,
            S3Config={"BucketAccessRoleArn": "role"},
        )["LocationArn"]
        self.task_arn = self.client.create_task(
            SourceLocationArn=self.source_location_arn,
            DestinationLocationArn=self.destination_location_arn,
        )["TaskArn"]

    def tearDown(self):
        # Delete all tasks:
        tasks = self.client.list_tasks()
        for task in tasks["Tasks"]:
            self.client.delete_task(TaskArn=task["TaskArn"])
        # Delete all locations:
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            self.client.delete_location(LocationArn=location["LocationArn"])
        self.client = None

    def test_init(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        assert not self.hook.locations
        assert not self.hook.tasks
        assert self.hook.wait_interval_seconds == 0

    def test_create_location_smb(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        locations = self.hook.get_conn().list_locations()
        assert len(locations["Locations"]) == 2

        server_hostname = "my.hostname"
        subdirectory = "my_dir"
        agent_arns = ["stuff"]
        user = "username123"
        domain = "COMPANY.DOMAIN"
        mount_options = {"Version": "SMB2"}

        location_uri = f"smb://{server_hostname}/{subdirectory}"

        create_location_kwargs = {
            "ServerHostname": server_hostname,
            "Subdirectory": subdirectory,
            "User": user,
            "Password": "password",
            "Domain": domain,
            "AgentArns": agent_arns,
            "MountOptions": mount_options,
        }
        location_arn = self.hook.create_location(location_uri, **create_location_kwargs)
        assert location_arn is not None

        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 3

        location_desc = self.client.describe_location_smb(LocationArn=location_arn)
        assert location_desc["LocationArn"] == location_arn
        assert location_desc["LocationUri"] == location_uri
        assert location_desc["AgentArns"] == agent_arns
        assert location_desc["User"] == user
        assert location_desc["Domain"] == domain
        assert location_desc["MountOptions"] == mount_options

    def test_create_location_s3(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        locations = self.hook.get_conn().list_locations()
        assert len(locations["Locations"]) == 2

        s3_bucket_arn = "some_s3_arn"
        subdirectory = "my_subdir"
        s3_config = {"BucketAccessRoleArn": "myrole"}

        location_uri = f"s3://{s3_bucket_arn}/{subdirectory}"

        create_location_kwargs = {
            "S3BucketArn": s3_bucket_arn,
            "Subdirectory": subdirectory,
            "S3Config": s3_config,
        }
        location_arn = self.hook.create_location(location_uri, **create_location_kwargs)
        assert location_arn is not None

        locations = self.client.list_locations()
        assert len(locations["Locations"]) == 3

        location_desc = self.client.describe_location_s3(LocationArn=location_arn)
        assert location_desc["LocationArn"] == location_arn
        assert location_desc["LocationUri"] == location_uri
        assert location_desc["S3Config"] == s3_config

    def test_create_task(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        log_group_arn = "cloudwatcharn123"
        name = "my_task"

        options = {  # Random options
            "VerifyMode": "NONE",
            "Atime": "NONE",
            "Mtime": "NONE",
            "Uid": "BOTH",
            "Gid": "INT_VALUE",
            "PreserveDeletedFiles": "PRESERVE",
            "PreserveDevices": "PRESERVE",
            "PosixPermissions": "BEST_EFFORT",
            "BytesPerSecond": 123,
        }

        create_task_kwargs = {
            "CloudWatchLogGroupArn": log_group_arn,
            "Name": name,
            "Options": options,
        }

        task_arn = self.hook.create_task(
            source_location_arn=self.source_location_arn,
            destination_location_arn=self.destination_location_arn,
            **create_task_kwargs,
        )

        task = self.client.describe_task(TaskArn=task_arn)
        assert task["TaskArn"] == task_arn
        assert task["Name"] == name
        assert task["CloudWatchLogGroupArn"] == log_group_arn
        assert task["Options"] == options

    def test_update_task(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_arn = self.task_arn

        task = self.client.describe_task(TaskArn=task_arn)
        assert "Name" not in task

        update_task_kwargs = {"Name": "xyz"}
        self.hook.update_task(task_arn, **update_task_kwargs)

        task = self.client.describe_task(TaskArn=task_arn)
        assert task["Name"] == "xyz"

    def test_delete_task(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_arn = self.task_arn

        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 1

        self.hook.delete_task(task_arn)

        tasks = self.client.list_tasks()
        assert len(tasks["Tasks"]) == 0

    def test_get_location_arns(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Get true location_arn from boto/moto self.client
        location_uri = f"smb://{self.source_server_hostname}/{self.source_subdirectory}"
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            if location["LocationUri"] == location_uri:
                location_arn = location["LocationArn"]

        # Verify our self.hook gets the same
        location_arns = self.hook.get_location_arns(location_uri)

        assert len(location_arns) == 1
        assert location_arns[0] == location_arn

    def test_get_location_arns_case_sensitive(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Get true location_arn from boto/moto self.client
        location_uri = f"smb://{self.source_server_hostname.upper()}/{self.source_subdirectory}"
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            if location["LocationUri"] == location_uri.lower():
                location_arn = location["LocationArn"]

        # Verify our self.hook can do case sensitive searches
        location_arns = self.hook.get_location_arns(location_uri, case_sensitive=True)
        assert len(location_arns) == 0
        location_arns = self.hook.get_location_arns(location_uri, case_sensitive=False)
        assert len(location_arns) == 1
        assert location_arns[0] == location_arn

    def test_get_location_arns_trailing_slash(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        # Get true location_arn from boto/moto self.client
        location_uri = f"smb://{self.source_server_hostname}/{self.source_subdirectory}/"
        locations = self.client.list_locations()
        for location in locations["Locations"]:
            if location["LocationUri"] == location_uri[:-1]:
                location_arn = location["LocationArn"]

        # Verify our self.hook manages trailing / correctly
        location_arns = self.hook.get_location_arns(location_uri, ignore_trailing_slash=False)
        assert len(location_arns) == 0
        location_arns = self.hook.get_location_arns(location_uri, ignore_trailing_slash=True)
        assert len(location_arns) == 1
        assert location_arns[0] == location_arn

    def test_get_task_arns_for_location_arns(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_arns = self.hook.get_task_arns_for_location_arns(
            [self.source_location_arn], [self.destination_location_arn]
        )
        assert len(task_arns) == 1
        assert task_arns[0] == self.task_arn

        task_arns = self.hook.get_task_arns_for_location_arns(["foo"], ["bar"])
        assert len(task_arns) == 0

    def test_start_task_execution(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task = self.client.describe_task(TaskArn=self.task_arn)
        assert "CurrentTaskExecutionArn" not in task

        task_execution_arn = self.hook.start_task_execution(self.task_arn)
        assert task_execution_arn is not None

        task = self.client.describe_task(TaskArn=self.task_arn)
        assert "CurrentTaskExecutionArn" in task
        assert task["CurrentTaskExecutionArn"] == task_execution_arn

        task_execution = self.client.describe_task_execution(TaskExecutionArn=task_execution_arn)
        assert "Status" in task_execution

    def test_cancel_task_execution(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_execution_arn = self.hook.start_task_execution(self.task_arn)
        assert task_execution_arn is not None

        self.hook.cancel_task_execution(task_execution_arn=task_execution_arn)

        task = self.client.describe_task(TaskArn=self.task_arn)
        assert "CurrentTaskExecutionArn" not in task

    def test_get_task_description(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task = self.client.describe_task(TaskArn=self.task_arn)
        assert "TaskArn" in task
        assert "Status" in task
        assert "SourceLocationArn" in task
        assert "DestinationLocationArn" in task
        assert "CurrentTaskExecutionArn" not in task

    def test_get_current_task_execution_arn(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_execution_arn = self.hook.start_task_execution(self.task_arn)

        current_task_execution = self.hook.get_current_task_execution_arn(self.task_arn)
        assert current_task_execution == task_execution_arn

    def test_wait_for_task_execution(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_execution_arn = self.hook.start_task_execution(self.task_arn)
        result = self.hook.wait_for_task_execution(task_execution_arn, max_iterations=20)

        assert result is not None

    def test_wait_for_task_execution_timeout(self, mock_get_conn):
        # ### Configure mock:
        mock_get_conn.return_value = self.client
        # ### Begin tests:

        task_execution_arn = self.hook.start_task_execution(self.task_arn)
        with pytest.raises(AirflowTaskTimeout):
            result = self.hook.wait_for_task_execution(task_execution_arn, max_iterations=1)
            assert result is None
