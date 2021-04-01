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

import os.path
from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class AwsGlueJobOperator(BaseOperator):
    """
    Creates an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala

    :param job_name: unique job name per AWS Account
    :type job_name: Optional[str]
    :param script_location: location of ETL script. Must be a local or S3 path
    :type script_location: Optional[str]
    :param job_desc: job description details
    :type job_desc: Optional[str]
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :type concurrent_run_limit: Optional[int]
    :param script_args: etl script arguments and AWS Glue arguments (templated)
    :type script_args: dict
    :param retry_limit: The maximum number of times to retry this job if it fails
    :type retry_limit: Optional[int]
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job.
    :type num_of_dpus: int
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
    :type s3_bucket: Optional[str]
    :param iam_role_name: AWS IAM Role for Glue Job Execution
    :type iam_role_name: Optional[str]
    :param create_job_kwargs: Extra arguments for Glue Job Creation
    :type create_job_kwargs: Optional[dict]
    """

    template_fields = ('script_args',)
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        job_name: str = 'aws_glue_default_job',
        job_desc: str = 'AWS Glue Job with Airflow',
        script_location: Optional[str] = None,
        concurrent_run_limit: Optional[int] = None,
        script_args: Optional[dict] = None,
        retry_limit: Optional[int] = None,
        num_of_dpus: int = 6,
        aws_conn_id: str = 'aws_default',
        region_name: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        iam_role_name: Optional[str] = None,
        create_job_kwargs: Optional[dict] = None,
        **kwargs,
    ):  # pylint: disable=too-many-arguments
        super().__init__(**kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_location = script_location
        self.concurrent_run_limit = concurrent_run_limit or 1
        self.script_args = script_args or {}
        self.retry_limit = retry_limit
        self.num_of_dpus = num_of_dpus
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.iam_role_name = iam_role_name
        self.s3_protocol = "s3://"
        self.s3_artifacts_prefix = 'artifacts/glue-scripts/'
        self.create_job_kwargs = create_job_kwargs

    def execute(self, context):
        """
        Executes AWS Glue Job from Airflow

        :return: the id of the current glue job.
        """
        if self.script_location and not self.script_location.startswith(self.s3_protocol):
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            script_name = os.path.basename(self.script_location)
            s3_hook.load_file(self.script_location, self.s3_bucket, self.s3_artifacts_prefix + script_name)
        glue_job = AwsGlueJobHook(
            job_name=self.job_name,
            desc=self.job_desc,
            concurrent_run_limit=self.concurrent_run_limit,
            script_location=self.script_location,
            retry_limit=self.retry_limit,
            num_of_dpus=self.num_of_dpus,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            s3_bucket=self.s3_bucket,
            iam_role_name=self.iam_role_name,
            create_job_kwargs=self.create_job_kwargs,
        )
        self.log.info("Initializing AWS Glue Job: %s", self.job_name)
        glue_job_run = glue_job.initialize_job(self.script_args)
        glue_job_run = glue_job.job_completion(self.job_name, glue_job_run['JobRunId'])
        self.log.info(
            "AWS Glue Job: %s status: %s. Run Id: %s",
            self.job_name,
            glue_job_run['JobRunState'],
            glue_job_run['JobRunId'],
        )
        return glue_job_run['JobRunId']
