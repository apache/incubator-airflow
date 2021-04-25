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
import os

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.operators.s3_bucket_tagging import (
    S3DeleteBucketTaggingOperator,
    S3GetBucketTaggingOperator,
    S3PutBucketTaggingOperator,
)
from airflow.utils.dates import days_ago

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'test-s3-bucket-tagging')
TAG_KEY = os.environ.get('TAG_KEY', 'test-s3-bucket-tagging-key')
TAG_VALUE = os.environ.get('TAG_VALUE', 'test-s3-bucket-tagging-value')


with DAG(
    dag_id='s3_bucket_tagging_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_tagging_dag_create',
        bucket_name=BUCKET_NAME,
        region_name='us-east-1',
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id='s3_bucket_tagging_dag_delete',
        bucket_name=BUCKET_NAME,
        force_delete=True,
    )

    # [START howto_operator_s3_bucket_tagging]
    get_tagging = S3GetBucketTaggingOperator(
        task_id='s3_bucket_tagging_dag_get_tagging', bucket_name=BUCKET_NAME
    )

    put_tagging = S3PutBucketTaggingOperator(
        task_id='s3_bucket_tagging_dag_put_tagging', bucket_name=BUCKET_NAME, key=TAG_KEY, value=TAG_VALUE
    )

    delete_tagging = S3DeleteBucketTaggingOperator(
        task_id='s3_bucket_tagging_dag_delete_tagging', bucket_name=BUCKET_NAME
    )
    # [END howto_operator_s3_bucket_tagging]

    create_bucket >> put_tagging >> get_tagging >> delete_tagging >> delete_bucket
