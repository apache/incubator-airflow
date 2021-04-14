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
"""
Example Airflow DAG for Google Cloud Storage time-span file transform operator.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSTimeSpanFileTransformOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State

SOURCE_BUCKET = os.environ.get("GCP_GCS_BUCKET_1", "test-gcs-example-bucket")
SOURCE_PREFIX = "gcs_timespan_file_transform_source"
SOURCE_GCP_CONN_ID = "google_cloud_default"
DESTINATION_BUCKET = SOURCE_BUCKET
DESTINATION_PREFIX = "gcs_timespan_file_transform_destination"
DESTINATION_GCP_CONN_ID = "google_cloud_default"

PATH_TO_TRANSFORM_SCRIPT = os.environ.get(
    'GCP_GCS_PATH_TO_TRANSFORM_SCRIPT', 'test_gcs_timespan_transform_script.py'
)


with models.DAG(
    "example_gcs_timespan_file_transform",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:

    # [START howto_operator_gcs_timespan_file_transform_operator_Task]
    gcs_timespan_transform_files_task = GCSTimeSpanFileTransformOperator(
        task_id="gcs_timespan_transform_files",
        source_bucket=SOURCE_BUCKET,
        source_prefix=SOURCE_PREFIX,
        source_gcp_conn_id=SOURCE_GCP_CONN_ID,
        destination_bucket=DESTINATION_BUCKET,
        destination_prefix=DESTINATION_PREFIX,
        destination_gcp_conn_id=DESTINATION_GCP_CONN_ID,
        transform_script=["python", PATH_TO_TRANSFORM_SCRIPT],
    )
    # [END howto_operator_gcs_timespan_file_transform_operator_Task]


if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
