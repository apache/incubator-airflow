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
Example Airflow DAG for Google Cloud Storage operators.
"""

import os

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSFileTransformOperator,
    GCSListObjectsOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-id")
BUCKET_1 = os.environ.get("GCP_GCS_BUCKET_1", "test-gcs-example-bucket")
GCS_ACL_ENTITY = os.environ.get("GCS_ACL_ENTITY", "allUsers")
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"

BUCKET_2 = os.environ.get("GCP_GCS_BUCKET_2", "test-gcs-example-bucket-2")

PATH_TO_TRANSFORM_SCRIPT = os.environ.get('GCP_GCS_PATH_TO_TRANSFORM_SCRIPT', 'test.py')
PATH_TO_UPLOAD_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example.txt")
PATH_TO_UPLOAD_FILE_PREFIX = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE_PREFIX", "test-gcs-")
PATH_TO_SAVED_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "test-gcs-example-download.txt")

BUCKET_FILE_LOCATION = PATH_TO_UPLOAD_FILE.rpartition("/")[-1]

with models.DAG(
    "example_gcs",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:
    create_bucket1 = GCSCreateBucketOperator(
        task_id="create_bucket1", bucket_name=BUCKET_1, project_id=PROJECT_ID
    )

    create_bucket2 = GCSCreateBucketOperator(
        task_id="create_bucket2", bucket_name=BUCKET_2, project_id=PROJECT_ID
    )

    list_buckets = GCSListObjectsOperator(task_id="list_buckets", bucket=BUCKET_1)

    list_buckets_result = BashOperator(
        task_id="list_buckets_result",
        bash_command="echo \"{{ task_instance.xcom_pull('list_buckets') }}\"",
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
    )

    transform_file = GCSFileTransformOperator(
        task_id="transform_file",
        source_bucket=BUCKET_1,
        source_object=BUCKET_FILE_LOCATION,
        transform_script=["python", PATH_TO_TRANSFORM_SCRIPT],
    )
    # [START howto_operator_gcs_bucket_create_acl_entry_task]
    gcs_bucket_create_acl_entry_task = GCSBucketCreateAclEntryOperator(
        bucket=BUCKET_1,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_BUCKET_ROLE,
        task_id="gcs_bucket_create_acl_entry_task",
    )
    # [END howto_operator_gcs_bucket_create_acl_entry_task]

    # [START howto_operator_gcs_object_create_acl_entry_task]
    gcs_object_create_acl_entry_task = GCSObjectCreateAclEntryOperator(
        bucket=BUCKET_1,
        object_name=BUCKET_FILE_LOCATION,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_OBJECT_ROLE,
        task_id="gcs_object_create_acl_entry_task",
    )
    # [END howto_operator_gcs_object_create_acl_entry_task]

    # [START howto_operator_gcs_download_file_task]
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
        filename=PATH_TO_SAVED_FILE,
    )
    # [END howto_operator_gcs_download_file_task]

    copy_file = GCSToGCSOperator(
        task_id="copy_file",
        source_bucket=BUCKET_1,
        source_object=BUCKET_FILE_LOCATION,
        destination_bucket=BUCKET_2,
        destination_object=BUCKET_FILE_LOCATION,
    )

    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files", bucket_name=BUCKET_1, objects=[BUCKET_FILE_LOCATION]
    )

    # [START howto_operator_gcs_delete_bucket]
    delete_bucket_1 = GCSDeleteBucketOperator(task_id="delete_bucket_1", bucket_name=BUCKET_1)
    delete_bucket_2 = GCSDeleteBucketOperator(task_id="delete_bucket_2", bucket_name=BUCKET_2)
    # [END howto_operator_gcs_delete_bucket]

    [create_bucket1, create_bucket2] >> list_buckets >> list_buckets_result
    [create_bucket1, create_bucket2] >> upload_file
    upload_file >> [download_file, copy_file]
    upload_file >> gcs_bucket_create_acl_entry_task >> gcs_object_create_acl_entry_task >> delete_files

    create_bucket1 >> delete_bucket_1
    create_bucket2 >> delete_bucket_2
    create_bucket2 >> copy_file
    create_bucket1 >> copy_file
    list_buckets >> delete_bucket_1
    upload_file >> delete_bucket_1
    create_bucket1 >> upload_file >> delete_bucket_1
    upload_file >> transform_file >> delete_bucket_1
    gcs_bucket_create_acl_entry_task >> delete_bucket_1
    gcs_object_create_acl_entry_task >> delete_bucket_1
    download_file >> delete_bucket_1
    copy_file >> delete_bucket_1
    copy_file >> delete_bucket_2
    delete_files >> delete_bucket_1

with models.DAG(
    "example_gcs_sensors",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag2:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_1, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
    )
    # [START howto_sensor_object_exists_task]
    gcs_object_exists = GCSObjectExistenceSensor(
        bucket=BUCKET_1,
        object=PATH_TO_UPLOAD_FILE,
        mode='poke',
        task_id="gcs_object_exists_task",
    )
    # [END howto_sensor_object_exists_task]
    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_1,
        prefix=PATH_TO_UPLOAD_FILE_PREFIX,
        mode='poke',
        task_id="gcs_object_with_prefix_exists_task",
    )
    # [END howto_sensor_object_with_prefix_exists_task]
    delete_bucket = GCSDeleteBucketOperator(task_id="delete_bucket", bucket_name=BUCKET_1)

    create_bucket >> upload_file >> [gcs_object_exists, gcs_object_with_prefix_exists] >> delete_bucket


if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
