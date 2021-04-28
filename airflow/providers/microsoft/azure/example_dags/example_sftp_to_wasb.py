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
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.utils.dates import days_ago

AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "airflow")
BLOB_PREFIX = os.environ.get("AZURE_BLOB_PREFIX", "airflow")
SFTP_SRC_PATH = os.environ.get("SFTP_SRC_PATH", "/sftp")
LOCAL_FILE_PATH = os.environ.get("LOCAL_SRC_PATH", "/tmp")
SAMPLE_FILENAME = os.environ.get("SFTP_SAMPLE_FILENAME", "sftp_to_wasb_test.txt")
FILE_COMPLETE_PATH = os.path.join(LOCAL_FILE_PATH, SAMPLE_FILENAME)
SFTP_FILE_COMPLETE_PATH = os.path.join(SFTP_SRC_PATH, SAMPLE_FILENAME)


def delete_sftp_file():
    """Delete a file at SFTP SERVER"""
    SFTPHook(ssh_conn_id="sftp_default").delete_file(SFTP_FILE_COMPLETE_PATH)


with DAG(
    "example_sftp_to_wasb",
    schedule_interval=None,
    start_date=days_ago(1),  # Override to match your needs
) as dag:

    transfer_files_to_sftp_step = SFTPOperator(
        task_id="transfer_files_from_local_to_sftp",
        ssh_conn_id="sftp_default",
        local_filepath=FILE_COMPLETE_PATH,
        remote_filepath=SFTP_FILE_COMPLETE_PATH,
    )

    # [START how_to_sftp_to_wasb]
    transfer_files_to_azure = SFTPToWasbOperator(
        task_id="transfer_files_from_sftp_to_wasb",
        # SFTP args
        sftp_conn_id="sftp_default",
        sftp_source_path=SFTP_SRC_PATH,
        # AZURE args
        wasb_conn_id="wasb_default",
        container_name=AZURE_CONTAINER_NAME,
        blob_prefix=BLOB_PREFIX,
    )
    # [END how_to_sftp_to_wasb]

    delete_blob_file_step = WasbDeleteBlobOperator(
        task_id="delete_blob_files",
        wasb_conn_id="wasb_default",
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_PREFIX + SAMPLE_FILENAME,
    )

    delete_sftp_step = PythonOperator(task_id="delete_sftp_file", python_callable=delete_sftp_file)

    transfer_files_to_sftp_step >> transfer_files_to_azure >> delete_blob_file_step >> delete_sftp_step
