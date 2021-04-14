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

from airflow import models
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.utils.dates import days_ago

FOLDER_ID = os.environ.get("FILE_ID", "1234567890qwerty")
FILE_NAME = os.environ.get("FILE_NAME", "file.pdf")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "out_file.pdf")

with models.DAG(
    "example_gdrive_to_local_with_gdrive_sensor",
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=["example"],
) as dag:
    # [START detect_file]
    detect_file = GoogleDriveFileExistenceSensor(
        task_id="detect_file", folder_id=FOLDER_ID, file_name=FILE_NAME
    )
    # [END detect_file]
    # [START download_from_gdrive_to_local]
    download_from_gdrive_to_local = GoogleDriveToLocalOperator(
        task_id="download_from_gdrive_to_local",
        folder_id=FOLDER_ID,
        file_name=FILE_NAME,
        output_file=OUTPUT_FILE,
    )
    # [END download_from_gdrive_to_local]
    detect_file >> download_from_gdrive_to_local
