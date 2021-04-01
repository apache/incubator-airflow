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
"""This module is deprecated. Please use `airflow.providers.google.cloud.sensors.gcs`."""

import warnings

from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.sensors.gcs`.",
    DeprecationWarning,
    stacklevel=2,
)


class GoogleCloudStorageObjectSensor(GCSObjectExistenceSensor):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GoogleCloudStorageObjectUpdatedSensor(GCSObjectUpdateSensor):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GoogleCloudStoragePrefixSensor(GCSObjectsWithPrefixExistenceSensor):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GoogleCloudStorageUploadSessionCompleteSensor(GCSUploadSessionCompleteSensor):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
