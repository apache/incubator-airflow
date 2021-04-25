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

import sys
import warnings
from typing import Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.xcom import MAX_XCOM_SIZE
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.base import apply_defaults


class GCSToLocalFilesystemOperator(BaseOperator):
    """
    Downloads a file from Google Cloud Storage.

    If a filename is supplied, it writes the file to the specified location, alternatively one can
    set the ``store_to_xcom_key`` parameter to True push the file content into xcom. When the file size
    exceeds the maximum size for xcom it is recommended to write to a file.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToLocalFilesystemOperator`

    :param bucket: The Google Cloud Storage bucket where the object is.
        Must not contain 'gs://' prefix. (templated)
    :type bucket: str
    :param object_name: The name of the object to download in the Google cloud
        storage bucket. (templated)
    :type object_name: str
    :param filename: The file path, including filename,  on the local file system (where the
        operator is being executed) that the file should be downloaded to. (templated)
        If no filename passed, the downloaded data will not be stored on the local file
        system.
    :type filename: str
    :param store_to_xcom_key: If this param is set, the operator will push
        the contents of the downloaded file to XCom with the key set in this
        parameter. If not set, the downloaded data will not be pushed to XCom. (templated)
    :type store_to_xcom_key: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'object_name',
        'filename',
        'store_to_xcom_key',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str,
        object_name: Optional[str] = None,
        filename: Optional[str] = None,
        store_to_xcom_key: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        google_cloud_storage_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        # To preserve backward compatibility
        # TODO: Remove one day
        if object_name is None:
            if 'object' in kwargs:
                object_name = kwargs['object']
                DeprecationWarning("Use 'object_name' instead of 'object'.")
            else:
                TypeError("__init__() missing 1 required positional argument: 'object_name'")

        if filename is not None and store_to_xcom_key is not None:
            raise ValueError("Either filename or store_to_xcom_key can be set")

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = google_cloud_storage_conn_id

        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object_name
        self.filename = filename  # noqa
        self.store_to_xcom_key = store_to_xcom_key  # noqa
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        self.log.info('Executing download: %s, %s, %s', self.bucket, self.object, self.filename)
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        if self.store_to_xcom_key:
            file_bytes = hook.download(bucket_name=self.bucket, object_name=self.object)
            if sys.getsizeof(file_bytes) < MAX_XCOM_SIZE:
                context['ti'].xcom_push(key=self.store_to_xcom_key, value=str(file_bytes))
            else:
                raise AirflowException('The size of the downloaded file is too large to push to XCom!')
        else:
            hook.download(bucket_name=self.bucket, object_name=self.object, filename=self.filename)
