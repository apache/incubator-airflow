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
You can execute this module to get ID Token.

    python -m airflow.providers.google.common.utils.id_token_credentials_provider

To obtain info about this token, run the following commands:

    ID_TOKEN="$(python -m airflow.providers.google.common.utils.id_token_credentials)"
    curl "https://www.googleapis.com/oauth2/v3/tokeninfo?id_token=${ID_TOKEN}" -v
"""

import json
import os
from typing import Optional

import google.auth.transport
import google.oauth2
from google.auth import credentials as google_auth_credentials, environment_vars, exceptions
from google.auth._default import _AUTHORIZED_USER_TYPE, _HELP_MESSAGE, _SERVICE_ACCOUNT_TYPE, _VALID_TYPES
from google.oauth2 import credentials as oauth2_credentials, service_account


class IDTokenCredentialsAdapter(google_auth_credentials.Credentials):
    """Convert Credentials with "openid" scope to IDTokenCredentials."""

    def __init__(self, credentials: oauth2_credentials.Credentials):
        super().__init__()
        self.credentials = credentials
        self.token = credentials.id_token

    @property
    def expired(self):
        return self.credentials.expired

    def refresh(self, request):
        self.credentials.refresh(request)
        self.token = self.credentials.id_token


def _load_credentials_from_file(
    filename: str, target_audience: Optional[str]
) -> Optional[google_auth_credentials.Credentials]:
    """
    Loads credentials from a file.

    The credentials file must be a service account key or a stored authorized user credential.

    :param filename: The full path to the credentials file.
    :type filename: str
    :return: Loaded credentials
    :rtype: google.auth.credentials.Credentials
    :raise google.auth.exceptions.DefaultCredentialsError: if the file is in the wrong format or is missing.
    """
    if not os.path.exists(filename):
        raise exceptions.DefaultCredentialsError(f"File {filename} was not found.")

    with open(filename) as file_obj:
        try:
            info = json.load(file_obj)
        except json.JSONDecodeError:
            raise exceptions.DefaultCredentialsError(f"File {filename} is not a valid json file.")

    # The type key should indicate that the file is either a service account
    # credentials file or an authorized user credentials file.
    credential_type = info.get("type")

    if credential_type == _AUTHORIZED_USER_TYPE:
        current_credentials = oauth2_credentials.Credentials.from_authorized_user_info(
            info, scopes=["openid", "email"]
        )
        current_credentials = IDTokenCredentialsAdapter(credentials=current_credentials)

        return current_credentials

    elif credential_type == _SERVICE_ACCOUNT_TYPE:
        try:
            return service_account.IDTokenCredentials.from_service_account_info(
                info, target_audience=target_audience
            )
        except ValueError:
            raise exceptions.DefaultCredentialsError(
                f"Failed to load service account credentials from {filename}"
            )

    raise exceptions.DefaultCredentialsError(
        f"The file {filename} does not have a valid type. Type is {credential_type}, "
        f"expected one of {_VALID_TYPES}."
    )


def _get_explicit_environ_credentials(
    target_audience: Optional[str],
) -> Optional[google_auth_credentials.Credentials]:
    """Gets credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    explicit_file = os.environ.get(environment_vars.CREDENTIALS)

    if explicit_file is None:
        return None

    current_credentials = _load_credentials_from_file(
        os.environ[environment_vars.CREDENTIALS], target_audience=target_audience
    )

    return current_credentials


def _get_gcloud_sdk_credentials(
    target_audience: Optional[str],
) -> Optional[google_auth_credentials.Credentials]:
    """Gets the credentials and project ID from the Cloud SDK."""
    from google.auth import _cloud_sdk

    # Check if application default credentials exist.
    credentials_filename = _cloud_sdk.get_application_default_credentials_path()

    if not os.path.isfile(credentials_filename):
        return None

    current_credentials = _load_credentials_from_file(credentials_filename, target_audience)

    return current_credentials


def _get_gce_credentials(
    target_audience: Optional[str], request: Optional[google.auth.transport.Request] = None
) -> Optional[google_auth_credentials.Credentials]:
    """Gets credentials and project ID from the GCE Metadata Service."""
    # Ping requires a transport, but we want application default credentials
    # to require no arguments. So, we'll use the _http_client transport which
    # uses http.client. This is only acceptable because the metadata server
    # doesn't do SSL and never requires proxies.

    # While this library is normally bundled with compute_engine, there are
    # some cases where it's not available, so we tolerate ImportError.
    try:
        from google.auth import compute_engine
        from google.auth.compute_engine import _metadata
    except ImportError:
        return None
    from google.auth.transport import _http_client

    if request is None:
        request = _http_client.Request()

    if _metadata.ping(request=request):
        return compute_engine.IDTokenCredentials(
            request, target_audience, use_metadata_identity_endpoint=True
        )

    return None


def get_default_id_token_credentials(
    target_audience: Optional[str], request: google.auth.transport.Request = None
) -> google_auth_credentials.Credentials:
    """Gets the default ID Token credentials for the current environment.

    `Application Default Credentials`_ provides an easy way to obtain credentials to call Google APIs for
    server-to-server or local applications.

    .. _Application Default Credentials: https://developers.google.com\
        /identity/protocols/application-default-credentials

    :param target_audience: The intended audience for these credentials.
    :type target_audience: Sequence[str]
    :param request: An object used to make HTTP requests. This is used to detect whether the application
            is running on Compute Engine. If not specified, then it will use the standard library http client
            to make requests.
    :type request: google.auth.transport.Request
    :return: the current environment's credentials.
    :rtype: google.auth.credentials.Credentials
    :raises ~google.auth.exceptions.DefaultCredentialsError:
        If no credentials were found, or if the credentials found were invalid.
    """
    checkers = (
        lambda: _get_explicit_environ_credentials(target_audience),
        lambda: _get_gcloud_sdk_credentials(target_audience),
        lambda: _get_gce_credentials(target_audience, request),
    )

    for checker in checkers:
        current_credentials = checker()
        if current_credentials is not None:
            return current_credentials

    raise exceptions.DefaultCredentialsError(_HELP_MESSAGE)


if __name__ == "__main__":
    from google.auth.transport import requests

    request_adapter = requests.Request()

    creds = get_default_id_token_credentials(target_audience=None)
    creds.refresh(request=request_adapter)
    print(creds.token)
