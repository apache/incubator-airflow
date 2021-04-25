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
import json
import os
import subprocess
from typing import Optional  # noqa: W0611

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import Connection

# Please keep these variables in alphabetical order.
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.logging_command_executor import LoggingCommandExecutor

GCP_AI_KEY = 'gcp_ai.json'
GCP_AUTOML_KEY = 'gcp_automl.json'
GCP_BIGQUERY_KEY = 'gcp_bigquery.json'
GCP_BIGTABLE_KEY = 'gcp_bigtable.json'
GCP_CLOUD_BUILD_KEY = 'gcp_cloud_build.json'
GCP_CLOUDSQL_KEY = 'gcp_cloudsql.json'
GCP_COMPUTE_KEY = 'gcp_compute.json'
GCP_COMPUTE_SSH_KEY = 'gcp_compute_ssh.json'
GCP_DATACATALOG_KEY = 'gcp_datacatalog.json'
GCP_DATAFLOW_KEY = 'gcp_dataflow.json'
GCP_DATAFUSION_KEY = 'gcp_datafusion.json'
GCP_DATAPROC_KEY = 'gcp_dataproc.json'
GCP_DATASTORE_KEY = 'gcp_datastore.json'
GCP_DLP_KEY = 'gcp_dlp.json'
GCP_FUNCTION_KEY = 'gcp_function.json'
GCP_GCS_KEY = 'gcp_gcs.json'
GCP_GCS_TRANSFER_KEY = 'gcp_gcs_transfer.json'
GCP_GKE_KEY = "gcp_gke.json"
GCP_KMS_KEY = "gcp_kms.json"
GCP_LIFE_SCIENCES_KEY = 'gcp_life_sciences.json'
GCP_MEMORYSTORE = 'gcp_memorystore.json'
GCP_PUBSUB_KEY = "gcp_pubsub.json"
GCP_SECRET_MANAGER_KEY = 'gcp_secret_manager.json'
GCP_SPANNER_KEY = 'gcp_spanner.json'
GCP_STACKDRIVER = 'gcp_stackdriver.json'
GCP_TASKS_KEY = 'gcp_tasks.json'
GCP_WORKFLOWS_KEY = "gcp_workflows.json"
GMP_KEY = 'gmp.json'
G_FIREBASE_KEY = 'g_firebase.json'
GCP_AWS_KEY = 'gcp_aws.json'

KEYPATH_EXTRA = 'extra__google_cloud_platform__key_path'
KEYFILE_DICT_EXTRA = 'extra__google_cloud_platform__keyfile_dict'
SCOPE_EXTRA = 'extra__google_cloud_platform__scope'
PROJECT_EXTRA = 'extra__google_cloud_platform__project'


class GcpAuthenticator(LoggingCommandExecutor):
    """
    Initialises the authenticator.

    :param gcp_key: name of the key to use for authentication (see GCP_*_KEY values)
    :param project_extra: optional extra project parameter passed to google cloud
           connection
    """

    original_account = None  # type: Optional[str]

    def __init__(self, gcp_key: str, project_extra: Optional[str] = None):
        super().__init__()
        self.gcp_key = gcp_key
        self.project_extra = project_extra
        self.project_id = self.get_project_id()
        self.full_key_path = None
        self._set_key_path()

    @staticmethod
    def get_project_id():
        return os.environ.get('GCP_PROJECT_ID')

    def set_key_path_in_airflow_connection(self):
        """
        Set key path in 'google_cloud_default' connection to point to the full
        key path
        :return: None
        """
        session = settings.Session()
        try:
            conn = session.query(Connection).filter(Connection.conn_id == 'google_cloud_default')[0]
            extras = conn.extra_dejson
            extras[KEYPATH_EXTRA] = self.full_key_path
            if extras.get(KEYFILE_DICT_EXTRA):
                del extras[KEYFILE_DICT_EXTRA]
            extras[SCOPE_EXTRA] = 'https://www.googleapis.com/auth/cloud-platform'
            extras[PROJECT_EXTRA] = self.project_extra if self.project_extra else self.project_id
            conn.extra = json.dumps(extras)
            session.commit()
        except BaseException as ex:
            self.log.error('Airflow DB Session error: %s', str(ex))
            session.rollback()
            raise
        finally:
            session.close()

    def set_dictionary_in_airflow_connection(self):
        """
        Set dictionary in 'google_cloud_default' connection to contain content
        of the json service account file.
        :return: None
        """
        session = settings.Session()
        try:
            conn = session.query(Connection).filter(Connection.conn_id == 'google_cloud_default')[0]
            extras = conn.extra_dejson
            with open(self.full_key_path) as path_file:
                content = json.load(path_file)
            extras[KEYFILE_DICT_EXTRA] = json.dumps(content)
            if extras.get(KEYPATH_EXTRA):
                del extras[KEYPATH_EXTRA]
            extras[SCOPE_EXTRA] = 'https://www.googleapis.com/auth/cloud-platform'
            extras[PROJECT_EXTRA] = self.project_extra
            conn.extra = json.dumps(extras)
            session.commit()
        except BaseException as ex:
            self.log.error('Airflow DB Session error: %s', str(ex))
            session.rollback()
            raise
        finally:
            session.close()

    def _set_key_path(self):
        """
        Sets full key path - if GCP_CONFIG_DIR points to absolute
            directory, it tries to find the key in this directory. Otherwise it assumes
            that Airflow is running from the directory where configuration is checked
            out next to airflow directory in config directory
            it tries to find the key folder in the workspace's config
            directory.
        :param : name of the key file to find.
        """
        if "GCP_CONFIG_DIR" in os.environ:
            gcp_config_dir = os.environ["GCP_CONFIG_DIR"]
        else:
            gcp_config_dir = os.path.join(AIRFLOW_MAIN_FOLDER, os.pardir, "config")
        if not os.path.isdir(gcp_config_dir):
            self.log.info("The %s is not a directory", gcp_config_dir)
        key_dir = os.path.join(gcp_config_dir, "keys")
        if not os.path.isdir(key_dir):
            self.log.error("The %s is not a directory", key_dir)
            return
        key_path = os.path.join(key_dir, self.gcp_key)
        if not os.path.isfile(key_path):
            self.log.error("The %s file is missing", key_path)
        self.full_key_path = key_path

    def _validate_key_set(self):
        if self.full_key_path is None:
            raise AirflowException("The gcp_key is not set!")
        if not os.path.isfile(self.full_key_path):
            raise AirflowException(
                "The key {} could not be found. Please copy it to the {} path.".format(
                    self.gcp_key, self.full_key_path
                )
            )

    def gcp_authenticate(self):
        """
        Authenticate with service account specified via key name.
        """
        self._validate_key_set()
        self.log.info("Setting the Google Cloud key to %s", self.full_key_path)
        # Checking if we can authenticate using service account credentials provided
        self.execute_cmd(
            [
                'gcloud',
                'auth',
                'activate-service-account',
                f'--key-file={self.full_key_path}',
                f'--project={self.project_id}',
            ]
        )
        self.set_key_path_in_airflow_connection()

    def gcp_revoke_authentication(self):
        """
        Change default authentication to none - which is not existing one.
        """
        self._validate_key_set()
        self.log.info("Revoking authentication - setting it to none")
        self.execute_cmd(['gcloud', 'config', 'get-value', 'account', f'--project={self.project_id}'])
        self.execute_cmd(['gcloud', 'config', 'set', 'account', 'none', f'--project={self.project_id}'])

    def gcp_store_authentication(self):
        """
        Store authentication as it was originally so it can be restored and revoke
        authentication.
        """
        self._validate_key_set()
        if not GcpAuthenticator.original_account:
            GcpAuthenticator.original_account = self.check_output(
                ['gcloud', 'config', 'get-value', 'account', f'--project={self.project_id}']
            ).decode('utf-8')
            self.log.info("Storing account: to restore it later %s", GcpAuthenticator.original_account)

    def gcp_restore_authentication(self):
        """
        Restore authentication to the original one.
        """
        self._validate_key_set()
        if GcpAuthenticator.original_account:
            self.log.info("Restoring original account stored: %s", GcpAuthenticator.original_account)
            subprocess.call(
                [
                    'gcloud',
                    'config',
                    'set',
                    'account',
                    GcpAuthenticator.original_account,
                    f'--project={self.project_id}',
                ]
            )
        else:
            self.log.info("Not restoring the original Google Cloud account: it is not set")
