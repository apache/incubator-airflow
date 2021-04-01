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
import re
import unittest

from airflow.providers_manager import ProvidersManager

ALL_PROVIDERS = [
    'apache-airflow-providers-airbyte',
    'apache-airflow-providers-amazon',
    'apache-airflow-providers-apache-beam',
    'apache-airflow-providers-apache-cassandra',
    'apache-airflow-providers-apache-druid',
    'apache-airflow-providers-apache-hdfs',
    'apache-airflow-providers-apache-hive',
    'apache-airflow-providers-apache-kylin',
    'apache-airflow-providers-apache-livy',
    'apache-airflow-providers-apache-pig',
    'apache-airflow-providers-apache-pinot',
    'apache-airflow-providers-apache-spark',
    'apache-airflow-providers-apache-sqoop',
    'apache-airflow-providers-celery',
    'apache-airflow-providers-cloudant',
    'apache-airflow-providers-cncf-kubernetes',
    'apache-airflow-providers-databricks',
    'apache-airflow-providers-datadog',
    'apache-airflow-providers-dingding',
    'apache-airflow-providers-discord',
    'apache-airflow-providers-docker',
    'apache-airflow-providers-elasticsearch',
    'apache-airflow-providers-exasol',
    'apache-airflow-providers-facebook',
    'apache-airflow-providers-ftp',
    'apache-airflow-providers-google',
    'apache-airflow-providers-grpc',
    'apache-airflow-providers-hashicorp',
    'apache-airflow-providers-http',
    'apache-airflow-providers-imap',
    'apache-airflow-providers-jdbc',
    'apache-airflow-providers-jenkins',
    'apache-airflow-providers-jira',
    'apache-airflow-providers-microsoft-azure',
    'apache-airflow-providers-microsoft-mssql',
    'apache-airflow-providers-microsoft-winrm',
    'apache-airflow-providers-mongo',
    'apache-airflow-providers-mysql',
    'apache-airflow-providers-neo4j',
    'apache-airflow-providers-odbc',
    'apache-airflow-providers-openfaas',
    'apache-airflow-providers-opsgenie',
    'apache-airflow-providers-oracle',
    'apache-airflow-providers-pagerduty',
    'apache-airflow-providers-papermill',
    'apache-airflow-providers-plexus',
    'apache-airflow-providers-postgres',
    'apache-airflow-providers-presto',
    'apache-airflow-providers-qubole',
    'apache-airflow-providers-redis',
    'apache-airflow-providers-salesforce',
    'apache-airflow-providers-samba',
    'apache-airflow-providers-segment',
    'apache-airflow-providers-sendgrid',
    'apache-airflow-providers-sftp',
    'apache-airflow-providers-singularity',
    'apache-airflow-providers-slack',
    'apache-airflow-providers-snowflake',
    'apache-airflow-providers-sqlite',
    'apache-airflow-providers-ssh',
    'apache-airflow-providers-tableau',
    'apache-airflow-providers-telegram',
    'apache-airflow-providers-vertica',
    'apache-airflow-providers-yandex',
    'apache-airflow-providers-zendesk',
]

CONNECTIONS_LIST = [
    'aws',
    'azure',
    'azure_batch',
    'azure_container_instances',
    'azure_cosmos',
    'azure_data_explorer',
    'azure_data_factory',
    'azure_data_lake',
    'cassandra',
    'cloudant',
    'databricks',
    'dataprep',
    'docker',
    'druid',
    'elasticsearch',
    'emr',
    'exasol',
    'facebook_social',
    'ftp',
    'gcpcloudsql',
    'gcpcloudsqldb',
    'gcpssh',
    'google_cloud_platform',
    'grpc',
    'hdfs',
    'hive_cli',
    'hive_metastore',
    'hiveserver2',
    'http',
    'imap',
    'jdbc',
    'jenkins',
    'jira',
    'kubernetes',
    'livy',
    'mongo',
    'mssql',
    'mysql',
    'neo4j',
    'odbc',
    'oracle',
    'pig_cli',
    'postgres',
    'presto',
    'qubole',
    'redis',
    's3',
    'samba',
    'segment',
    'sftp',
    'snowflake',
    'spark',
    'spark_jdbc',
    'spark_sql',
    'sqlite',
    'sqoop',
    'ssh',
    'tableau',
    'vault',
    'vertica',
    'wasb',
    'yandexcloud',
]

CONNECTION_FORM_WIDGETS = [
    'extra__google_cloud_platform__key_path',
    'extra__google_cloud_platform__keyfile_dict',
    'extra__google_cloud_platform__num_retries',
    'extra__google_cloud_platform__project',
    'extra__google_cloud_platform__scope',
    'extra__grpc__auth_type',
    'extra__grpc__credential_pem_file',
    'extra__grpc__scopes',
    'extra__jdbc__drv_clsname',
    'extra__jdbc__drv_path',
    'extra__kubernetes__in_cluster',
    'extra__kubernetes__kube_config',
    'extra__kubernetes__kube_config_path',
    'extra__kubernetes__namespace',
    'extra__snowflake__account',
    'extra__snowflake__aws_access_key_id',
    'extra__snowflake__aws_secret_access_key',
    'extra__snowflake__database',
    'extra__snowflake__region',
    'extra__snowflake__warehouse',
    'extra__yandexcloud__folder_id',
    'extra__yandexcloud__oauth',
    'extra__yandexcloud__public_ssh_key',
    'extra__yandexcloud__service_account_json',
    'extra__yandexcloud__service_account_json_path',
]

CONNECTIONS_WITH_FIELD_BEHAVIOURS = [
    'cloudant',
    'docker',
    'gcpssh',
    'google_cloud_platform',
    'jdbc',
    'kubernetes',
    'qubole',
    'sftp',
    'snowflake',
    'spark',
    'ssh',
    'yandexcloud',
]

EXTRA_LINKS = [
    'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink',
    'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink',
    'airflow.providers.google.cloud.operators.mlengine.AIPlatformConsoleLink',
    'airflow.providers.qubole.operators.qubole.QDSLink',
]


class TestProviderManager(unittest.TestCase):
    def test_providers_are_loaded(self):
        provider_manager = ProvidersManager()
        provider_list = list(provider_manager.providers.keys())
        # No need to sort the list - it should be sorted alphabetically !
        for provider in provider_list:
            package_name = provider_manager.providers[provider][1]['package-name']
            version = provider_manager.providers[provider][0]
            assert re.search(r'[0-9]*\.[0-9]*\.[0-9]*.*', version)
            assert package_name == provider
        assert ALL_PROVIDERS == provider_list

    def test_hooks(self):
        provider_manager = ProvidersManager()
        connections_list = list(provider_manager.hooks.keys())
        assert CONNECTIONS_LIST == connections_list

    def test_connection_form_widgets(self):
        provider_manager = ProvidersManager()
        connections_form_widgets = list(provider_manager.connection_form_widgets.keys())
        assert CONNECTION_FORM_WIDGETS == connections_form_widgets

    def test_field_behaviours(self):
        provider_manager = ProvidersManager()
        connections_with_field_behaviours = list(provider_manager.field_behaviours.keys())
        assert CONNECTIONS_WITH_FIELD_BEHAVIOURS == connections_with_field_behaviours

    def test_extra_links(self):
        provider_manager = ProvidersManager()
        extra_link_class_names = list(provider_manager.extra_links_class_names)
        assert EXTRA_LINKS == extra_link_class_names
