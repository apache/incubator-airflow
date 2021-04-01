#!/usr/bin/env python
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
import argparse
import errno
import json
import os
import time
from os.path import expanduser
from threading import Thread
from typing import Optional
from urllib.parse import urlsplit

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_CLOUDSQL_KEY, GcpAuthenticator
from tests.test_utils.logging_command_executor import LoggingCommandExecutor

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'europe-west1')

GCSQL_POSTGRES_SERVER_CA_FILE = os.environ.get('GCSQL_POSTGRES_SERVER_CA_FILE', ".key/postgres-server-ca.pem")
GCSQL_POSTGRES_CLIENT_CERT_FILE = os.environ.get(
    'GCSQL_POSTGRES_CLIENT_CERT_FILE', ".key/postgres-client-cert.pem"
)
GCSQL_POSTGRES_CLIENT_KEY_FILE = os.environ.get(
    'GCSQL_POSTGRES_CLIENT_KEY_FILE', ".key/postgres-client-key.pem"
)
GCSQL_POSTGRES_PUBLIC_IP_FILE = os.environ.get('GCSQL_POSTGRES_PUBLIC_IP_FILE', ".key/postgres-ip.env")
GCSQL_POSTGRES_USER = os.environ.get('GCSQL_POSTGRES_USER', 'postgres_user')
GCSQL_POSTGRES_DATABASE_NAME = os.environ.get('GCSQL_POSTGRES_DATABASE_NAME', 'postgresdb')
GCSQL_MYSQL_CLIENT_CERT_FILE = os.environ.get('GCSQL_MYSQL_CLIENT_CERT_FILE', ".key/mysql-client-cert.pem")
GCSQL_MYSQL_CLIENT_KEY_FILE = os.environ.get('GCSQL_MYSQL_CLIENT_KEY_FILE', ".key/mysql-client-key.pem")
GCSQL_MYSQL_SERVER_CA_FILE = os.environ.get('GCSQL_MYSQL_SERVER_CA_FILE', ".key/mysql-server-ca.pem")
GCSQL_MYSQL_PUBLIC_IP_FILE = os.environ.get('GCSQL_MYSQL_PUBLIC_IP_FILE', ".key/mysql-ip.env")
GCSQL_MYSQL_USER = os.environ.get('GCSQL_MYSQL_USER', 'mysql_user')
GCSQL_MYSQL_DATABASE_NAME = os.environ.get('GCSQL_MYSQL_DATABASE_NAME', 'mysqldb')

GCSQL_MYSQL_EXPORT_URI = os.environ.get('GCSQL_MYSQL_EXPORT_URI', 'gs://bucketName/fileName')
DB_VERSION_MYSQL = 'MYSQL_5_7'
DV_VERSION_POSTGRES = 'POSTGRES_9_6'

HOME_DIR = expanduser("~")

TEARDOWN_LOCK_FILE = "/tmp/do_not_teardown_cloudsql_database"
TEARDOWN_LOCK_FILE_QUERY = "/tmp/do_not_teardown_cloudsql_database_query"

QUERY_SUFFIX = "-query"


def get_absolute_path(path):
    if path.startswith("/"):
        return path
    else:
        return os.path.join(HOME_DIR, path)


server_ca_file_postgres = get_absolute_path(GCSQL_POSTGRES_SERVER_CA_FILE)
client_cert_file_postgres = get_absolute_path(GCSQL_POSTGRES_CLIENT_CERT_FILE)
client_key_file_postgres = get_absolute_path(GCSQL_POSTGRES_CLIENT_KEY_FILE)

server_ca_file_mysql = get_absolute_path(GCSQL_MYSQL_SERVER_CA_FILE)
client_cert_file_mysql = get_absolute_path(GCSQL_MYSQL_CLIENT_CERT_FILE)
client_key_file_mysql = get_absolute_path(GCSQL_MYSQL_CLIENT_KEY_FILE)


def get_postgres_instance_name(instance_suffix=''):
    if instance_suffix is None:
        return None
    return os.environ.get('GCSQL_POSTGRES_INSTANCE_NAME', 'testpostgres') + instance_suffix


def get_mysql_instance_name(instance_suffix=''):
    if instance_suffix is None:
        return None
    return os.environ.get('GCSQL_MYSQL_INSTANCE_NAME', 'testmysql') + instance_suffix


class CloudSqlQueryTestHelper(LoggingCommandExecutor):
    def create_instances(
        self, instance_suffix='', failover_instance_suffix=None, master_instance_suffix=None
    ):
        thread_mysql = Thread(
            target=lambda: self.__create_instance(
                get_mysql_instance_name(instance_suffix),
                DB_VERSION_MYSQL,
                master_instance_name=get_mysql_instance_name(master_instance_suffix),
                failover_replica_name=get_mysql_instance_name(failover_instance_suffix),
            )
        )
        thread_postgres = Thread(
            target=lambda: self.__create_instance(
                get_postgres_instance_name(instance_suffix),
                DV_VERSION_POSTGRES,
                master_instance_name=get_postgres_instance_name(master_instance_suffix),
                failover_replica_name=get_postgres_instance_name(failover_instance_suffix),
            )
        )
        thread_mysql.start()
        thread_postgres.start()
        thread_mysql.join()
        thread_postgres.join()

    def delete_instances(self, instance_suffix='', master_instance_suffix=None):
        thread_mysql = Thread(
            target=lambda: self.__delete_instance(
                get_mysql_instance_name(instance_suffix),
                master_instance_name=get_mysql_instance_name(master_instance_suffix),
            )
        )
        thread_postgres = Thread(
            target=lambda: self.__delete_instance(
                get_postgres_instance_name(instance_suffix),
                master_instance_name=get_mysql_instance_name(master_instance_suffix),
            )
        )
        thread_mysql.start()
        thread_postgres.start()
        thread_mysql.join()
        thread_postgres.join()

    def get_ip_addresses(self, instance_suffix):
        with open(GCSQL_MYSQL_PUBLIC_IP_FILE, "w") as file:
            ip_address = self.__get_ip_address(
                get_mysql_instance_name(instance_suffix), 'GCSQL_MYSQL_PUBLIC_IP'
            )
            file.write(ip_address)
        with open(GCSQL_POSTGRES_PUBLIC_IP_FILE, "w") as file:
            ip_address = self.__get_ip_address(
                get_postgres_instance_name(instance_suffix), 'GCSQL_POSTGRES_PUBLIC_IP'
            )
            file.write(ip_address)

    def raise_database_exception(self, database):
        raise Exception(
            "The {database} instance does not exist. Make sure to run  "
            "`python {f} --action=before-tests` before running the test"
            " (and remember to run `python {f} --action=after-tests` "
            "after you are done.".format(f=__file__, database=database)
        )

    def check_if_instances_are_up(self, instance_suffix=''):
        res_postgres = self.execute_cmd(
            [
                'gcloud',
                'sql',
                'instances',
                'describe',
                get_postgres_instance_name(instance_suffix),
                f"--project={GCP_PROJECT_ID}",
            ]
        )
        if res_postgres != 0:
            self.raise_database_exception('postgres')
        res_postgres = self.execute_cmd(
            [
                'gcloud',
                'sql',
                'instances',
                'describe',
                get_postgres_instance_name(instance_suffix),
                f"--project={GCP_PROJECT_ID}",
            ]
        )
        if res_postgres != 0:
            self.raise_database_exception('mysql')

    def authorize_address(self, instance_suffix=''):
        ip_address = self.__get_my_public_ip()
        self.log.info('Authorizing access from IP: %s', ip_address)
        postgres_thread = Thread(
            target=lambda: self.execute_cmd(
                [
                    'gcloud',
                    'sql',
                    'instances',
                    'patch',
                    get_postgres_instance_name(instance_suffix),
                    '--quiet',
                    f"--authorized-networks={ip_address}",
                    f"--project={GCP_PROJECT_ID}",
                ]
            )
        )
        mysql_thread = Thread(
            target=lambda: self.execute_cmd(
                [
                    'gcloud',
                    'sql',
                    'instances',
                    'patch',
                    get_mysql_instance_name(instance_suffix),
                    '--quiet',
                    f"--authorized-networks={ip_address}",
                    f"--project={GCP_PROJECT_ID}",
                ]
            )
        )
        postgres_thread.start()
        mysql_thread.start()
        postgres_thread.join()
        mysql_thread.join()

    def setup_instances(self, instance_suffix=''):
        mysql_thread = Thread(
            target=lambda: self.__setup_instance_and_certs(
                get_mysql_instance_name(instance_suffix),
                DB_VERSION_MYSQL,
                server_ca_file_mysql,
                client_key_file_mysql,
                client_cert_file_mysql,
                GCSQL_MYSQL_DATABASE_NAME,
                GCSQL_MYSQL_USER,
            )
        )
        postgres_thread = Thread(
            target=lambda: self.__setup_instance_and_certs(
                get_postgres_instance_name(instance_suffix),
                DV_VERSION_POSTGRES,
                server_ca_file_postgres,
                client_key_file_postgres,
                client_cert_file_postgres,
                GCSQL_POSTGRES_DATABASE_NAME,
                GCSQL_POSTGRES_USER,
            )
        )
        mysql_thread.start()
        postgres_thread.start()
        mysql_thread.join()
        postgres_thread.join()
        self.get_ip_addresses(instance_suffix)
        self.authorize_address(instance_suffix)

    def delete_service_account_acls(self):
        self.__delete_service_accounts_acls()

    def __create_instance(
        self, instance_name, db_version, failover_replica_name=None, master_instance_name=None
    ):
        self.log.info(
            'Creating a test %s instance "%s"... Failover(%s), Master(%s)',
            db_version,
            instance_name,
            failover_replica_name,
            master_instance_name,
        )
        try:
            create_instance_opcode = self.__create_sql_instance(
                instance_name,
                db_version,
                failover_replica_name=failover_replica_name,
                master_instance_name=master_instance_name,
            )
            if create_instance_opcode:  # return code 1, some error occurred
                operation_name = self.__get_operation_name(instance_name)
                self.log.info('Waiting for operation: %s ...', operation_name)
                self.__wait_for_create(operation_name)
                self.log.info('... Done.')

            self.log.info('... Done creating a test %s instance "%s"!\n', db_version, instance_name)
        except Exception as ex:
            self.log.error('Exception occurred. Aborting creating a test instance.\n\n%s', ex)
            raise ex

    def __delete_service_accounts_acls(self):
        export_bucket_split = urlsplit(GCSQL_MYSQL_EXPORT_URI)
        export_bucket_name = export_bucket_split[1]  # netloc (bucket)
        self.log.info('Deleting temporary service accounts from bucket "%s"...', export_bucket_name)
        all_permissions = self.check_output(
            [
                'gsutil',
                'iam',
                'get',
                f"gs://{export_bucket_name}",
                f"--project={GCP_PROJECT_ID}",
            ]
        )
        all_permissions_dejson = json.loads(all_permissions.decode("utf-8"))
        for binding in all_permissions_dejson['bindings']:
            if binding['role'] != 'roles/storage.legacyBucketWriter':
                continue

            for member in binding['members']:
                if member.startswith('serviceAccount:gcp-storage-account'):
                    self.log.info("Skip removing member %s", member)
                    continue

                self.log.info("Remove member: %s", member)
                member_type, member_email = member.split(':')

                if member_type != 'serviceAccount':
                    self.log.warning(
                        "Skip removing member %s as the type %s is not service account", member, member_type
                    )
                self.execute_cmd(['gsutil', 'acl', 'ch', '-d', member_email, f"gs://{export_bucket_name}"])

    @staticmethod
    def set_ip_addresses_in_env():
        CloudSqlQueryTestHelper.__set_ip_address_in_env(GCSQL_MYSQL_PUBLIC_IP_FILE)
        CloudSqlQueryTestHelper.__set_ip_address_in_env(GCSQL_POSTGRES_PUBLIC_IP_FILE)

    @staticmethod
    def __set_ip_address_in_env(file_name):
        if os.path.exists(file_name):
            with open(file_name) as file:
                env, ip_address = file.read().split("=")
                os.environ[env] = ip_address

    def __setup_instance_and_certs(
        self,
        instance_name,
        db_version,
        server_ca_file,
        client_key_file,
        client_cert_file,
        db_name,
        db_username,
    ):
        self.log.info('Setting up a test %s instance "%s"...', db_version, instance_name)
        try:
            self.__remove_keys_and_certs([server_ca_file, client_key_file, client_cert_file])

            self.__wait_for_operations(instance_name)
            self.__write_to_file(server_ca_file, self.__get_server_ca_cert(instance_name))
            client_cert_name = 'client-cert-name'
            self.__wait_for_operations(instance_name)
            self.__delete_client_cert(instance_name, client_cert_name)
            self.__wait_for_operations(instance_name)
            self.__create_client_cert(instance_name, client_key_file, client_cert_name)
            self.__wait_for_operations(instance_name)
            self.__write_to_file(client_cert_file, self.__get_client_cert(instance_name, client_cert_name))
            self.__wait_for_operations(instance_name)
            self.__wait_for_operations(instance_name)
            self.__create_user(instance_name, db_username)
            self.__wait_for_operations(instance_name)
            self.__delete_db(instance_name, db_name)
            self.__create_db(instance_name, db_name)
            self.log.info('... Done setting up a test %s instance "%s"!\n', db_version, instance_name)
        except Exception as ex:
            self.log.error('Exception occurred. Aborting setting up test instance and certs.\n\n%s', ex)
            raise ex

    def __delete_instance(self, instance_name: str, master_instance_name: Optional[str]) -> None:
        if master_instance_name is not None:
            self.__wait_for_operations(master_instance_name)
        self.__wait_for_operations(instance_name)
        self.log.info('Deleting Cloud SQL instance "%s"...', instance_name)
        self.execute_cmd(['gcloud', 'sql', 'instances', 'delete', instance_name, '--quiet'])
        self.log.info('... Done.')

    def __get_my_public_ip(self):
        return self.check_output(['curl', 'https://ipinfo.io/ip']).decode('utf-8').strip()

    def __create_sql_instance(
        self,
        instance_name: str,
        db_version: str,
        master_instance_name: Optional[str],
        failover_replica_name: Optional[str],
    ) -> int:
        cmd = [
            'gcloud',
            'sql',
            'instances',
            'create',
            instance_name,
            '--region',
            GCP_LOCATION,
            '--project',
            GCP_PROJECT_ID,
            '--database-version',
            db_version,
            '--tier',
            'db-f1-micro',
        ]
        if master_instance_name:
            cmd.extend(['--master-instance-name', master_instance_name])
            self.__wait_for_operations(master_instance_name)

        if failover_replica_name and failover_replica_name.find('mysql') >= 0:
            if failover_replica_name:
                cmd.extend(['--failover-replica-name', failover_replica_name])
                cmd.extend(['--enable-bin-log'])
        return self.execute_cmd(cmd)

    def __get_server_ca_cert(self, instance_name: str) -> bytes:
        self.log.info('Getting server CA cert for "%s"...', instance_name)
        output = self.check_output(
            ['gcloud', 'sql', 'instances', 'describe', instance_name, '--format=value(serverCaCert.cert)']
        )
        self.log.info('... Done.')
        return output

    def __get_client_cert(self, instance_name: str, client_cert_name: str) -> bytes:
        self.log.info('Getting client cert for "%s"...', instance_name)
        output = self.check_output(
            [
                'gcloud',
                'sql',
                'ssl',
                'client-certs',
                'describe',
                client_cert_name,
                '-i',
                instance_name,
                '--format=get(cert)',
            ]
        )
        self.log.info('... Done.')
        return output

    def __create_user(self, instance_name: str, username: str) -> None:
        self.log.info('Creating user "%s" in Cloud SQL instance "%s"...', username, instance_name)
        self.execute_cmd(
            [
                'gcloud',
                'sql',
                'users',
                'create',
                username,
                '-i',
                instance_name,
                '--host',
                '%',
                '--password',
                'JoxHlwrPzwch0gz9',
                '--quiet',
            ]
        )
        self.log.info('... Done.')

    def __delete_db(self, instance_name: str, db_name: str) -> None:
        self.log.info('Deleting database "%s" in Cloud SQL instance "%s"...', db_name, instance_name)
        self.execute_cmd(['gcloud', 'sql', 'databases', 'delete', db_name, '-i', instance_name, '--quiet'])
        self.log.info('... Done.')

    def __create_db(self, instance_name: str, db_name: str) -> None:
        self.log.info('Creating database "%s" in Cloud SQL instance "%s"...', db_name, instance_name)
        self.execute_cmd(['gcloud', 'sql', 'databases', 'create', db_name, '-i', instance_name, '--quiet'])
        self.log.info('... Done.')

    def __write_to_file(self, filepath: str, content: bytes) -> None:
        # https://stackoverflow.com/a/12517490
        self.log.info("Checking file under: %s", filepath)
        if not os.path.exists(os.path.dirname(filepath)):
            self.log.info("File doesn't exits. Creating dir...")
            try:
                os.makedirs(os.path.dirname(filepath))
            except OSError as exc:  # Guard against race condition
                self.log.error("Error while creating dir.")
                if exc.errno != errno.EEXIST:
                    raise
        self.log.info("... Done. Dir created.")

        with open(filepath, "w") as file:
            file.write(str(content.decode('utf-8')))
        self.log.info('Written file in: %s', filepath)

    def __remove_keys_and_certs(self, filepaths):
        if not filepaths:
            return
        self.log.info('Removing client keys and certs...')

        for filepath in filepaths:
            if os.path.exists(filepath):
                os.remove(filepath)
        self.log.info('Done ...')

    def __delete_client_cert(self, instance_name, common_name):
        self.log.info('Deleting client key and cert for "%s"...', instance_name)
        self.execute_cmd(
            [
                'gcloud',
                'sql',
                'ssl',
                'client-certs',
                'delete',
                common_name,
                '--instance',
                instance_name,
                '--quiet',
            ]
        )
        self.log.info('... Done.')

    def __create_client_cert(self, instance_name, client_key_file, common_name):
        self.log.info('Creating client key and cert for "%s"...', instance_name)
        try:
            os.remove(client_key_file)
        except OSError:
            pass
        self.execute_cmd(
            [
                'gcloud',
                'sql',
                'ssl',
                'client-certs',
                'create',
                common_name,
                client_key_file,
                '-i',
                instance_name,
            ]
        )
        self.log.info('... Done.')

    def __get_operation_name(self, instance_name: str) -> str:
        op_name_bytes = self.check_output(
            ['gcloud', 'sql', 'operations', 'list', '--instance', instance_name, '--format=get(name)']
        )
        return op_name_bytes.decode('utf-8').strip()

    def __print_operations(self, operations):
        self.log.info("\n==== OPERATIONS >>>>")
        self.log.info(operations)
        self.log.info("<<<< OPERATIONS ====\n")

    def __wait_for_operations(self, instance_name: str) -> None:
        while True:
            operations = self.__get_operations(instance_name)
            self.__print_operations(operations)
            if "RUNNING" in operations:
                self.log.info("Found a running operation %s. Sleeping 5s before retrying...", operations)
                time.sleep(5)
            else:
                break

    def __get_ip_address(self, instance_name: str, env_var: str) -> str:
        ip_address = (
            self.check_output(
                [
                    'gcloud',
                    'sql',
                    'instances',
                    'describe',
                    instance_name,
                    '--format=get(ipAddresses[0].ipAddress)',
                ]
            )
            .decode('utf-8')
            .strip()
        )
        os.environ[env_var] = ip_address
        return f"{env_var}={ip_address}"

    def __get_operations(self, instance_name: str) -> str:
        op_name_bytes = self.check_output(
            ['gcloud', 'sql', 'operations', 'list', '-i', instance_name, '--format=get(NAME,TYPE,STATUS)']
        )
        return op_name_bytes.decode('utf-8').strip()

    def __wait_for_create(self, operation_name: str) -> None:
        self.execute_cmd(
            ['gcloud', 'beta', 'sql', 'operations', 'wait', '--project', GCP_PROJECT_ID, operation_name]
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create or delete Cloud SQL instances for system tests.')
    parser.add_argument(
        '--action',
        required=True,
        choices=(
            'create',
            'delete',
            'setup-instances',
            'create-query',
            'delete-query',
            'delete-service-accounts-acls',
        ),
    )
    action = parser.parse_args().action

    helper = CloudSqlQueryTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_CLOUDSQL_KEY)
    helper.log.info("Starting action: %s", action)

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'create':
            helper.create_instances(failover_instance_suffix='-failover-replica')
            helper.create_instances(instance_suffix="-read-replica", master_instance_suffix='')
            helper.create_instances(instance_suffix="2")
            helper.create_instances(instance_suffix=QUERY_SUFFIX)
            helper.setup_instances(instance_suffix=QUERY_SUFFIX)
            with open(TEARDOWN_LOCK_FILE, "wt") as teardown_file:
                teardown_file.write("")

        elif action == 'delete':
            helper.delete_instances(instance_suffix="2")
            helper.delete_instances(instance_suffix="-failover-replica", master_instance_suffix='')
            helper.delete_instances(instance_suffix="-read-replica", master_instance_suffix='')
            helper.delete_instances()
            try:
                os.remove(TEARDOWN_LOCK_FILE)
            except FileNotFoundError:
                pass
        elif action == 'create-query':
            helper.create_instances(instance_suffix=QUERY_SUFFIX)
            helper.setup_instances(instance_suffix=QUERY_SUFFIX)
            with open(TEARDOWN_LOCK_FILE_QUERY, "wt") as teardown_file:
                teardown_file.write("")

        elif action == 'delete-query':
            helper.delete_instances(instance_suffix=QUERY_SUFFIX)
            try:
                os.remove(TEARDOWN_LOCK_FILE_QUERY)
            except FileNotFoundError:
                pass

        elif action == 'setup-instances':
            helper.setup_instances()
            helper.setup_instances(instance_suffix="2")
        elif action == 'delete-service-accounts-acls':
            helper.delete_service_account_acls()
        else:
            raise Exception(f"Unknown action: {action}")
    finally:
        gcp_authenticator.gcp_restore_authentication()
    helper.log.info("Finishing action: %s", action)
