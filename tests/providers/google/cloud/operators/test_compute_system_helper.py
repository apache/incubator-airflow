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
import os

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_COMPUTE_KEY, GcpAuthenticator
from tests.test_utils.logging_command_executor import LoggingCommandExecutor

GCE_INSTANCE = os.environ.get('GCE_INSTANCE', 'testinstance')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCE_INSTANCE_GROUP_MANAGER_NAME = os.environ.get('GCE_INSTANCE_GROUP_MANAGER_NAME', 'instance-group-test')
GCE_ZONE = os.environ.get('GCE_ZONE', 'europe-west1-b')
GCE_TEMPLATE_NAME = os.environ.get('GCE_TEMPLATE_NAME', 'instance-template-test')
GCE_NEW_TEMPLATE_NAME = os.environ.get('GCE_NEW_TEMPLATE_NAME', 'instance-template-test-new')


class GCPComputeTestHelper(LoggingCommandExecutor):
    def delete_instance(self):
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                '--verbosity=none',
                'instances',
                'delete',
                GCE_INSTANCE,
                '--zone',
                GCE_ZONE,
            ]
        )

    def create_instance(self):
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                'instances',
                'create',
                GCE_INSTANCE,
                '--zone',
                GCE_ZONE,
            ]
        )

    def delete_instance_group_and_template(self, silent=False):
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                '--verbosity=none',
                'instance-groups',
                'managed',
                'delete',
                GCE_INSTANCE_GROUP_MANAGER_NAME,
                '--zone',
                GCE_ZONE,
            ],
            silent=silent,
        )
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                '--verbosity=none',
                'instance-templates',
                'delete',
                GCE_NEW_TEMPLATE_NAME,
            ],
            silent=silent,
        )
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                '--verbosity=none',
                'instance-templates',
                'delete',
                GCE_TEMPLATE_NAME,
            ],
            silent=silent,
        )

    def create_instance_group_and_template(self):
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                'instance-templates',
                'create',
                GCE_TEMPLATE_NAME,
            ]
        )
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                'instance-groups',
                'managed',
                'create',
                GCE_INSTANCE_GROUP_MANAGER_NAME,
                '--template',
                GCE_TEMPLATE_NAME,
                '--zone',
                GCE_ZONE,
                '--size=1',
            ]
        )
        self.execute_cmd(
            [
                'gcloud',
                'beta',
                'compute',
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                'instance-groups',
                'managed',
                'wait-until-stable',
                GCE_INSTANCE_GROUP_MANAGER_NAME,
                '--zone',
                GCE_ZONE,
            ]
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create or delete GCE instances/instance groups for system tests.'
    )
    parser.add_argument(
        '--action',
        dest='action',
        required=True,
        choices=(
            'create-instance',
            'delete-instance',
            'create-instance-group',
            'delete-instance-group',
            'before-tests',
            'after-tests',
        ),
    )
    action = parser.parse_args().action

    helper = GCPComputeTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_COMPUTE_KEY)
    helper.log.info("Starting action: %s", action)

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            pass
        elif action == 'after-tests':
            pass
        elif action == 'create-instance':
            helper.create_instance()
        elif action == 'delete-instance':
            helper.delete_instance()
        elif action == 'create-instance-group':
            helper.create_instance_group_and_template()
        elif action == 'delete-instance-group':
            helper.delete_instance_group_and_template()
        else:
            raise Exception(f"Unknown action: {action}")
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info("Finishing action: %s", action)
