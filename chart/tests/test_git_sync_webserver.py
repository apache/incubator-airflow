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

import unittest

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class GitSyncWebserverTest(unittest.TestCase):
    def test_should_add_dags_volume_to_the_webserver_if_git_sync_and_persistence_is_enabled(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])

    def test_should_add_dags_volume_to_the_webserver_if_git_sync_is_enabled_and_persistence_is_disabled(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": False}}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])

    def test_should_add_git_sync_container_to_webserver_if_persistence_is_not_enabled_but_git_sync_is(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {"enabled": True, "containerName": "git-sync"},
                    "persistence": {"enabled": False},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "git-sync" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

    def test_should_have_service_account_defined(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "RELEASE-NAME-webserver" == jmespath.search("spec.template.spec.serviceAccountName", docs[0])

    @parameterized.expand([(True,), (False,)])
    def test_git_sync_with_exclude_webserver(self, exclude_webserver):
        """
        If that dags.gitSync.excludeWebserver=True - git sync related containers, volume mounts & volumes
        are not created.
        """
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {"enabled": True, "excludeWebserver": exclude_webserver},
                    "persistence": {"enabled": False},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        containers_names = [
            container["name"] for container in jmespath.search("spec.template.spec.containers", docs[0])
        ]

        volume_mount_names = [
            vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        ]

        volume_names = [volume["name"] for volume in jmespath.search("spec.template.spec.volumes", docs[0])]

        if exclude_webserver:
            assert "git-sync" not in containers_names
            assert "dags" not in volume_mount_names
            assert "dags" not in volume_names
        else:
            assert "git-sync" in containers_names
            assert "dags" in volume_mount_names
            assert "dags" in volume_names
