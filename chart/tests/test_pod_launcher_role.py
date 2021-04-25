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


class PodLauncherTest(unittest.TestCase):
    @parameterized.expand(
        [
            ("CeleryKubernetesExecutor", True, True, ['scheduler', 'worker']),
            ("KubernetesExecutor", True, True, ['scheduler', 'worker']),
            ("CeleryExecutor", True, True, ['worker']),
            ("LocalExecutor", True, True, ['scheduler']),
            ("LocalExecutor", False, False, []),
        ]
    )
    def test_pod_launcher_role(self, executor, rbac, allow, expected_accounts):
        docs = render_chart(
            values={
                "rbacEnabled": rbac,
                "allowPodLaunching": allow,
                "executor": executor,
            },
            show_only=["templates/rbac/pod-launcher-rolebinding.yaml"],
        )
        if expected_accounts:
            for idx, suffix in enumerate(expected_accounts):
                assert f"RELEASE-NAME-{suffix}" == jmespath.search(f"subjects[{idx}].name", docs[0])
        else:
            assert [] == docs
