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
from contextlib import contextmanager


@contextmanager
def with_group(title):
    """
    If used in GitHub Action, creates an expandable group in the GitHub Action log.
    Otherwise, display simple text groups.

    For more information, see:
    https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#grouping-log-lines
    """
    if os.environ.get('GITHUB_ACTIONS', 'false') != "true":
        print("#" * 20, title, "#" * 20)
        yield
        return
    print(f"::group::{title}")
    yield
    print("\033[0m")
    print("::endgroup::")
