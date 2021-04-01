#!/usr/bin/env python3
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
"""
Module to check pre-commit hook names for length
"""
import argparse
import sys

import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore[no-redef]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-length', help="Max length for hook names")
    args = parser.parse_args()
    max_length = int(args.max_length) or 70

    retval = 0

    with open('.pre-commit-config.yaml', 'rb') as f:
        content = yaml.load(f, SafeLoader)
        errors = get_errors(content, max_length)
    if errors:
        retval = 1
        print(f"found pre-commit hook names with length exceeding {max_length} characters")
        print("move add details in description if necessary")
    for hook_name in errors:
        print(f"    * '{hook_name}': length {len(hook_name)}")
    return retval


def get_errors(content, max_length):
    errors = []
    for repo in content['repos']:
        for hook in repo['hooks']:
            if 'name' not in hook:
                continue
            name = hook['name']
            if len(name) > max_length:
                errors.append(name)
    return errors


if __name__ == '__main__':
    sys.exit(main())
