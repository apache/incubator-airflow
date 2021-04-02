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
from __future__ import absolute_import

import re
import os
from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.dag_processing import list_py_file_paths


class ChainBetweenDAGAndOperatorNotAllowedRule(BaseRule):
    title = "Chain between DAG and operator not allowed."

    description = "Assigning task to a DAG using bitwise shift (bit-shift) operators are no longer supported."

    def _change_info(self, file_path, line_number):
        return "{} Affected file: {} (line {})".format(
            self.title, file_path, line_number
        )

    def _check_file(self, file_path):
        problems = []
        with open(file_path, "r") as file_pointer:
            try:
                lines = file_pointer.readlines()

                python_space = r"\s*\\?\s*\n?\s*"
                likely_dag_class_names = r"(?:DAG|models\.DAG|airflow\.models\.DAG)"
                # Find all the dag variable names.
                dag_vars = re.findall(
                    r"([A-Za-z0-9_]+){}={}{}\(".format(python_space, python_space, likely_dag_class_names),
                    "".join(lines))
                history = ""
                for line_number, line in enumerate(lines, 1):
                    # Someone could have put the bitshift operator on a different line than the dag they
                    # were using it on, so search for dag >> or << dag in all previous lines that did
                    # not contain a logged issue.
                    history += line
                    matches = [
                        re.search(r"DAG\([^\)]+\){}>>".format(python_space), history),
                        re.search(r"<<{}{}\(".format(python_space, likely_dag_class_names), history)
                    ]
                    for dag_var in dag_vars:
                        matches.extend([
                            re.search(r"(\s|^){}{}>>".format(dag_var, python_space), history),
                            re.search(r"<<\s*{}{}".format(python_space, dag_var), history),
                        ])
                    if any(matches):
                        problems.append(self._change_info(file_path, line_number))
                        # If we found a problem, clear our history so we don't re-log the problem
                        # on the next line.
                        history = ""
            except UnicodeDecodeError:
                problems.append("Unable to read python file {}".format(file_path))
        return problems

    def check(self):
        dag_folder = conf.get("core", "dags_folder")
        file_paths = list_py_file_paths(directory=dag_folder, include_examples=False)
        file_paths = [file for file in file_paths if os.path.splitext(file)[1] == ".py"]
        problems = []
        for file_path in file_paths:
            problems.extend(self._check_file(file_path))
        return problems
