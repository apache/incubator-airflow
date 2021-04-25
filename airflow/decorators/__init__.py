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

from typing import Callable, Optional

from airflow.decorators.python import python_task
from airflow.models.dag import dag  # noqa # pylint: disable=unused-import


class _TaskDecorator:
    def __call__(
        self, python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
    ):
        """
        Python operator decorator. Wraps a function into an Airflow operator.
        Accepts kwargs for operator kwarg. This decorator can be reused in a single DAG.

        :param python_callable: Function to decorate
        :type python_callable: Optional[Callable]
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        """
        return self.python(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)

    @staticmethod
    def python(python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs):
        """
        Python operator decorator. Wraps a function into an Airflow operator.
        Accepts kwargs for operator kwarg. This decorator can be reused in a single DAG.

        :param python_callable: Function to decorate
        :type python_callable: Optional[Callable]
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        """
        return python_task(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)


task = _TaskDecorator()
