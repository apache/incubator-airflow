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

from typing import Callable, Dict, Iterable, List, Optional, Union

from airflow.decorators.python import python_task
from airflow.decorators.python_virtualenv import _virtualenv_task
from airflow.decorators.task_group import task_group  # noqa # pylint: disable=unused-import
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

    @staticmethod
    def virtualenv(
        python_callable: Optional[Callable] = None,
        multiple_outputs: Optional[bool] = None,
        requirements: Optional[Iterable[str]] = None,
        python_version: Optional[Union[str, int, float]] = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        string_args: Optional[Iterable[str]] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Allows one to run a function in a virtualenv that is
        created and destroyed automatically (with certain caveats).

        The function must be defined using def, and not be
        part of a class. All imports must happen inside the function
        and no variables outside of the scope may be referenced. A global scope
        variable named virtualenv_string_args will be available (populated by
        string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
        can use a return value.
        Note that if your virtualenv runs in a different Python major version than Airflow,
        you cannot use return values, op_args, op_kwargs, or use any macros that are being provided to
        Airflow through plugins. You can use string_args though.

        .. seealso::
            For more information on how to use this operator, take a look at the guide:
            :ref:`howto/operator:PythonVirtualenvOperator`

        :param python_callable: A python function with no references to outside variables,
            defined with def, which will be run in a virtualenv
        :type python_callable: function
        :param multiple_outputs: if set, function return value will be
            unrolled to multiple XCom values. List/Tuples will unroll to xcom values
            with index as key. Dict will unroll to xcom values with keys as XCom keys.
            Defaults to False.
        :type multiple_outputs: bool
        :param requirements: A list of requirements as specified in a pip install command
        :type requirements: list[str]
        :param python_version: The Python version to run the virtualenv with. Note that
            both 2 and 2.7 are acceptable forms.
        :type python_version: Optional[Union[str, int, float]]
        :param use_dill: Whether to use dill to serialize
            the args and result (pickle is default). This allow more complex types
            but requires you to include dill in your requirements.
        :type use_dill: bool
        :param system_site_packages: Whether to include
            system_site_packages in your virtualenv.
            See virtualenv documentation for more information.
        :type system_site_packages: bool
        :param op_args: A list of positional arguments to pass to python_callable.
        :type op_args: list
        :param op_kwargs: A dict of keyword arguments to pass to python_callable.
        :type op_kwargs: dict
        :param string_args: Strings that are present in the global var virtualenv_string_args,
            available to python_callable at runtime as a list[str]. Note that args are split
            by newline.
        :type string_args: list[str]
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in your callable's context after the template has been applied
        :type templates_dict: dict of str
        :param templates_exts: a list of file extensions to resolve while
            processing templated fields, for examples ``['.sql', '.hql']``
        :type templates_exts: list[str]
        """
        return _virtualenv_task(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            requirements=requirements,
            python_version=python_version,
            use_dill=use_dill,
            system_site_packages=system_site_packages,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )


task = _TaskDecorator()
