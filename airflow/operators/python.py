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
import inspect
import os
import pickle
import sys
import types
import warnings
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import dill

# To maintain backwards compatibility, we import the task object into this file
# This prevents breakages in dags that use `from airflow.operators.python import task`
from airflow.decorators.python import (  # noqa # pylint: disable=unused-import
    PYTHON_OPERATOR_UI_COLOR,
    python_task,
)
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskinstance import _CURRENT_CONTEXT
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.process_utils import execute_in_subprocess
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script


def task(python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs):
    """
    Deprecated function that calls @task.python and allows users to turn a python function into
    an Airflow task. Please use the following instead:

    from airflow.decorators import task

    @task
    def my_task()

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :type multiple_outputs: bool
    :return:
    """
    warnings.warn(
        """airflow.operators.python.task is deprecated. Please use the following instead

        from airflow.decorators import task
        @task
        def my_task()""",
        DeprecationWarning,
        stacklevel=2,
    )
    return python_task(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)


class PythonOperator(BaseOperator):
    """
    Executes a Python callable

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonOperator`

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list (templated)
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :type templates_dict: dict[str]
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :type templates_exts: list[str]
    """

    template_fields = ('templates_dict', 'op_args', 'op_kwargs')
    template_fields_renderers = {"templates_dict": "json", "op_args": "py", "op_kwargs": "py"}
    ui_color = PYTHON_OPERATOR_UI_COLOR

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs = (
        'python_callable',
        'op_kwargs',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Optional[List] = None,
        op_kwargs: Optional[Dict] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        if kwargs.get("provide_context"):
            warnings.warn(
                "provide_context is deprecated as of 2.0 and is no longer required",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs.pop('provide_context', None)
        super().__init__(**kwargs)
        if not callable(python_callable):
            raise AirflowException('`python_callable` param must be callable')
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts

    def execute(self, context: Dict):
        context.update(self.op_kwargs)
        context['templates_dict'] = self.templates_dict

        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        return return_value

    def execute_callable(self):
        """
        Calls the python callable with the given arguments.

        :return: the return value of the call.
        :rtype: any
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)


class BranchPythonOperator(PythonOperator, SkipMixin):
    """
    Allows a workflow to "branch" or follow a path following the execution
    of this task.

    It derives the PythonOperator and expects a Python function that returns
    a single task_id or list of task_ids to follow. The task_id(s) returned
    should point to a task directly downstream from {self}. All other "branches"
    or directly downstream tasks are marked with a state of ``skipped`` so that
    these paths can't move forward. The ``skipped`` states are propagated
    downstream to allow for the DAG state to fill up and the DAG run's state
    to be inferred.
    """

    def execute(self, context: Dict):
        branch = super().execute(context)
        self.skip_all_except(context['ti'], branch)
        return branch


class ShortCircuitOperator(PythonOperator, SkipMixin):
    """
    Allows a workflow to continue only if a condition is met. Otherwise, the
    workflow "short-circuits" and downstream tasks are skipped.

    The ShortCircuitOperator is derived from the PythonOperator. It evaluates a
    condition and short-circuits the workflow if the condition is False. Any
    downstream tasks are marked with a state of "skipped". If the condition is
    True, downstream tasks proceed as normal.

    The condition is determined by the result of `python_callable`.
    """

    def execute(self, context: Dict):
        condition = super().execute(context)
        self.log.info("Condition result is %s", condition)

        if condition:
            self.log.info('Proceeding with downstream tasks...')
            return

        self.log.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        self.log.info("Done.")


class PythonVirtualenvOperator(PythonOperator):
    """
    Allows one to run a function in a virtualenv that is created and destroyed
    automatically (with certain caveats).

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

    BASE_SERIALIZABLE_CONTEXT_KEYS = {
        'ds_nodash',
        'inlets',
        'next_ds',
        'next_ds_nodash',
        'outlets',
        'params',
        'prev_ds',
        'prev_ds_nodash',
        'run_id',
        'task_instance_key_str',
        'test_mode',
        'tomorrow_ds',
        'tomorrow_ds_nodash',
        'ts',
        'ts_nodash',
        'ts_nodash_with_tz',
        'yesterday_ds',
        'yesterday_ds_nodash',
    }
    PENDULUM_SERIALIZABLE_CONTEXT_KEYS = {
        'execution_date',
        'next_execution_date',
        'prev_execution_date',
        'prev_execution_date_success',
        'prev_start_date_success',
    }
    AIRFLOW_SERIALIZABLE_CONTEXT_KEYS = {'macros', 'conf', 'dag', 'dag_run', 'task'}

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        python_callable: Callable,
        requirements: Optional[Iterable[str]] = None,
        python_version: Optional[Union[str, int, float]] = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        op_args: Optional[List] = None,
        op_kwargs: Optional[Dict] = None,
        string_args: Optional[Iterable[str]] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ):
        if (
            not isinstance(python_callable, types.FunctionType)
            or isinstance(python_callable, types.LambdaType)
            and python_callable.__name__ == "<lambda>"
        ):
            raise AirflowException('PythonVirtualenvOperator only supports functions for python_callable arg')
        if (
            python_version
            and str(python_version)[0] != str(sys.version_info.major)
            and (op_args or op_kwargs)
        ):
            raise AirflowException(
                "Passing op_args or op_kwargs is not supported across different Python "
                "major versions for PythonVirtualenvOperator. Please use string_args."
            )
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )
        self.requirements = list(requirements or [])
        self.string_args = string_args or []
        self.python_version = python_version
        self.use_dill = use_dill
        self.system_site_packages = system_site_packages
        if not self.system_site_packages and self.use_dill and 'dill' not in self.requirements:
            self.requirements.append('dill')
        self.pickling_library = dill if self.use_dill else pickle

    def execute(self, context: Dict):
        serializable_context = {key: context[key] for key in self._get_serializable_context_keys()}
        return super().execute(context=serializable_context)

    def execute_callable(self):
        with TemporaryDirectory(prefix='venv') as tmp_dir:
            if self.templates_dict:
                self.op_kwargs['templates_dict'] = self.templates_dict

            input_filename = os.path.join(tmp_dir, 'script.in')
            output_filename = os.path.join(tmp_dir, 'script.out')
            string_args_filename = os.path.join(tmp_dir, 'string_args.txt')
            script_filename = os.path.join(tmp_dir, 'script.py')

            prepare_virtualenv(
                venv_directory=tmp_dir,
                python_bin=f'python{self.python_version}' if self.python_version else None,
                system_site_packages=self.system_site_packages,
                requirements=self.requirements,
            )

            self._write_args(input_filename)
            self._write_string_args(string_args_filename)
            write_python_script(
                jinja_context=dict(
                    op_args=self.op_args,
                    op_kwargs=self.op_kwargs,
                    pickling_library=self.pickling_library.__name__,
                    python_callable=self.python_callable.__name__,
                    python_callable_source=dedent(inspect.getsource(self.python_callable)),
                ),
                filename=script_filename,
            )

            execute_in_subprocess(
                cmd=[
                    f'{tmp_dir}/bin/python',
                    script_filename,
                    input_filename,
                    output_filename,
                    string_args_filename,
                ]
            )

            return self._read_result(output_filename)

    def _write_args(self, filename):
        if self.op_args or self.op_kwargs:
            with open(filename, 'wb') as file:
                self.pickling_library.dump({'args': self.op_args, 'kwargs': self.op_kwargs}, file)

    def _get_serializable_context_keys(self):
        def _is_airflow_env():
            return self.system_site_packages or 'apache-airflow' in self.requirements

        def _is_pendulum_env():
            return 'pendulum' in self.requirements and 'lazy_object_proxy' in self.requirements

        serializable_context_keys = self.BASE_SERIALIZABLE_CONTEXT_KEYS.copy()
        if _is_airflow_env():
            serializable_context_keys.update(self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS)
        if _is_pendulum_env() or _is_airflow_env():
            serializable_context_keys.update(self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS)
        return serializable_context_keys

    def _write_string_args(self, filename):
        with open(filename, 'w') as file:
            file.write('\n'.join(map(str, self.string_args)))

    def _read_result(self, filename):
        if os.stat(filename).st_size == 0:
            return None
        with open(filename, 'rb') as file:
            try:
                return self.pickling_library.load(file)
            except ValueError:
                self.log.error(
                    "Error deserializing result. Note that result deserialization "
                    "is not supported across major Python versions."
                )
                raise


def get_current_context() -> Dict[str, Any]:
    """
    Obtain the execution context for the currently executing operator without
    altering user method's signature.
    This is the simplest method of retrieving the execution context dictionary.

    **Old style:**

    .. code:: python

        def my_task(**context):
            ti = context["ti"]

    **New style:**

    .. code:: python

        from airflow.operators.python import get_current_context
        def my_task():
            context = get_current_context()
            ti = context["ti"]

    Current context will only have value if this method was called after an operator
    was starting to execute.
    """
    if not _CURRENT_CONTEXT:
        raise AirflowException(
            "Current context was requested but no context was found! "
            "Are you running within an airflow task?"
        )
    return _CURRENT_CONTEXT[-1]
