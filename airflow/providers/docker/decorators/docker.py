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

import base64
import inspect
import os
import pickle
import shlex
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Callable, Dict, Optional, TypeVar

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.python_virtualenv import remove_task_decorator, write_python_script


def _generate_decode_command(env_var, file):
    return shlex.quote(
        f'python -c "import os; import base64;'
        f' x = base64.b64decode(os.environ["{env_var}"]);'
        f' f = open("{file}", "wb"); f.write(x);'
        f' f.close()"'
    )


def _b64_encode_file(filename):
    with open(filename, "rb") as file_to_encode:
        return base64.b64encode(file_to_encode.read())


class _DockerDecoratedOperator(DecoratedOperator, DockerOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

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
    """

    template_fields = ('op_args', 'op_kwargs')
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs = ('python_callable',)

    @apply_defaults
    def __init__(
        self,
        **kwargs,
    ) -> None:
        self.string_args = [1, 2, 1]
        self._output_filename = ""
        command = "dummy command"
        self.pickling_library = pickle
        super().__init__(
            command=command, retrieve_output=True, retrieve_output_path="/tmp/script.out", **kwargs
        )

    def execute(self, context: Dict):
        with TemporaryDirectory(prefix='venv') as tmp_dir:
            input_filename = os.path.join(tmp_dir, 'script.in')
            self._output_filename = os.path.join(tmp_dir, 'script.out')
            string_args_filename = os.path.join(tmp_dir, 'string_args.txt')
            script_filename = os.path.join(tmp_dir, 'script.py')
            self._write_args(input_filename)
            self._write_string_args(string_args_filename)
            py_source = self._get_python_source()
            write_python_script(
                jinja_context=dict(
                    op_args=self.op_args,
                    op_kwargs=self.op_kwargs,
                    pickling_library=self.pickling_library.__name__,
                    python_callable=self.python_callable.__name__,
                    python_callable_source=py_source,
                ),
                filename=script_filename,
            )

            self.environment["PYTHON_SCRIPT"] = _b64_encode_file(script_filename)
            if self.op_args or self.op_kwargs:
                self.environment["PYTHON_INPUT"] = _b64_encode_file(input_filename)
            else:
                self.environment["PYTHON_INPUT"] = ""

            self.command = (
                f'bash -cx \'{_generate_decode_command("PYTHON_SCRIPT", "/tmp/script.py")} &&'
                f'touch /tmp/string_args &&'
                f'touch /tmp/script.in &&'
                f'{_generate_decode_command("PYTHON_INPUT", "/tmp/script.in")} &&'
                f'python /tmp/script.py /tmp/script.in /tmp/script.out /tmp/string_args \''
            )
            return super().execute(context)

    def _write_args(self, filename):
        if self.op_args or self.op_kwargs:
            with open(filename, 'wb') as file:
                self.pickling_library.dump({'args': self.op_args, 'kwargs': self.op_kwargs}, file)

    def _write_string_args(self, filename):
        with open(filename, 'w') as file:
            file.write('\n'.join(map(str, self.string_args)))

    def _get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.docker")
        return res


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def _docker_task(
    python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_DockerDecoratedOperator,
        **kwargs,
    )
