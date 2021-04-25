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
import json
from typing import Any, Callable, Dict, List, Optional, Union

from rich.box import ASCII_DOUBLE_HEAD
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from tabulate import tabulate

import airflow.utils.yaml as yaml
from airflow.plugins_manager import PluginsDirectorySource
from airflow.utils.platform import is_tty


class AirflowConsole(Console):
    """Airflow rich console"""

    def __init__(self, show_header: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the width to constant to pipe whole output from console
        self._width = 200 if not is_tty() else self._width

        # If show header in tables
        self.show_header = show_header

    def print_as_json(self, data: Dict):
        """Renders dict as json text representation"""
        json_content = json.dumps(data)
        self.print(Syntax(json_content, "json", theme="ansi_dark"), soft_wrap=True)

    def print_as_yaml(self, data: Dict):
        """Renders dict as yaml text representation"""
        yaml_content = yaml.dump(data)
        self.print(Syntax(yaml_content, "yaml", theme="ansi_dark"), soft_wrap=True)

    def print_as_table(self, data: List[Dict]):
        """Renders list of dictionaries as table"""
        if not data:
            self.print("No data found")
            return

        table = SimpleTable(show_header=self.show_header)
        for col in data[0].keys():
            table.add_column(col)

        for row in data:
            table.add_row(*[str(d) for d in row.values()])
        self.print(table)

    def print_as_plain_table(self, data: List[Dict]):
        """Renders list of dictionaries as a simple table than can be easily piped"""
        if not data:
            self.print("No data found")
            return
        rows = [d.values() for d in data]
        output = tabulate(rows, tablefmt="plain", headers=data[0].keys())
        print(output)

    # pylint: disable=too-many-return-statements
    def _normalize_data(self, value: Any, output: str) -> Optional[Union[list, str, dict]]:
        if isinstance(value, (tuple, list)):
            if output == "table":
                return ",".join(self._normalize_data(x, output) for x in value)
            return [self._normalize_data(x, output) for x in value]
        if isinstance(value, dict) and output != "table":
            return {k: self._normalize_data(v, output) for k, v in value.items()}
        if inspect.isclass(value) and not isinstance(value, PluginsDirectorySource):
            return value.__name__
        if value is None:
            return None
        return str(value)

    def print_as(self, data: List[Union[Dict, Any]], output: str, mapper: Optional[Callable] = None):
        """Prints provided using format specified by output argument"""
        output_to_renderer = {
            "json": self.print_as_json,
            "yaml": self.print_as_yaml,
            "table": self.print_as_table,
            "plain": self.print_as_plain_table,
        }
        renderer = output_to_renderer.get(output)
        if not renderer:
            raise ValueError(
                f"Unknown formatter: {output}. Allowed options: {list(output_to_renderer.keys())}"
            )

        if not all(isinstance(d, dict) for d in data) and not mapper:
            raise ValueError("To tabulate non-dictionary data you need to provide `mapper` function")

        if mapper:
            dict_data: List[Dict] = [mapper(d) for d in data]
        else:
            dict_data: List[Dict] = data
        dict_data = [{k: self._normalize_data(v, output) for k, v in d.items()} for d in dict_data]
        renderer(dict_data)


class SimpleTable(Table):
    """A rich Table with some default hardcoded for consistency."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.show_edge = kwargs.get("show_edge", False)
        self.pad_edge = kwargs.get("pad_edge", False)
        self.box = kwargs.get("box", ASCII_DOUBLE_HEAD)
        self.show_header = kwargs.get("show_header", False)
        self.title_style = kwargs.get("title_style", "bold green")
        self.title_justify = kwargs.get("title_justify", "left")
        self.caption = kwargs.get("caption", " ")

    def add_column(self, *args, **kwargs) -> None:  # pylint: disable=signature-differs
        """Add a column to the table. We use different default"""
        kwargs["overflow"] = kwargs.get("overflow")  # to avoid truncating
        super().add_column(*args, **kwargs)
