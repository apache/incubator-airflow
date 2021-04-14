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

"""Use libyaml for YAML dump/load operations where possible.

If libyaml is available we will use it -- it is significantly faster.

This module delegates all other properties to the yaml module, so it can be used as:

.. code-block:: python
    import airflow.utils.yaml as yaml

And then be used directly in place of the normal python module.
"""
import sys
from typing import TYPE_CHECKING, Any, BinaryIO, TextIO, Union, cast

if TYPE_CHECKING:
    from yaml.error import MarkedYAMLError  # noqa


def safe_load(stream: Union[bytes, str, BinaryIO, TextIO]) -> Any:
    """Like yaml.safe_load, but use the C libyaml for speed where we can"""
    # delay import until use.
    from yaml import load as orig

    try:
        from yaml import CSafeLoader as SafeLoader
    except ImportError:
        from yaml import SafeLoader  # type: ignore[no-redef]

    return orig(stream, SafeLoader)


def dump(data: Any, **kwargs) -> str:
    """Like yaml.safe_dump, but use the C libyaml for speed where we can"""
    # delay import until use.
    from yaml import dump as orig

    try:
        from yaml import CSafeDumper as SafeDumper
    except ImportError:
        from yaml import SafeDumper  # type: ignore[no-redef]

    return cast(str, orig(data, Dumper=SafeDumper, **kwargs))


def __getattr__(name):
    # Delegate anything else to the yaml module
    import yaml

    if name == "FullLoader":
        # Try to use CFullLoader by default
        getattr(yaml, "CFullLoader", yaml.FullLoader)

    return getattr(yaml, name)


if sys.version_info < (3, 7):
    from pep562 import Pep562

    Pep562(__name__)
