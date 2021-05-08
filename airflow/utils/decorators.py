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
#

import warnings
from functools import wraps
from typing import Callable, TypeVar, cast

T = TypeVar('T', bound=Callable)  # pylint: disable=invalid-name


def apply_defaults(func: T) -> T:
    """
    This decorator is deprecated.

    In previous versions, all subclasses of BaseOperator must use apply_default decorator for the"
    `default_args` feature to work properly.

    In current version, it is optional. The decorator is applied automatically using the metaclass.
    """
    warnings.warn(
        "This decorator is deprecated. \n"
        "\n"
        "In previous versions, all subclasses of BaseOperator must use apply_default decorator for the"
        "`default_args` feature to work properly.\n"
        "\n"
        "In current version, it is optional. The decorator is applied automatically using the metaclass.\n",
        DeprecationWarning,
        stacklevel=3,
    )

    # Make it still be a wrapper to keep the previous behaviour of an extra stack frame
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return cast(T, wrapper)
