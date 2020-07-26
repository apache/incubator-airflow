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
"""This module is deprecated. Please use `airflow.providers.sqlite.operators.sqlite`."""

import warnings

# pylint: disable=unused-import
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow.utils.decorators import apply_defaults

warnings.warn(
    "This module is deprecated. Please use `airflow.operators.sql`.",
    DeprecationWarning, stacklevel=2
)


class VerticaOperator(SQLExecuteQueryOperator):
    """
    This class is deprecated.
    Please use `airflow.operators.sql.SQLExecuteQueryOperator`.
    """
    @apply_defaults
    def __init__(
        self,
        *,
        vertica_conn_id='vertica_default',
        **kwargs
    ) -> None:
        super().__init__(conn_id=vertica_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.operators.sql.SQLExecuteQueryOperator`.""",
            DeprecationWarning, stacklevel=2
        )
