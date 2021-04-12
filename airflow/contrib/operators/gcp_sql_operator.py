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
"""This module is deprecated. Please use :mod:`airflow.providers.google.cloud.operators.cloud_sql`."""

import warnings

from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLBaseOperator,
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceDatabaseOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
    CloudSQLExportInstanceOperator,
    CloudSQLImportInstanceOperator,
    CloudSQLInstancePatchOperator,
    CloudSQLPatchInstanceDatabaseOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.cloud_sql`",
    DeprecationWarning,
    stacklevel=2,
)


class CloudSqlBaseOperator(CloudSQLBaseOperator):
    """
    This class is deprecated. Please use
    `airflow.providers.google.cloud.operators.sql.CloudSQLBaseOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceCreateOperator(CloudSQLCreateInstanceOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLCreateInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceDatabaseCreateOperator(CloudSQLCreateInstanceDatabaseOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLCreateInstanceDatabaseOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceDatabaseDeleteOperator(CloudSQLDeleteInstanceDatabaseOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLDeleteInstanceDatabaseOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceDatabasePatchOperator(CloudSQLPatchInstanceDatabaseOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLPatchInstanceDatabaseOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceDeleteOperator(CloudSQLDeleteInstanceOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLDeleteInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceExportOperator(CloudSQLExportInstanceOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLExportInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstanceImportOperator(CloudSQLImportInstanceOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators.sql
    .CloudSQLImportInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlInstancePatchOperator(CloudSQLInstancePatchOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators
    .sql.CloudSQLInstancePatchOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)


class CloudSqlQueryOperator(CloudSQLExecuteQueryOperator):
    """
    This class is deprecated. Please use `airflow.providers.google.cloud.operators
    .sql.CloudSQLExecuteQueryOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(self.__doc__, DeprecationWarning, stacklevel=3)
        super().__init__(*args, **kwargs)
