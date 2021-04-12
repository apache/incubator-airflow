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
"""This module is deprecated. Please use :mod:`airflow.providers.google.cloud.operators.dataproc`."""

import warnings

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocJobBaseOperator,
    DataprocScaleClusterOperator,
    DataprocSubmitHadoopJobOperator,
    DataprocSubmitHiveJobOperator,
    DataprocSubmitPigJobOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocSubmitSparkJobOperator,
    DataprocSubmitSparkSqlJobOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.dataproc`.",
    DeprecationWarning,
    stacklevel=2,
)


class DataprocClusterCreateOperator(DataprocCreateClusterOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataprocClusterDeleteOperator(DataprocDeleteClusterOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataprocClusterScaleOperator(DataprocScaleClusterOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcHadoopOperator(DataprocSubmitHadoopJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcHiveOperator(DataprocSubmitHiveJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcJobBaseOperator(DataprocJobBaseOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcPigOperator(DataprocSubmitPigJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcPySparkOperator(DataprocSubmitPySparkJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcSparkOperator(DataprocSubmitSparkJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataProcSparkSqlOperator(DataprocSubmitSparkSqlJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataprocWorkflowTemplateInstantiateInlineOperator(DataprocInstantiateInlineWorkflowTemplateOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc
            .DataprocInstantiateInlineWorkflowTemplateOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class DataprocWorkflowTemplateInstantiateOperator(DataprocInstantiateWorkflowTemplateOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.dataproc
            .DataprocInstantiateWorkflowTemplateOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
