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
"""This module is deprecated. Please use :mod:`airflow.providers.google.cloud.operators.compute`."""

import warnings

from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineBaseOperator,
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.compute`.",
    DeprecationWarning,
    stacklevel=2,
)


class GceBaseOperator(ComputeEngineBaseOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GceInstanceGroupManagerUpdateTemplateOperator(ComputeEngineInstanceGroupUpdateManagerTemplateOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.compute
    .ComputeEngineInstanceGroupUpdateManagerTemplateOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated. Please use
            `airflow.providers.google.cloud.operators.compute
            .ComputeEngineInstanceGroupUpdateManagerTemplateOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GceInstanceStartOperator(ComputeEngineStartInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators
    .compute.ComputeEngineStartInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.compute
            .ComputeEngineStartInstanceOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GceInstanceStopOperator(ComputeEngineStopInstanceOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.compute.ComputeEngineStopInstanceOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.compute
            .ComputeEngineStopInstanceOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GceInstanceTemplateCopyOperator(ComputeEngineCopyInstanceTemplateOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.compute.ComputeEngineCopyInstanceTemplateOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """"This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.compute
            .ComputeEngineCopyInstanceTemplateOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GceSetMachineTypeOperator(ComputeEngineSetMachineTypeOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.compute.ComputeEngineSetMachineTypeOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.compute
            .ComputeEngineSetMachineTypeOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
