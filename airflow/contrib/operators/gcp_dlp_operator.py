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
"""This module is deprecated. Please use :mod:`airflow.providers.google.cloud.operators.dlp`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.google.cloud.operators.dlp import (  # noqa
    CloudDLPCancelDLPJobOperator,
    CloudDLPCreateDeidentifyTemplateOperator,
    CloudDLPCreateDLPJobOperator,
    CloudDLPCreateInspectTemplateOperator,
    CloudDLPCreateJobTriggerOperator,
    CloudDLPCreateStoredInfoTypeOperator,
    CloudDLPDeidentifyContentOperator,
    CloudDLPDeleteDeidentifyTemplateOperator,
    CloudDLPDeleteDLPJobOperator,
    CloudDLPDeleteInspectTemplateOperator,
    CloudDLPDeleteJobTriggerOperator,
    CloudDLPDeleteStoredInfoTypeOperator,
    CloudDLPGetDeidentifyTemplateOperator,
    CloudDLPGetDLPJobOperator,
    CloudDLPGetDLPJobTriggerOperator,
    CloudDLPGetInspectTemplateOperator,
    CloudDLPGetStoredInfoTypeOperator,
    CloudDLPInspectContentOperator,
    CloudDLPListDeidentifyTemplatesOperator,
    CloudDLPListDLPJobsOperator,
    CloudDLPListInfoTypesOperator,
    CloudDLPListInspectTemplatesOperator,
    CloudDLPListJobTriggersOperator,
    CloudDLPListStoredInfoTypesOperator,
    CloudDLPRedactImageOperator,
    CloudDLPReidentifyContentOperator,
    CloudDLPUpdateDeidentifyTemplateOperator,
    CloudDLPUpdateInspectTemplateOperator,
    CloudDLPUpdateJobTriggerOperator,
    CloudDLPUpdateStoredInfoTypeOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.dlp`.",
    DeprecationWarning,
    stacklevel=2,
)


class CloudDLPDeleteDlpJobOperator(CloudDLPDeleteDLPJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudDLPGetDlpJobOperator(CloudDLPGetDLPJobOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudDLPGetJobTripperOperator(CloudDLPGetDLPJobTriggerOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobTriggerOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobTriggerOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudDLPListDlpJobsOperator(CloudDLPListDLPJobsOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
