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
"""This module is deprecated. Please use :mod:`airflow.providers.google.cloud.operators.vision`."""

import warnings

from airflow.providers.google.cloud.operators.vision import (  # noqa # pylint: disable=unused-import
    CloudVisionAddProductToProductSetOperator,
    CloudVisionCreateProductOperator,
    CloudVisionCreateProductSetOperator,
    CloudVisionCreateReferenceImageOperator,
    CloudVisionDeleteProductOperator,
    CloudVisionDeleteProductSetOperator,
    CloudVisionDetectImageLabelsOperator,
    CloudVisionDetectImageSafeSearchOperator,
    CloudVisionDetectTextOperator,
    CloudVisionGetProductOperator,
    CloudVisionGetProductSetOperator,
    CloudVisionImageAnnotateOperator,
    CloudVisionRemoveProductFromProductSetOperator,
    CloudVisionTextDetectOperator,
    CloudVisionUpdateProductOperator,
    CloudVisionUpdateProductSetOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.vision`.",
    DeprecationWarning,
    stacklevel=2,
)


class CloudVisionAnnotateImageOperator(CloudVisionImageAnnotateOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionDetectDocumentTextOperator(CloudVisionTextDetectOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductCreateOperator(CloudVisionCreateProductOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductDeleteOperator(CloudVisionDeleteProductOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductGetOperator(CloudVisionGetProductOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductSetCreateOperator(CloudVisionCreateProductSetOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductSetDeleteOperator(CloudVisionDeleteProductSetOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductSetGetOperator(CloudVisionGetProductSetOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductSetUpdateOperator(CloudVisionUpdateProductSetOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionProductUpdateOperator(CloudVisionUpdateProductOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudVisionReferenceImageCreateOperator(CloudVisionCreateReferenceImageOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
