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
import json
import warnings
from typing import Any, Iterable, Optional, Union, cast

from bson import json_util

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.decorators import apply_defaults

_DEPRECATION_MSG = (
    "The s3_conn_id parameter has been deprecated. You should pass instead the aws_conn_id parameter."
)


class MongoToS3Operator(BaseOperator):
    """Operator meant to move data from mongo via pymongo to s3 via boto.

    :param mongo_conn_id: reference to a specific mongo connection
    :type mongo_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param mongo_collection: reference to a specific collection in your mongo db
    :type mongo_collection: str
    :param mongo_query: query to execute. A list including a dict of the query
    :type mongo_query: list
    :param s3_bucket: reference to a specific S3 bucket to store the data
    :type s3_bucket: str
    :param s3_key: in which S3 key the file will be stored
    :type s3_key: str
    :param mongo_db: reference to a specific mongo database
    :type mongo_db: str
    :param replace: whether or not to replace the file in S3 if it previously existed
    :type replace: bool
    :param allow_disk_use: in the case you are retrieving a lot of data, you may have
        to use the disk to save it instead of saving all in the RAM
    :type allow_disk_use: bool
    :param compression: type of compression to use for output file in S3. Currently only gzip is supported.
    :type compression: str
    """

    template_fields = ('s3_bucket', 's3_key', 'mongo_query', 'mongo_collection')
    ui_color = '#589636'
    template_fields_renderers = {"mongo_query": "py"}
    # pylint: disable=too-many-instance-attributes

    @apply_defaults
    def __init__(
        self,
        *,
        s3_conn_id: Optional[str] = None,
        mongo_conn_id: str = 'mongo_default',
        aws_conn_id: str = 'aws_default',
        mongo_collection: str,
        mongo_query: Union[list, dict],
        s3_bucket: str,
        s3_key: str,
        mongo_db: Optional[str] = None,
        replace: bool = False,
        allow_disk_use: bool = False,
        compression: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if s3_conn_id:
            warnings.warn(_DEPRECATION_MSG, DeprecationWarning, stacklevel=3)
            aws_conn_id = s3_conn_id

        self.mongo_conn_id = mongo_conn_id
        self.aws_conn_id = aws_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = isinstance(self.mongo_query, list)

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.replace = replace
        self.allow_disk_use = allow_disk_use
        self.compression = compression

    def execute(self, context) -> bool:
        """Is written to depend on transform method"""
        s3_conn = S3Hook(self.aws_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        if self.is_pipeline:
            results = MongoHook(self.mongo_conn_id).aggregate(
                mongo_collection=self.mongo_collection,
                aggregate_query=cast(list, self.mongo_query),
                mongo_db=self.mongo_db,
                allowDiskUse=self.allow_disk_use,
            )

        else:
            results = MongoHook(self.mongo_conn_id).find(
                mongo_collection=self.mongo_collection,
                query=cast(dict, self.mongo_query),
                mongo_db=self.mongo_db,
            )

        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))

        s3_conn.load_string(
            string_data=docs_str,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=self.replace,
            compression=self.compression,
        )

    @staticmethod
    def _stringify(iterable: Iterable, joinable: str = '\n') -> str:
        """
        Takes an iterable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join([json.dumps(doc, default=json_util.default) for doc in iterable])

    @staticmethod
    def transform(docs: Any) -> Any:
        """This method is meant to be extended by child classes
        to perform transformations unique to those operators needs.
        Processes pyMongo cursor and returns an iterable with each element being
        a JSON serializable dictionary

        Base transform() assumes no processing is needed
        ie. docs is a pyMongo cursor of documents and cursor just
        needs to be passed through

        Override this method for custom transformations
        """
        return docs
