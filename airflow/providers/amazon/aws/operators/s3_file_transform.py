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

import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3FileTransformOperator(BaseOperator):
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination S3
    location.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file. The operator then takes over control and uploads the
    local destination file to S3.

    S3 Select is also available to filter the source contents. Users can
    omit the transformation script if S3 Select expression is specified.

    :param source_s3_key: The key to be retrieved from S3. (templated)
    :type source_s3_key: str
    :param dest_s3_key: The key to be written from S3. (templated)
    :type dest_s3_key: str
    :param transform_script: location of the executable transformation script
    :type transform_script: str
    :param select_expression: S3 Select expression
    :type select_expression: str
    :param script_args: arguments for transformation script (templated)
    :type script_args: sequence of str
    :param source_aws_conn_id: source s3 connection
    :type source_aws_conn_id: str
           (Adding this param to template_fields so that it can be overriden using jinja template from Dags
            This feature can be useful if user wants to update/override the aws_conn_id for some kind of Dag Isolation etc)
    :param source_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :type source_verify: bool or str
    :param dest_aws_conn_id: destination s3 connection
    :type dest_aws_conn_id: str
          (Adding this param to template_fields so that it can be overriden using jinja template from Dags
           This feature can be useful if user wants to update/override the aws_conn_id for some kind of Dag Isolation etc)
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        See: ``source_verify``
    :type dest_verify: bool or str
    :param replace: Replace dest S3 key if it already exists
    :type replace: bool
    """

    template_fields = ('source_s3_key', 'dest_s3_key', 'script_args' , 'source_aws_conn_id' , 'dest_aws_conn_id')
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(
        self,
        *,
        source_s3_key: str,
        dest_s3_key: str,
        transform_script: Optional[str] = None,
        select_expression=None,
        script_args: Optional[Sequence[str]] = None,
        source_aws_conn_id: str = 'aws_default',
        source_verify: Optional[Union[bool, str]] = None,
        dest_aws_conn_id: str = 'aws_default',
        dest_verify: Optional[Union[bool, str]] = None,
        replace: bool = False,
        **kwargs,
    ) -> None:
        # pylint: disable=too-many-arguments
        super().__init__(**kwargs)
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.transform_script = transform_script
        self.select_expression = select_expression
        self.script_args = script_args or []
        self.output_encoding = sys.getdefaultencoding()

    def execute(self, context):
        if self.transform_script is None and self.select_expression is None:
            raise AirflowException("Either transform_script or select_expression must be specified")

        source_s3 = S3Hook(aws_conn_id=self.source_aws_conn_id, verify=self.source_verify)
        dest_s3 = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)

        self.log.info("Downloading source S3 file %s", self.source_s3_key)
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException(f"The source key {self.source_s3_key} does not exist")
        source_s3_key_object = source_s3.get_key(self.source_s3_key)

        with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
            self.log.info("Dumping S3 file %s contents to local file %s", self.source_s3_key, f_source.name)

            if self.select_expression is not None:
                content = source_s3.select_key(key=self.source_s3_key, expression=self.select_expression)
                f_source.write(content.encode("utf-8"))
            else:
                source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            if self.transform_script is not None:
                with subprocess.Popen(
                    [self.transform_script, f_source.name, f_dest.name, *self.script_args],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                ) as process:
                    self.log.info("Output:")
                    for line in iter(process.stdout.readline, b''):
                        self.log.info(line.decode(self.output_encoding).rstrip())

                    process.wait()

                    if process.returncode:
                        raise AirflowException(f"Transform script failed: {process.returncode}")
                    else:
                        self.log.info(
                            "Transform script successful. Output temporarily located at %s", f_dest.name
                        )

            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name if self.transform_script else f_source.name,
                key=self.dest_s3_key,
                replace=self.replace,
            )
            self.log.info("Upload successful")
