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

import bz2
import errno
import filecmp
import logging
import shutil
import unittest
from collections import OrderedDict
from gzip import GzipFile
from itertools import product
from tempfile import NamedTemporaryFile, mkdtemp
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.transfers.s3_to_hive import S3ToHiveOperator

try:
    import boto3
    from moto import mock_s3
except ImportError:
    mock_s3 = None


class TestS3ToHiveTransfer(unittest.TestCase):
    def setUp(self):
        self.file_names = {}
        self.task_id = 'S3ToHiveTransferTest'
        self.s3_key = 'S32hive_test_file'
        self.field_dict = OrderedDict([('Sno', 'BIGINT'), ('Some,Text', 'STRING')])
        self.hive_table = 'S32hive_test_table'
        self.delimiter = '\t'
        self.create = True
        self.recreate = True
        self.partition = {'ds': 'STRING'}
        self.headers = True
        self.check_headers = True
        self.wildcard_match = False
        self.input_compressed = False
        self.kwargs = {
            'task_id': self.task_id,
            's3_key': self.s3_key,
            'field_dict': self.field_dict,
            'hive_table': self.hive_table,
            'delimiter': self.delimiter,
            'create': self.create,
            'recreate': self.recreate,
            'partition': self.partition,
            'headers': self.headers,
            'check_headers': self.check_headers,
            'wildcard_match': self.wildcard_match,
            'input_compressed': self.input_compressed,
        }
        try:
            header = b"Sno\tSome,Text \n"
            line1 = b"1\tAirflow Test\n"
            line2 = b"2\tS32HiveTransfer\n"
            self.tmp_dir = mkdtemp(prefix='test_tmps32hive_')
            # create sample txt, gz and bz2 with and without headers
            with NamedTemporaryFile(mode='wb+', dir=self.tmp_dir, delete=False) as f_txt_h:
                self._set_fn(f_txt_h.name, '.txt', True)
                f_txt_h.writelines([header, line1, line2])
            fn_gz = self._get_fn('.txt', True) + ".gz"
            with GzipFile(filename=fn_gz, mode="wb") as f_gz_h:
                self._set_fn(fn_gz, '.gz', True)
                f_gz_h.writelines([header, line1, line2])
            fn_gz_upper = self._get_fn('.txt', True) + ".GZ"
            with GzipFile(filename=fn_gz_upper, mode="wb") as f_gz_upper_h:
                self._set_fn(fn_gz_upper, '.GZ', True)
                f_gz_upper_h.writelines([header, line1, line2])
            fn_bz2 = self._get_fn('.txt', True) + '.bz2'
            with bz2.BZ2File(filename=fn_bz2, mode="wb") as f_bz2_h:
                self._set_fn(fn_bz2, '.bz2', True)
                f_bz2_h.writelines([header, line1, line2])
            # create sample txt, bz and bz2 without header
            with NamedTemporaryFile(mode='wb+', dir=self.tmp_dir, delete=False) as f_txt_nh:
                self._set_fn(f_txt_nh.name, '.txt', False)
                f_txt_nh.writelines([line1, line2])
            fn_gz = self._get_fn('.txt', False) + ".gz"
            with GzipFile(filename=fn_gz, mode="wb") as f_gz_nh:
                self._set_fn(fn_gz, '.gz', False)
                f_gz_nh.writelines([line1, line2])
            fn_gz_upper = self._get_fn('.txt', False) + ".GZ"
            with GzipFile(filename=fn_gz_upper, mode="wb") as f_gz_upper_nh:
                self._set_fn(fn_gz_upper, '.GZ', False)
                f_gz_upper_nh.writelines([line1, line2])
            fn_bz2 = self._get_fn('.txt', False) + '.bz2'
            with bz2.BZ2File(filename=fn_bz2, mode="wb") as f_bz2_nh:
                self._set_fn(fn_bz2, '.bz2', False)
                f_bz2_nh.writelines([line1, line2])
        # Base Exception so it catches Keyboard Interrupt
        except BaseException as e:
            logging.error(e)
            self.tearDown()

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e

    # Helper method to create a dictionary of file names and
    # file types (file extension and header)
    def _set_fn(self, fn, ext, header):
        key = self._get_key(ext, header)
        self.file_names[key] = fn

    # Helper method to fetch a file of a
    # certain format (file extension and header)
    def _get_fn(self, ext, header):
        key = self._get_key(ext, header)
        return self.file_names[key]

    @staticmethod
    def _get_key(ext, header):
        key = ext + "_" + ('h' if header else 'nh')
        return key

    @staticmethod
    def _check_file_equality(fn_1, fn_2, ext):
        # gz files contain mtime and filename in the header that
        # causes filecmp to return False even if contents are identical
        # Hence decompress to test for equality
        if ext.lower() == '.gz':
            with GzipFile(fn_1, 'rb') as f_1, NamedTemporaryFile(mode='wb') as f_txt_1:
                with GzipFile(fn_2, 'rb') as f_2, NamedTemporaryFile(mode='wb') as f_txt_2:
                    shutil.copyfileobj(f_1, f_txt_1)
                    shutil.copyfileobj(f_2, f_txt_2)
                    f_txt_1.flush()
                    f_txt_2.flush()
                    return filecmp.cmp(f_txt_1.name, f_txt_2.name, shallow=False)
        else:
            return filecmp.cmp(fn_1, fn_2, shallow=False)

    def test_bad_parameters(self):
        self.kwargs['check_headers'] = True
        self.kwargs['headers'] = False
        with pytest.raises(AirflowException, match="To check_headers.*"):
            S3ToHiveOperator(**self.kwargs)

    def test__get_top_row_as_list(self):
        self.kwargs['delimiter'] = '\t'
        fn_txt = self._get_fn('.txt', True)
        header_list = S3ToHiveOperator(**self.kwargs)._get_top_row_as_list(fn_txt)
        assert header_list == ['Sno', 'Some,Text'], "Top row from file doesn't matched expected value"

        self.kwargs['delimiter'] = ','
        header_list = S3ToHiveOperator(**self.kwargs)._get_top_row_as_list(fn_txt)
        assert header_list == ['Sno\tSome', 'Text'], "Top row from file doesn't matched expected value"

    def test__match_headers(self):
        self.kwargs['field_dict'] = OrderedDict([('Sno', 'BIGINT'), ('Some,Text', 'STRING')])
        assert S3ToHiveOperator(**self.kwargs)._match_headers(
            ['Sno', 'Some,Text']
        ), "Header row doesn't match expected value"
        # Testing with different column order
        assert not S3ToHiveOperator(**self.kwargs)._match_headers(
            ['Some,Text', 'Sno']
        ), "Header row doesn't match expected value"
        # Testing with extra column in header
        assert not S3ToHiveOperator(**self.kwargs)._match_headers(
            ['Sno', 'Some,Text', 'ExtraColumn']
        ), "Header row doesn't match expected value"

    def test__delete_top_row_and_compress(self):
        s32hive = S3ToHiveOperator(**self.kwargs)
        # Testing gz file type
        fn_txt = self._get_fn('.txt', True)
        gz_txt_nh = s32hive._delete_top_row_and_compress(fn_txt, '.gz', self.tmp_dir)
        fn_gz = self._get_fn('.gz', False)
        assert self._check_file_equality(gz_txt_nh, fn_gz, '.gz'), "gz Compressed file not as expected"
        # Testing bz2 file type
        bz2_txt_nh = s32hive._delete_top_row_and_compress(fn_txt, '.bz2', self.tmp_dir)
        fn_bz2 = self._get_fn('.bz2', False)
        assert self._check_file_equality(bz2_txt_nh, fn_bz2, '.bz2'), "bz2 Compressed file not as expected"

    @unittest.skipIf(mock is None, 'mock package not present')
    @unittest.skipIf(mock_s3 is None, 'moto package not present')
    @mock.patch('airflow.providers.apache.hive.transfers.s3_to_hive.HiveCliHook')
    @mock_s3
    def test_execute(self, mock_hiveclihook):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket='bucket')

        # Testing txt, zip, bz2 files with and without header row
        for (ext, has_header) in product(['.txt', '.gz', '.bz2', '.GZ'], [True, False]):
            self.kwargs['headers'] = has_header
            self.kwargs['check_headers'] = has_header
            logging.info("Testing %s format %s header", ext, 'with' if has_header else 'without')
            self.kwargs['input_compressed'] = ext.lower() != '.txt'
            self.kwargs['s3_key'] = 's3://bucket/' + self.s3_key + ext
            ip_fn = self._get_fn(ext, self.kwargs['headers'])
            op_fn = self._get_fn(ext, False)

            # Upload the file into the Mocked S3 bucket
            conn.upload_file(ip_fn, 'bucket', self.s3_key + ext)

            # file parameter to HiveCliHook.load_file is compared
            # against expected file output
            mock_hiveclihook().load_file.side_effect = lambda *args, **kwargs: self.assertTrue(
                self._check_file_equality(args[0], op_fn, ext),
                f'{ext} output file not as expected',
            )
            # Execute S3ToHiveTransfer
            s32hive = S3ToHiveOperator(**self.kwargs)
            s32hive.execute(None)

    @unittest.skipIf(mock is None, 'mock package not present')
    @unittest.skipIf(mock_s3 is None, 'moto package not present')
    @mock.patch('airflow.providers.apache.hive.transfers.s3_to_hive.HiveCliHook')
    @mock_s3
    def test_execute_with_select_expression(self, mock_hiveclihook):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket='bucket')

        select_expression = "SELECT * FROM S3Object s"
        bucket = 'bucket'

        # Only testing S3ToHiveTransfer calls S3Hook.select_key with
        # the right parameters and its execute method succeeds here,
        # since Moto doesn't support select_object_content as of 1.3.2.
        for (ext, has_header) in product(['.txt', '.gz', '.GZ'], [True, False]):
            input_compressed = ext.lower() != '.txt'
            key = self.s3_key + ext

            self.kwargs['check_headers'] = False
            self.kwargs['headers'] = has_header
            self.kwargs['input_compressed'] = input_compressed
            self.kwargs['select_expression'] = select_expression
            self.kwargs['s3_key'] = f's3://{bucket}/{key}'

            ip_fn = self._get_fn(ext, has_header)

            # Upload the file into the Mocked S3 bucket
            conn.upload_file(ip_fn, bucket, key)

            input_serialization = {'CSV': {'FieldDelimiter': self.delimiter}}
            if input_compressed:
                input_serialization['CompressionType'] = 'GZIP'
            if has_header:
                input_serialization['CSV']['FileHeaderInfo'] = 'USE'

            # Confirm that select_key was called with the right params
            with mock.patch(
                'airflow.providers.amazon.aws.hooks.s3.S3Hook.select_key', return_value=""
            ) as mock_select_key:
                # Execute S3ToHiveTransfer
                s32hive = S3ToHiveOperator(**self.kwargs)
                s32hive.execute(None)

                mock_select_key.assert_called_once_with(
                    bucket_name=bucket,
                    key=key,
                    expression=select_expression,
                    input_serialization=input_serialization,
                )
