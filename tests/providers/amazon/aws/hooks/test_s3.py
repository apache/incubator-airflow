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
import gzip as gz
import os
import tempfile
from unittest import mock
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None


# This class needs to be separated out because if there are earlier mocks in the same class
# the tests will fail on teardown.
class TestAwsS3HookNoMock:
    def test_check_for_bucket_raises_error_with_invalid_conn_id(self, monkeypatch):
        monkeypatch.delenv('AWS_PROFILE', raising=False)
        monkeypatch.delenv('AWS_ACCESS_KEY_ID', raising=False)
        monkeypatch.delenv('AWS_SECRET_ACCESS_KEY', raising=False)
        hook = S3Hook(aws_conn_id="does_not_exist")
        with pytest.raises(NoCredentialsError):
            hook.check_for_bucket("test-non-existing-bucket")


@pytest.mark.skipif(mock_s3 is None, reason='moto package not present')
class TestAwsS3Hook:
    @mock_s3
    def test_get_conn(self):
        hook = S3Hook()
        assert hook.get_conn() is not None

    @mock_s3
    def test_use_threads_default_value(self):
        hook = S3Hook()
        assert hook.transfer_config.use_threads is True

    @mock_s3
    def test_use_threads_set_value(self):
        hook = S3Hook(transfer_config_args={"use_threads": False})
        assert hook.transfer_config.use_threads is False

    def test_parse_s3_url(self):
        parsed = S3Hook.parse_s3_url("s3://test/this/is/not/a-real-key.txt")
        assert parsed == ("test", "this/is/not/a-real-key.txt"), "Incorrect parsing of the s3 url"

    def test_check_for_bucket(self, s3_bucket):
        hook = S3Hook()
        assert hook.check_for_bucket(s3_bucket) is True
        assert hook.check_for_bucket('not-a-bucket') is False

    @mock_s3
    def test_get_bucket(self):
        hook = S3Hook()
        assert hook.get_bucket('bucket') is not None

    @mock_s3
    def test_create_bucket_default_region(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        assert hook.get_bucket('new_bucket') is not None

    @mock_s3
    def test_create_bucket_us_standard_region(self, monkeypatch):
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)

        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket', region_name='us-east-1')
        bucket = hook.get_bucket('new_bucket')
        assert bucket is not None
        region = bucket.meta.client.get_bucket_location(Bucket=bucket.name).get('LocationConstraint')
        # https://github.com/spulec/moto/pull/1961
        # If location is "us-east-1", LocationConstraint should be None
        assert region is None

    @mock_s3
    def test_create_bucket_other_region(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket', region_name='us-east-2')
        bucket = hook.get_bucket('new_bucket')
        assert bucket is not None
        region = bucket.meta.client.get_bucket_location(Bucket=bucket.name).get('LocationConstraint')
        assert region == 'us-east-2'

    def test_check_for_prefix(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='a', Body=b'a')
        bucket.put_object(Key='dir/b', Body=b'b')

        assert hook.check_for_prefix(bucket_name=s3_bucket, prefix='dir/', delimiter='/') is True
        assert hook.check_for_prefix(bucket_name=s3_bucket, prefix='a', delimiter='/') is False

    def test_list_prefixes(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='a', Body=b'a')
        bucket.put_object(Key='dir/b', Body=b'b')

        assert [] == hook.list_prefixes(s3_bucket, prefix='non-existent/')
        assert ['dir/'] == hook.list_prefixes(s3_bucket, delimiter='/')
        assert ['a'] == hook.list_keys(s3_bucket, delimiter='/')
        assert ['dir/b'] == hook.list_keys(s3_bucket, prefix='dir/')

    def test_list_prefixes_paged(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)

        # we don't need to test the paginator that's covered by boto tests
        keys = [f"{i}/b" for i in range(2)]
        dirs = [f"{i}/" for i in range(2)]
        for key in keys:
            bucket.put_object(Key=key, Body=b'a')

        assert sorted(dirs) == sorted(hook.list_prefixes(s3_bucket, delimiter='/', page_size=1))

    def test_list_keys(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='a', Body=b'a')
        bucket.put_object(Key='dir/b', Body=b'b')

        assert [] == hook.list_keys(s3_bucket, prefix='non-existent/')
        assert ['a', 'dir/b'] == hook.list_keys(s3_bucket)
        assert ['a'] == hook.list_keys(s3_bucket, delimiter='/')
        assert ['dir/b'] == hook.list_keys(s3_bucket, prefix='dir/')

    def test_list_keys_paged(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)

        keys = [str(i) for i in range(2)]
        for key in keys:
            bucket.put_object(Key=key, Body=b'a')

        assert sorted(keys) == sorted(hook.list_keys(s3_bucket, delimiter='/', page_size=1))

    def test_check_for_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='a', Body=b'a')

        assert hook.check_for_key('a', s3_bucket) is True
        assert hook.check_for_key(f's3://{s3_bucket}//a') is True
        assert hook.check_for_key('b', s3_bucket) is False
        assert hook.check_for_key(f's3://{s3_bucket}//b') is False

    def test_get_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='a', Body=b'a')

        assert hook.get_key('a', s3_bucket).key == 'a'
        assert hook.get_key(f's3://{s3_bucket}/a').key == 'a'

    def test_read_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='my_key', Body=b'Cont\xC3\xA9nt')

        assert hook.read_key('my_key', s3_bucket) == 'Contént'

    # As of 1.3.2, Moto doesn't support select_object_content yet.
    @mock.patch('airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_client_type')
    def test_select_key(self, mock_get_client_type, s3_bucket):
        mock_get_client_type.return_value.select_object_content.return_value = {
            'Payload': [{'Records': {'Payload': b'Cont\xC3\xA9nt'}}]
        }
        hook = S3Hook()
        assert hook.select_key('my_key', s3_bucket) == 'Contént'

    def test_check_for_wildcard_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='abc', Body=b'a')
        bucket.put_object(Key='a/b', Body=b'a')

        assert hook.check_for_wildcard_key('a*', s3_bucket) is True
        assert hook.check_for_wildcard_key('abc', s3_bucket) is True
        assert hook.check_for_wildcard_key(f's3://{s3_bucket}//a*') is True
        assert hook.check_for_wildcard_key(f's3://{s3_bucket}//abc') is True

        assert hook.check_for_wildcard_key('a', s3_bucket) is False
        assert hook.check_for_wildcard_key('b', s3_bucket) is False
        assert hook.check_for_wildcard_key(f's3://{s3_bucket}//a') is False
        assert hook.check_for_wildcard_key(f's3://{s3_bucket}//b') is False

    def test_get_wildcard_key(self, s3_bucket):
        hook = S3Hook()
        bucket = hook.get_bucket(s3_bucket)
        bucket.put_object(Key='abc', Body=b'a')
        bucket.put_object(Key='a/b', Body=b'a')

        # The boto3 Class API is _odd_, and we can't do an isinstance check as
        # each instance is a different class, so lets just check one property
        # on S3.Object. Not great but...
        assert hook.get_wildcard_key('a*', s3_bucket).key == 'a/b'
        assert hook.get_wildcard_key('a*', s3_bucket, delimiter='/').key == 'abc'
        assert hook.get_wildcard_key('abc', s3_bucket, delimiter='/').key == 'abc'
        assert hook.get_wildcard_key(f's3://{s3_bucket}/a*').key == 'a/b'
        assert hook.get_wildcard_key(f's3://{s3_bucket}/a*', delimiter='/').key == 'abc'
        assert hook.get_wildcard_key(f's3://{s3_bucket}/abc', delimiter='/').key == 'abc'

        assert hook.get_wildcard_key('a', s3_bucket) is None
        assert hook.get_wildcard_key('b', s3_bucket) is None
        assert hook.get_wildcard_key(f's3://{s3_bucket}/a') is None
        assert hook.get_wildcard_key(f's3://{s3_bucket}/b') is None

    def test_load_string(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket)
        resource = boto3.resource('s3').Object(s3_bucket, 'my_key')  # pylint: disable=no-member
        assert resource.get()['Body'].read() == b'Cont\xC3\xA9nt'

    def test_load_string_compress(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket, compression='gzip')
        resource = boto3.resource('s3').Object(s3_bucket, 'my_key')  # pylint: disable=no-member
        data = gz.decompress(resource.get()['Body'].read())
        assert data == b'Cont\xC3\xA9nt'

    def test_load_string_compress_exception(self, s3_bucket):
        hook = S3Hook()
        with pytest.raises(NotImplementedError):
            hook.load_string("Contént", "my_key", s3_bucket, compression='bad-compression')

    def test_load_string_acl(self, s3_bucket):
        hook = S3Hook()
        hook.load_string("Contént", "my_key", s3_bucket, acl_policy='public-read')
        response = boto3.client('s3').get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer='requester')
        assert (response['Grants'][1]['Permission'] == 'READ') and (
            response['Grants'][0]['Permission'] == 'FULL_CONTROL'
        )

    def test_load_bytes(self, s3_bucket):
        hook = S3Hook()
        hook.load_bytes(b"Content", "my_key", s3_bucket)
        resource = boto3.resource('s3').Object(s3_bucket, 'my_key')  # pylint: disable=no-member
        assert resource.get()['Body'].read() == b'Content'

    def test_load_bytes_acl(self, s3_bucket):
        hook = S3Hook()
        hook.load_bytes(b"Content", "my_key", s3_bucket, acl_policy='public-read')
        response = boto3.client('s3').get_object_acl(Bucket=s3_bucket, Key="my_key", RequestPayer='requester')
        assert (response['Grants'][1]['Permission'] == 'READ') and (
            response['Grants'][0]['Permission'] == 'FULL_CONTROL'
        )

    def test_load_fileobj(self, s3_bucket):
        hook = S3Hook()
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket)
            resource = boto3.resource('s3').Object(s3_bucket, 'my_key')  # pylint: disable=no-member
            assert resource.get()['Body'].read() == b'Content'

    def test_load_fileobj_acl(self, s3_bucket):
        hook = S3Hook()
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket, acl_policy='public-read')
            response = boto3.client('s3').get_object_acl(
                Bucket=s3_bucket, Key="my_key", RequestPayer='requester'
            )  # pylint: disable=no-member # noqa: E501 # pylint: disable=C0301
            assert (response['Grants'][1]['Permission'] == 'READ') and (
                response['Grants'][0]['Permission'] == 'FULL_CONTROL'
            )

    def test_load_file_gzip(self, s3_bucket):
        hook = S3Hook()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file(temp_file.name, "my_key", s3_bucket, gzip=True)
            resource = boto3.resource('s3').Object(s3_bucket, 'my_key')  # pylint: disable=no-member
            assert gz.decompress(resource.get()['Body'].read()) == b'Content'
            os.unlink(temp_file.name)

    def test_load_file_acl(self, s3_bucket):
        hook = S3Hook()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file(temp_file.name, "my_key", s3_bucket, gzip=True, acl_policy='public-read')
            response = boto3.client('s3').get_object_acl(
                Bucket=s3_bucket, Key="my_key", RequestPayer='requester'
            )  # pylint: disable=no-member # noqa: E501 # pylint: disable=C0301
            assert (response['Grants'][1]['Permission'] == 'READ') and (
                response['Grants'][0]['Permission'] == 'FULL_CONTROL'
            )
            os.unlink(temp_file.name)

    def test_copy_object_acl(self, s3_bucket):
        hook = S3Hook()
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket)
            hook.copy_object("my_key", "my_key", s3_bucket, s3_bucket)
            response = boto3.client('s3').get_object_acl(
                Bucket=s3_bucket, Key="my_key", RequestPayer='requester'
            )  # pylint: disable=no-member # noqa: E501 # pylint: disable=C0301
            assert (response['Grants'][0]['Permission'] == 'FULL_CONTROL') and (len(response['Grants']) == 1)

    @mock_s3
    def test_delete_bucket_if_bucket_exist(self, s3_bucket):
        # assert if the bucket is created
        mock_hook = S3Hook()
        mock_hook.create_bucket(bucket_name=s3_bucket)
        assert mock_hook.check_for_bucket(bucket_name=s3_bucket)
        mock_hook.delete_bucket(bucket_name=s3_bucket, force_delete=True)
        assert not mock_hook.check_for_bucket(s3_bucket)

    @mock_s3
    def test_delete_bucket_if_not_bucket_exist(self, s3_bucket):
        # assert if exception is raised if bucket not present
        mock_hook = S3Hook()
        with pytest.raises(ClientError) as ctx:
            assert mock_hook.delete_bucket(bucket_name=s3_bucket, force_delete=True)
        assert ctx.value.response['Error']['Code'] == 'NoSuchBucket'

    @mock.patch.object(S3Hook, 'get_connection', return_value=Connection(schema='test_bucket'))
    def test_provide_bucket_name(self, mock_get_connection):
        class FakeS3Hook(S3Hook):
            @provide_bucket_name
            def test_function(self, bucket_name=None):
                return bucket_name

        fake_s3_hook = FakeS3Hook()

        test_bucket_name = fake_s3_hook.test_function()
        assert test_bucket_name == mock_get_connection.return_value.schema

        test_bucket_name = fake_s3_hook.test_function(bucket_name='bucket')
        assert test_bucket_name == 'bucket'

    def test_delete_objects_key_does_not_exist(self, s3_bucket):
        hook = S3Hook()
        with pytest.raises(AirflowException) as ctx:
            hook.delete_objects(bucket=s3_bucket, keys=['key-1'])

        assert isinstance(ctx.value, AirflowException)
        assert str(ctx.value) == "Errors when deleting: ['key-1']"

    def test_delete_objects_one_key(self, mocked_s3_res, s3_bucket):
        key = 'key-1'
        mocked_s3_res.Object(s3_bucket, key).put(Body=b'Data')
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=[key])
        assert [o.key for o in mocked_s3_res.Bucket(s3_bucket).objects.all()] == []

    def test_delete_objects_many_keys(self, mocked_s3_res, s3_bucket):
        num_keys_to_remove = 1001
        keys = []
        for index in range(num_keys_to_remove):
            key = f'key-{index}'
            mocked_s3_res.Object(s3_bucket, key).put(Body=b'Data')
            keys.append(key)

        assert sum(1 for _ in mocked_s3_res.Bucket(s3_bucket).objects.all()) == num_keys_to_remove
        hook = S3Hook()
        hook.delete_objects(bucket=s3_bucket, keys=keys)
        assert [o.key for o in mocked_s3_res.Bucket(s3_bucket).objects.all()] == []

    def test_unify_bucket_name_and_key(self):
        class FakeS3Hook(S3Hook):
            @unify_bucket_name_and_key
            def test_function_with_wildcard_key(self, wildcard_key, bucket_name=None):
                return bucket_name, wildcard_key

            @unify_bucket_name_and_key
            def test_function_with_key(self, key, bucket_name=None):
                return bucket_name, key

            @unify_bucket_name_and_key
            def test_function_with_test_key(self, test_key, bucket_name=None):
                return bucket_name, test_key

        fake_s3_hook = FakeS3Hook()

        test_bucket_name_with_wildcard_key = fake_s3_hook.test_function_with_wildcard_key('s3://foo/bar*.csv')
        assert ('foo', 'bar*.csv') == test_bucket_name_with_wildcard_key

        test_bucket_name_with_key = fake_s3_hook.test_function_with_key('s3://foo/bar.csv')
        assert ('foo', 'bar.csv') == test_bucket_name_with_key

        with pytest.raises(ValueError) as ctx:
            fake_s3_hook.test_function_with_test_key('s3://foo/bar.csv')
        assert isinstance(ctx.value, ValueError)

    @mock.patch('airflow.providers.amazon.aws.hooks.s3.NamedTemporaryFile')
    def test_download_file(self, mock_temp_file):
        mock_temp_file.return_value.__enter__ = Mock(return_value=mock_temp_file)
        s3_hook = S3Hook(aws_conn_id='s3_test')
        s3_hook.check_for_key = Mock(return_value=True)
        s3_obj = Mock()
        s3_obj.download_fileobj = Mock(return_value=None)
        s3_hook.get_key = Mock(return_value=s3_obj)
        key = 'test_key'
        bucket = 'test_bucket'

        s3_hook.download_file(key=key, bucket_name=bucket)

        s3_hook.check_for_key.assert_called_once_with(key, bucket)
        s3_hook.get_key.assert_called_once_with(key, bucket)
        s3_obj.download_fileobj.assert_called_once_with(mock_temp_file)

    def test_generate_presigned_url(self, s3_bucket):
        hook = S3Hook()
        presigned_url = hook.generate_presigned_url(
            client_method="get_object", params={'Bucket': s3_bucket, 'Key': "my_key"}
        )

        url = presigned_url.split("?")[1]
        params = {x[0]: x[1] for x in [x.split("=") for x in url[0:].split("&")]}

        assert {"AWSAccessKeyId", "Signature", "Expires"}.issubset(set(params.keys()))

    def test_should_throw_error_if_extra_args_is_not_dict(self):
        with pytest.raises(ValueError):
            S3Hook(extra_args=1)

    def test_should_throw_error_if_extra_args_contains_unknown_arg(self, s3_bucket):
        hook = S3Hook(extra_args={"unknown_s3_args": "value"})
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            with pytest.raises(ValueError):
                hook.load_file_obj(temp_file, "my_key", s3_bucket, acl_policy='public-read')

    def test_should_pass_extra_args(self, s3_bucket):
        hook = S3Hook(extra_args={"ContentLanguage": "value"})
        with tempfile.TemporaryFile() as temp_file:
            temp_file.write(b"Content")
            temp_file.seek(0)
            hook.load_file_obj(temp_file, "my_key", s3_bucket, acl_policy='public-read')
            resource = boto3.resource('s3').Object(s3_bucket, 'my_key')  # pylint: disable=no-member
            assert resource.get()['ContentLanguage'] == "value"

    @mock_s3
    def test_get_bucket_tagging_no_tags_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')

        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name='new_bucket')

    @mock_s3
    def test_get_bucket_tagging_no_bucket_raises_error(self):
        hook = S3Hook()

        with pytest.raises(ClientError, match=r".*NoSuchBucket.*"):
            hook.get_bucket_tagging(bucket_name='new_bucket')

    @mock_s3
    def test_put_bucket_tagging_with_valid_set(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        hook.put_bucket_tagging(bucket_name='new_bucket', tag_set=tag_set)

        assert hook.get_bucket_tagging(bucket_name='new_bucket') == tag_set

    @mock_s3
    def test_put_bucket_tagging_with_pair(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        key = 'Color'
        value = 'Green'
        hook.put_bucket_tagging(bucket_name='new_bucket', key=key, value=value)

        assert hook.get_bucket_tagging(bucket_name='new_bucket') == tag_set

    @mock_s3
    def test_put_bucket_tagging_with_pair_and_set(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        expected = [{'Key': 'Color', 'Value': 'Green'}, {'Key': 'Fruit', 'Value': 'Apple'}]
        tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        key = 'Fruit'
        value = 'Apple'
        hook.put_bucket_tagging(bucket_name='new_bucket', tag_set=tag_set, key=key, value=value)

        result = hook.get_bucket_tagging(bucket_name='new_bucket')
        assert len(result) == 2
        assert result == expected

    @mock_s3
    def test_put_bucket_tagging_with_key_but_no_value_raises_error(self):
        hook = S3Hook()

        hook.create_bucket(bucket_name='new_bucket')
        key = 'Color'
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name='new_bucket', key=key)

    @mock_s3
    def test_put_bucket_tagging_with_value_but_no_key_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        value = 'Color'
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name='new_bucket', value=value)

    @mock_s3
    def test_put_bucket_tagging_with_key_and_set_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        key = 'Color'
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name='new_bucket', key=key, tag_set=tag_set)

    @mock_s3
    def test_put_bucket_tagging_with_value_and_set_raises_error(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        value = 'Green'
        with pytest.raises(ValueError):
            hook.put_bucket_tagging(bucket_name='new_bucket', value=value, tag_set=tag_set)

    @mock_s3
    def test_put_bucket_tagging_when_tags_exist_overwrites(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        initial_tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        hook.put_bucket_tagging(bucket_name='new_bucket', tag_set=initial_tag_set)
        assert len(hook.get_bucket_tagging(bucket_name='new_bucket')) == 1
        assert hook.get_bucket_tagging(bucket_name='new_bucket') == initial_tag_set

        new_tag_set = [{'Key': 'Fruit', 'Value': 'Apple'}]
        hook.put_bucket_tagging(bucket_name='new_bucket', tag_set=new_tag_set)

        result = hook.get_bucket_tagging(bucket_name='new_bucket')
        assert len(result) == 1
        assert result == new_tag_set

    @mock_s3
    def test_delete_bucket_tagging(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')
        tag_set = [{'Key': 'Color', 'Value': 'Green'}]
        hook.put_bucket_tagging(bucket_name='new_bucket', tag_set=tag_set)
        hook.get_bucket_tagging(bucket_name='new_bucket')

        hook.delete_bucket_tagging(bucket_name='new_bucket')
        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name='new_bucket')

    @mock_s3
    def test_delete_bucket_tagging_with_no_tags(self):
        hook = S3Hook()
        hook.create_bucket(bucket_name='new_bucket')

        hook.delete_bucket_tagging(bucket_name='new_bucket')

        with pytest.raises(ClientError, match=r".*NoSuchTagSet.*"):
            hook.get_bucket_tagging(bucket_name='new_bucket')
