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

import time
import unittest
from datetime import datetime
from unittest import mock

import pytest
from dateutil.tz import tzlocal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sagemaker import (
    LogState,
    SageMakerHook,
    secondary_training_status_changed,
    secondary_training_status_message,
)

role = 'arn:aws:iam:role/test-role'

path = 'local/data'
bucket = 'test-bucket'
key = 'test/data'
data_url = f's3://{bucket}/{key}'

job_name = 'test-job'
model_name = 'test-model'
config_name = 'test-endpoint-config'
endpoint_name = 'test-endpoint'

image = 'test-image'
test_arn_return = {'Arn': 'testarn'}
output_url = f's3://{bucket}/test/output'

create_training_params = {
    'AlgorithmSpecification': {'TrainingImage': image, 'TrainingInputMode': 'File'},
    'RoleArn': role,
    'OutputDataConfig': {'S3OutputPath': output_url},
    'ResourceConfig': {'InstanceCount': 2, 'InstanceType': 'ml.c4.8xlarge', 'VolumeSizeInGB': 50},
    'TrainingJobName': job_name,
    'HyperParameters': {'k': '10', 'feature_dim': '784', 'mini_batch_size': '500', 'force_dense': 'True'},
    'StoppingCondition': {'MaxRuntimeInSeconds': 60 * 60},
    'InputDataConfig': [
        {
            'ChannelName': 'train',
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': data_url,
                    'S3DataDistributionType': 'FullyReplicated',
                }
            },
            'CompressionType': 'None',
            'RecordWrapperType': 'None',
        },
        {
            'ChannelName': 'train_fs',
            'DataSource': {
                'FileSystemDataSource': {
                    'DirectoryPath': '/tmp',
                    'FileSystemAccessMode': 'ro',
                    'FileSystemId': 'fs-abc',
                    'FileSystemType': 'FSxLustre',
                }
            },
            'CompressionType': 'None',
            'RecordWrapperType': 'None',
        },
    ],
}

create_tuning_params = {
    'HyperParameterTuningJobName': job_name,
    'HyperParameterTuningJobConfig': {
        'Strategy': 'Bayesian',
        'HyperParameterTuningJobObjective': {'Type': 'Maximize', 'MetricName': 'test_metric'},
        'ResourceLimits': {'MaxNumberOfTrainingJobs': 123, 'MaxParallelTrainingJobs': 123},
        'ParameterRanges': {
            'IntegerParameterRanges': [
                {'Name': 'k', 'MinValue': '2', 'MaxValue': '10'},
            ]
        },
    },
    'TrainingJobDefinition': {
        'StaticHyperParameters': create_training_params['HyperParameters'],
        'AlgorithmSpecification': create_training_params['AlgorithmSpecification'],
        'RoleArn': 'string',
        'InputDataConfig': create_training_params['InputDataConfig'],
        'OutputDataConfig': create_training_params['OutputDataConfig'],
        'ResourceConfig': create_training_params['ResourceConfig'],
        'StoppingCondition': dict(MaxRuntimeInSeconds=60 * 60),
    },
}

create_transform_params = {
    'TransformJobName': job_name,
    'ModelName': model_name,
    'BatchStrategy': 'MultiRecord',
    'TransformInput': {'DataSource': {'S3DataSource': {'S3DataType': 'S3Prefix', 'S3Uri': data_url}}},
    'TransformOutput': {
        'S3OutputPath': output_url,
    },
    'TransformResources': {'InstanceType': 'ml.m4.xlarge', 'InstanceCount': 123},
}

create_transform_params_fs = {
    'TransformJobName': job_name,
    'ModelName': model_name,
    'BatchStrategy': 'MultiRecord',
    'TransformInput': {
        'DataSource': {
            'FileSystemDataSource': {
                'DirectoryPath': '/tmp',
                'FileSystemAccessMode': 'ro',
                'FileSystemId': 'fs-abc',
                'FileSystemType': 'FSxLustre',
            }
        }
    },
    'TransformOutput': {
        'S3OutputPath': output_url,
    },
    'TransformResources': {'InstanceType': 'ml.m4.xlarge', 'InstanceCount': 123},
}

create_model_params = {
    'ModelName': model_name,
    'PrimaryContainer': {
        'Image': image,
        'ModelDataUrl': output_url,
    },
    'ExecutionRoleArn': role,
}

create_endpoint_config_params = {
    'EndpointConfigName': config_name,
    'ProductionVariants': [
        {
            'VariantName': 'AllTraffic',
            'ModelName': model_name,
            'InitialInstanceCount': 1,
            'InstanceType': 'ml.c4.xlarge',
        }
    ],
}

create_endpoint_params = {'EndpointName': endpoint_name, 'EndpointConfigName': config_name}

update_endpoint_params = create_endpoint_params

DESCRIBE_TRAINING_COMPLETED_RETURN = {
    'TrainingJobStatus': 'Completed',
    'ResourceConfig': {'InstanceCount': 1, 'InstanceType': 'ml.c4.xlarge', 'VolumeSizeInGB': 10},
    'TrainingStartTime': datetime(2018, 2, 17, 7, 15, 0, 103000),
    'TrainingEndTime': datetime(2018, 2, 17, 7, 19, 34, 953000),
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
    },
}

DESCRIBE_TRAINING_INPROGRESS_RETURN = dict(DESCRIBE_TRAINING_COMPLETED_RETURN)
DESCRIBE_TRAINING_INPROGRESS_RETURN.update({'TrainingJobStatus': 'InProgress'})

DESCRIBE_TRAINING_FAILED_RETURN = dict(DESCRIBE_TRAINING_COMPLETED_RETURN)
DESCRIBE_TRAINING_FAILED_RETURN.update({'TrainingJobStatus': 'Failed', 'FailureReason': 'Unknown'})

DESCRIBE_TRAINING_STOPPING_RETURN = dict(DESCRIBE_TRAINING_COMPLETED_RETURN)
DESCRIBE_TRAINING_STOPPING_RETURN.update({'TrainingJobStatus': 'Stopping'})

message = 'message'
status = 'status'
SECONDARY_STATUS_DESCRIPTION_1 = {
    'SecondaryStatusTransitions': [{'StatusMessage': message, 'Status': status}]
}
SECONDARY_STATUS_DESCRIPTION_2 = {
    'SecondaryStatusTransitions': [{'StatusMessage': 'different message', 'Status': status}]
}

DEFAULT_LOG_STREAMS = {'logStreams': [{'logStreamName': job_name + '/xxxxxxxxx'}]}
LIFECYCLE_LOG_STREAMS = [
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
]

DEFAULT_LOG_EVENTS = [
    {'nextForwardToken': None, 'events': [{'timestamp': 1, 'message': 'hi there #1'}]},
    {'nextForwardToken': None, 'events': []},
]
STREAM_LOG_EVENTS = [
    {'nextForwardToken': None, 'events': [{'timestamp': 1, 'message': 'hi there #1'}]},
    {'nextForwardToken': None, 'events': []},
    {
        'nextForwardToken': None,
        'events': [{'timestamp': 1, 'message': 'hi there #1'}, {'timestamp': 2, 'message': 'hi there #2'}],
    },
    {'nextForwardToken': None, 'events': []},
    {
        'nextForwardToken': None,
        'events': [
            {'timestamp': 2, 'message': 'hi there #2'},
            {'timestamp': 2, 'message': 'hi there #2a'},
            {'timestamp': 3, 'message': 'hi there #3'},
        ],
    },
    {'nextForwardToken': None, 'events': []},
]

test_evaluation_config = {
    'Image': image,
    'Role': role,
    'S3Operations': {
        'S3CreateBucket': [{'Bucket': bucket}],
        'S3Upload': [{'Path': path, 'Bucket': bucket, 'Key': key, 'Tar': False}],
    },
}


class TestSageMakerHook(unittest.TestCase):
    @mock.patch.object(AwsLogsHook, 'get_log_events')
    def test_multi_stream_iter(self, mock_log_stream):
        event = {'timestamp': 1}
        mock_log_stream.side_effect = [iter([event]), iter([]), None]
        hook = SageMakerHook()
        event_iter = hook.multi_stream_iter('log', [None, None, None])
        assert next(event_iter) == (0, event)

    @mock.patch.object(S3Hook, 'create_bucket')
    @mock.patch.object(S3Hook, 'load_file')
    def test_configure_s3_resources(self, mock_load_file, mock_create_bucket):
        hook = SageMakerHook()
        evaluation_result = {'Image': image, 'Role': role}
        hook.configure_s3_resources(test_evaluation_config)
        assert test_evaluation_config == evaluation_result
        mock_create_bucket.assert_called_once_with(bucket_name=bucket)
        mock_load_file.assert_called_once_with(path, key, bucket)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(S3Hook, 'check_for_key')
    @mock.patch.object(S3Hook, 'check_for_bucket')
    @mock.patch.object(S3Hook, 'check_for_prefix')
    def test_check_s3_url(self, mock_check_prefix, mock_check_bucket, mock_check_key, mock_client):
        mock_client.return_value = None
        hook = SageMakerHook()
        mock_check_bucket.side_effect = [False, True, True, True]
        mock_check_key.side_effect = [False, True, False]
        mock_check_prefix.side_effect = [False, True, True]
        with pytest.raises(AirflowException):
            hook.check_s3_url(data_url)
        with pytest.raises(AirflowException):
            hook.check_s3_url(data_url)
        assert hook.check_s3_url(data_url) is True
        assert hook.check_s3_url(data_url) is True

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'check_s3_url')
    def test_check_valid_training(self, mock_check_url, mock_client):
        mock_client.return_value = None
        hook = SageMakerHook()
        hook.check_training_config(create_training_params)
        mock_check_url.assert_called_once_with(data_url)

        # InputDataConfig is optional, verify if check succeeds without InputDataConfig
        create_training_params_no_inputdataconfig = create_training_params.copy()
        create_training_params_no_inputdataconfig.pop("InputDataConfig")
        hook.check_training_config(create_training_params_no_inputdataconfig)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'check_s3_url')
    def test_check_valid_tuning(self, mock_check_url, mock_client):
        mock_client.return_value = None
        hook = SageMakerHook()
        hook.check_tuning_config(create_tuning_params)
        mock_check_url.assert_called_once_with(data_url)

    @mock.patch.object(SageMakerHook, 'get_client_type')
    def test_conn(self, mock_get_client_type):
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        assert hook.aws_conn_id == 'sagemaker_test_conn_id'

    @mock.patch.object(SageMakerHook, 'check_training_config')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_training_job(self, mock_client, mock_check_training):
        mock_check_training.return_value = True
        mock_session = mock.Mock()
        attrs = {'create_training_job.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_training_job(
            create_training_params, wait_for_completion=False, print_log=False
        )
        mock_session.create_training_job.assert_called_once_with(**create_training_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'check_training_config')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_training_ends_with_wait(self, mock_client, mock_check_training):
        mock_check_training.return_value = True
        mock_session = mock.Mock()
        attrs = {
            'create_training_job.return_value': test_arn_return,
            'describe_training_job.side_effect': [
                DESCRIBE_TRAINING_INPROGRESS_RETURN,
                DESCRIBE_TRAINING_STOPPING_RETURN,
                DESCRIBE_TRAINING_COMPLETED_RETURN,
                DESCRIBE_TRAINING_COMPLETED_RETURN,
            ],
        }
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id_1')
        hook.create_training_job(
            create_training_params, wait_for_completion=True, print_log=False, check_interval=1
        )
        assert mock_session.describe_training_job.call_count == 4

    @mock.patch.object(SageMakerHook, 'check_training_config')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_training_throws_error_when_failed_with_wait(self, mock_client, mock_check_training):
        mock_check_training.return_value = True
        mock_session = mock.Mock()
        attrs = {
            'create_training_job.return_value': test_arn_return,
            'describe_training_job.side_effect': [
                DESCRIBE_TRAINING_INPROGRESS_RETURN,
                DESCRIBE_TRAINING_STOPPING_RETURN,
                DESCRIBE_TRAINING_FAILED_RETURN,
                DESCRIBE_TRAINING_COMPLETED_RETURN,
            ],
        }
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id_1')
        with pytest.raises(AirflowException):
            hook.create_training_job(
                create_training_params,
                wait_for_completion=True,
                print_log=False,
                check_interval=1,
            )
        assert mock_session.describe_training_job.call_count == 3

    @mock.patch.object(SageMakerHook, 'check_tuning_config')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_tuning_job(self, mock_client, mock_check_tuning_config):
        mock_session = mock.Mock()
        attrs = {'create_hyper_parameter_tuning_job.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_tuning_job(create_tuning_params, wait_for_completion=False)
        mock_session.create_hyper_parameter_tuning_job.assert_called_once_with(**create_tuning_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'check_s3_url')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_transform_job(self, mock_client, mock_check_url):
        mock_check_url.return_value = True
        mock_session = mock.Mock()
        attrs = {'create_transform_job.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_transform_job(create_transform_params, wait_for_completion=False)
        mock_session.create_transform_job.assert_called_once_with(**create_transform_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_transform_job_fs(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'create_transform_job.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_transform_job(create_transform_params_fs, wait_for_completion=False)
        mock_session.create_transform_job.assert_called_once_with(**create_transform_params_fs)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_model(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'create_model.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_model(create_model_params)
        mock_session.create_model.assert_called_once_with(**create_model_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_endpoint_config(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'create_endpoint_config.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_endpoint_config(create_endpoint_config_params)
        mock_session.create_endpoint_config.assert_called_once_with(**create_endpoint_config_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_endpoint(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'create_endpoint.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.create_endpoint(create_endpoint_params, wait_for_completion=False)
        mock_session.create_endpoint.assert_called_once_with(**create_endpoint_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_update_endpoint(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'update_endpoint.return_value': test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.update_endpoint(update_endpoint_params, wait_for_completion=False)
        mock_session.update_endpoint.assert_called_once_with(**update_endpoint_params)
        assert response == test_arn_return

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_training_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_training_job.return_value': 'InProgress'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_training_job(job_name)
        mock_session.describe_training_job.assert_called_once_with(TrainingJobName=job_name)
        assert response == 'InProgress'

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_tuning_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_hyper_parameter_tuning_job.return_value': 'InProgress'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_tuning_job(job_name)
        mock_session.describe_hyper_parameter_tuning_job.assert_called_once_with(
            HyperParameterTuningJobName=job_name
        )
        assert response == 'InProgress'

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_transform_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_transform_job.return_value': 'InProgress'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_transform_job(job_name)
        mock_session.describe_transform_job.assert_called_once_with(TransformJobName=job_name)
        assert response == 'InProgress'

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_model(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_model.return_value': model_name}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_model(model_name)
        mock_session.describe_model.assert_called_once_with(ModelName=model_name)
        assert response == model_name

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_endpoint_config(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_endpoint_config.return_value': config_name}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_endpoint_config(config_name)
        mock_session.describe_endpoint_config.assert_called_once_with(EndpointConfigName=config_name)
        assert response == config_name

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_endpoint(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_endpoint.return_value': 'InProgress'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_endpoint(endpoint_name)
        mock_session.describe_endpoint.assert_called_once_with(EndpointName=endpoint_name)
        assert response == 'InProgress'

    def test_secondary_training_status_changed_true(self):
        changed = secondary_training_status_changed(
            SECONDARY_STATUS_DESCRIPTION_1, SECONDARY_STATUS_DESCRIPTION_2
        )
        assert changed

    def test_secondary_training_status_changed_false(self):
        changed = secondary_training_status_changed(
            SECONDARY_STATUS_DESCRIPTION_1, SECONDARY_STATUS_DESCRIPTION_1
        )
        assert not changed

    def test_secondary_training_status_message_status_changed(self):
        now = datetime.now(tzlocal())
        SECONDARY_STATUS_DESCRIPTION_1['LastModifiedTime'] = now
        expected = '{} {} - {}'.format(
            datetime.utcfromtimestamp(time.mktime(now.timetuple())).strftime('%Y-%m-%d %H:%M:%S'),
            status,
            message,
        )
        assert (
            secondary_training_status_message(SECONDARY_STATUS_DESCRIPTION_1, SECONDARY_STATUS_DESCRIPTION_2)
            == expected
        )

    @mock.patch.object(AwsLogsHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(time, 'monotonic')
    def test_describe_training_job_with_logs_in_progress(self, mock_time, mock_client, mock_log_client):
        mock_session = mock.Mock()
        mock_log_session = mock.Mock()
        attrs = {'describe_training_job.return_value': DESCRIBE_TRAINING_COMPLETED_RETURN}
        log_attrs = {
            'describe_log_streams.side_effect': LIFECYCLE_LOG_STREAMS,
            'get_log_events.side_effect': STREAM_LOG_EVENTS,
        }
        mock_time.return_value = 50
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        mock_log_session.configure_mock(**log_attrs)
        mock_log_client.return_value = mock_log_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_training_job_with_log(
            job_name=job_name,
            positions={},
            stream_names=[],
            instance_count=1,
            state=LogState.WAIT_IN_PROGRESS,
            last_description={},
            last_describe_job_call=0,
        )
        assert response == (LogState.JOB_COMPLETE, {}, 50)

    @mock.patch.object(AwsLogsHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_training_job_with_logs_job_complete(self, mock_client, mock_log_client):
        mock_session = mock.Mock()
        mock_log_session = mock.Mock()
        attrs = {'describe_training_job.return_value': DESCRIBE_TRAINING_COMPLETED_RETURN}
        log_attrs = {
            'describe_log_streams.side_effect': LIFECYCLE_LOG_STREAMS,
            'get_log_events.side_effect': STREAM_LOG_EVENTS,
        }
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        mock_log_session.configure_mock(**log_attrs)
        mock_log_client.return_value = mock_log_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_training_job_with_log(
            job_name=job_name,
            positions={},
            stream_names=[],
            instance_count=1,
            state=LogState.JOB_COMPLETE,
            last_description={},
            last_describe_job_call=0,
        )
        assert response == (LogState.COMPLETE, {}, 0)

    @mock.patch.object(AwsLogsHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_training_job_with_logs_complete(self, mock_client, mock_log_client):
        mock_session = mock.Mock()
        mock_log_session = mock.Mock()
        attrs = {'describe_training_job.return_value': DESCRIBE_TRAINING_COMPLETED_RETURN}
        log_attrs = {
            'describe_log_streams.side_effect': LIFECYCLE_LOG_STREAMS,
            'get_log_events.side_effect': STREAM_LOG_EVENTS,
        }
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        mock_log_session.configure_mock(**log_attrs)
        mock_log_client.return_value = mock_log_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id')
        response = hook.describe_training_job_with_log(
            job_name=job_name,
            positions={},
            stream_names=[],
            instance_count=1,
            state=LogState.COMPLETE,
            last_description={},
            last_describe_job_call=0,
        )
        assert response == (LogState.COMPLETE, {}, 0)

    @mock.patch.object(SageMakerHook, 'check_training_config')
    @mock.patch.object(AwsLogsHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'describe_training_job_with_log')
    def test_training_with_logs(self, mock_describe, mock_client, mock_log_client, mock_check_training):
        mock_check_training.return_value = True
        mock_describe.side_effect = [
            (LogState.WAIT_IN_PROGRESS, DESCRIBE_TRAINING_INPROGRESS_RETURN, 0),
            (LogState.JOB_COMPLETE, DESCRIBE_TRAINING_STOPPING_RETURN, 0),
            (LogState.COMPLETE, DESCRIBE_TRAINING_COMPLETED_RETURN, 0),
        ]
        mock_session = mock.Mock()
        mock_log_session = mock.Mock()
        attrs = {
            'create_training_job.return_value': test_arn_return,
            'describe_training_job.return_value': DESCRIBE_TRAINING_COMPLETED_RETURN,
        }
        log_attrs = {
            'describe_log_streams.side_effect': LIFECYCLE_LOG_STREAMS,
            'get_log_events.side_effect': STREAM_LOG_EVENTS,
        }
        mock_session.configure_mock(**attrs)
        mock_log_session.configure_mock(**log_attrs)
        mock_client.return_value = mock_session
        mock_log_client.return_value = mock_log_session
        hook = SageMakerHook(aws_conn_id='sagemaker_test_conn_id_1')
        hook.create_training_job(
            create_training_params, wait_for_completion=True, print_log=True, check_interval=1
        )
        assert mock_describe.call_count == 3
        assert mock_session.describe_training_job.call_count == 1
