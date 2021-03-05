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
"""Airflow logging settings"""

import os
from pathlib import Path
from typing import Any, Dict, Union
from urllib.parse import urlparse

from airflow.configuration import conf
from airflow.exceptions import AirflowException

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py. Please see AIRFLOW-1455.
LOG_LEVEL: str = conf.get('logging', 'LOGGING_LEVEL').upper()


# Flask appbuilder's info level log is very verbose,
# so it's set to 'WARN' by default.
FAB_LOG_LEVEL: str = conf.get('logging', 'FAB_LOGGING_LEVEL').upper()

LOG_FORMAT: str = conf.get('logging', 'LOG_FORMAT')

COLORED_LOG_FORMAT: str = conf.get('logging', 'COLORED_LOG_FORMAT')

COLORED_LOG: bool = conf.getboolean('logging', 'COLORED_CONSOLE_LOG')

COLORED_FORMATTER_CLASS: str = conf.get('logging', 'COLORED_FORMATTER_CLASS')

BASE_LOG_FOLDER: str = conf.get('logging', 'BASE_LOG_FOLDER')

PROCESSOR_LOG_FOLDER: str = conf.get('scheduler', 'CHILD_PROCESS_LOG_DIRECTORY')

DAG_PROCESSOR_MANAGER_LOG_LOCATION: str = conf.get('logging', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')

FILENAME_TEMPLATE: str = conf.get('logging', 'LOG_FILENAME_TEMPLATE')

PROCESSOR_FILENAME_TEMPLATE: str = conf.get('logging', 'LOG_PROCESSOR_FILENAME_TEMPLATE')

DEFAULT_LOGGING_CONFIG: Dict[str, Any] = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow': {'format': LOG_FORMAT},
        'airflow_coloured': {
            'format': COLORED_LOG_FORMAT if COLORED_LOG else LOG_FORMAT,
            'class': COLORED_FORMATTER_CLASS if COLORED_LOG else 'logging.Formatter',
        },
    },
    'handlers': {
        'console': {
            'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
            'formatter': 'airflow_coloured',
            'stream': 'sys.stdout',
        },
        'task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        'processor': {
            'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        },
    },
    'loggers': {
        'airflow.processor': {
            'handlers': ['processor'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task': {
            'handlers': ['task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'flask_appbuilder': {
            'handler': ['console'],
            'level': FAB_LOG_LEVEL,
            'propagate': True,
        },
    },
    'root': {
        'handlers': ['console'],
        'level': LOG_LEVEL,
    },
}

EXTRA_LOGGER_NAMES: str = conf.get('logging', 'EXTRA_LOGGER_NAMES', fallback=None)
if EXTRA_LOGGER_NAMES:
    new_loggers = {
        logger_name.strip(): {
            'handler': ['console'],
            'level': LOG_LEVEL,
            'propagate': True,
        }
        for logger_name in EXTRA_LOGGER_NAMES.split(",")
    }
    DEFAULT_LOGGING_CONFIG['loggers'].update(new_loggers)

DEFAULT_DAG_PARSING_LOGGING_CONFIG: Dict[str, Dict[str, Dict[str, Any]]] = {
    'handlers': {
        'processor_manager': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'airflow',
            'filename': DAG_PROCESSOR_MANAGER_LOG_LOCATION,
            'mode': 'a',
            'maxBytes': 104857600,  # 100MB
            'backupCount': 5,
        }
    },
    'loggers': {
        'airflow.processor_manager': {
            'handlers': ['processor_manager'],
            'level': LOG_LEVEL,
            'propagate': False,
        }
    },
}

# Only update the handlers and loggers when CONFIG_PROCESSOR_MANAGER_LOGGER is set.
# This is to avoid exceptions when initializing RotatingFileHandler multiple times
# in multiple processes.
if os.environ.get('CONFIG_PROCESSOR_MANAGER_LOGGER') == 'True':
    DEFAULT_LOGGING_CONFIG['handlers'].update(DEFAULT_DAG_PARSING_LOGGING_CONFIG['handlers'])
    DEFAULT_LOGGING_CONFIG['loggers'].update(DEFAULT_DAG_PARSING_LOGGING_CONFIG['loggers'])

    # Manually create log directory for processor_manager handler as RotatingFileHandler
    # will only create file but not the directory.
    processor_manager_handler_config: Dict[str, Any] = DEFAULT_DAG_PARSING_LOGGING_CONFIG['handlers'][
        'processor_manager'
    ]
    directory: str = os.path.dirname(processor_manager_handler_config['filename'])
    Path(directory).mkdir(parents=True, exist_ok=True, mode=0o755)

##################
# Remote logging #
##################

REMOTE_LOGGING: bool = conf.getboolean('logging', 'remote_logging')

if REMOTE_LOGGING:

    ELASTICSEARCH_HOST: str = conf.get('elasticsearch', 'HOST')

    # Storage bucket URL for remote logging
    # S3 buckets should start with "s3://"
    # Cloudwatch log groups should start with "cloudwatch://"
    # GCS buckets should start with "gs://"
    # WASB buckets should start with "wasb"
    # just to help Airflow select correct handler
    REMOTE_BASE_LOG_FOLDER: str = conf.get('logging', 'REMOTE_BASE_LOG_FOLDER')

    if REMOTE_BASE_LOG_FOLDER.startswith('s3://'):
        S3_REMOTE_HANDLERS: Dict[str, Dict[str, str]] = {
            'task': {
                'class': 'airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler',
                'formatter': 'airflow',
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                's3_log_folder': REMOTE_BASE_LOG_FOLDER,
                'filename_template': FILENAME_TEMPLATE,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(S3_REMOTE_HANDLERS)
    elif REMOTE_BASE_LOG_FOLDER.startswith('cloudwatch://'):
        CLOUDWATCH_REMOTE_HANDLERS: Dict[str, Dict[str, str]] = {
            'task': {
                'class': 'airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudwatchTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                'log_group_arn': urlparse(REMOTE_BASE_LOG_FOLDER).netloc,
                'filename_template': FILENAME_TEMPLATE,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(CLOUDWATCH_REMOTE_HANDLERS)
    elif REMOTE_BASE_LOG_FOLDER.startswith('gs://'):
        key_path = conf.get('logging', 'GOOGLE_KEY_PATH', fallback=None)
        GCS_REMOTE_HANDLERS: Dict[str, Dict[str, str]] = {
            'task': {
                'class': 'airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                'gcs_log_folder': REMOTE_BASE_LOG_FOLDER,
                'filename_template': FILENAME_TEMPLATE,
                'gcp_key_path': key_path,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(GCS_REMOTE_HANDLERS)
    elif REMOTE_BASE_LOG_FOLDER.startswith('wasb'):
        WASB_REMOTE_HANDLERS: Dict[str, Dict[str, Union[str, bool]]] = {
            'task': {
                'class': 'airflow.providers.microsoft.azure.log.wasb_task_handler.WasbTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                'wasb_log_folder': REMOTE_BASE_LOG_FOLDER,
                'wasb_container': 'airflow-logs',
                'filename_template': FILENAME_TEMPLATE,
                'delete_local_copy': False,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(WASB_REMOTE_HANDLERS)
    elif REMOTE_BASE_LOG_FOLDER.startswith('stackdriver://'):
        key_path = conf.get('logging', 'GOOGLE_KEY_PATH', fallback=None)
        # stackdriver:///airflow-tasks => airflow-tasks
        log_name = urlparse(REMOTE_BASE_LOG_FOLDER).path[1:]
        STACKDRIVER_REMOTE_HANDLERS = {
            'task': {
                'class': 'airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverTaskHandler',
                'formatter': 'airflow',
                'name': log_name,
                'gcp_key_path': key_path,
            }
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(STACKDRIVER_REMOTE_HANDLERS)
    elif ELASTICSEARCH_HOST:
        ELASTICSEARCH_LOG_ID_TEMPLATE: str = conf.get('elasticsearch', 'LOG_ID_TEMPLATE')
        ELASTICSEARCH_END_OF_LOG_MARK: str = conf.get('elasticsearch', 'END_OF_LOG_MARK')
        ELASTICSEARCH_FRONTEND: str = conf.get('elasticsearch', 'frontend')
        ELASTICSEARCH_WRITE_STDOUT: bool = conf.getboolean('elasticsearch', 'WRITE_STDOUT')
        ELASTICSEARCH_JSON_FORMAT: bool = conf.getboolean('elasticsearch', 'JSON_FORMAT')
        ELASTICSEARCH_JSON_FIELDS: str = conf.get('elasticsearch', 'JSON_FIELDS')
        ELASTICSEARCH_HOST_FIELD: str = conf.get('elasticsearch', 'HOST_FIELD')
        ELASTICSEARCH_OFFSET_FIELD: str = conf.get('elasticsearch', 'OFFSET_FIELD')

        ELASTIC_REMOTE_HANDLERS: Dict[str, Dict[str, Union[str, bool]]] = {
            'task': {
                'class': 'airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                'log_id_template': ELASTICSEARCH_LOG_ID_TEMPLATE,
                'filename_template': FILENAME_TEMPLATE,
                'end_of_log_mark': ELASTICSEARCH_END_OF_LOG_MARK,
                'host': ELASTICSEARCH_HOST,
                'frontend': ELASTICSEARCH_FRONTEND,
                'write_stdout': ELASTICSEARCH_WRITE_STDOUT,
                'json_format': ELASTICSEARCH_JSON_FORMAT,
                'json_fields': ELASTICSEARCH_JSON_FIELDS,
                'host_field': ELASTICSEARCH_HOST_FIELD,
                'offset_field': ELASTICSEARCH_OFFSET_FIELD,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(ELASTIC_REMOTE_HANDLERS)
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
            "'remote_base_log_folder' option in the 'logging' section."
        )
