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
"""Default celery configuration."""
import logging
import ssl

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException


def _broker_supports_visibility_timeout(url):
    return url.startswith("redis://") or url.startswith("sqs://")


log = logging.getLogger(__name__)

broker_url = conf.get('celery', 'BROKER_URL')

broker_transport_options = conf.getsection('celery_broker_transport_options') or {}
if 'visibility_timeout' not in broker_transport_options:
    if _broker_supports_visibility_timeout(broker_url):
        broker_transport_options['visibility_timeout'] = 21600

DEFAULT_CELERY_CONFIG = {
    'accept_content': ['json'],
    'event_serializer': 'json',
    'worker_prefetch_multiplier': conf.getint('celery', 'worker_prefetch_multiplier', fallback=1),
    'task_acks_late': True,
    'task_default_queue': conf.get('operators', 'DEFAULT_QUEUE'),
    'task_default_exchange': conf.get('operators', 'DEFAULT_QUEUE'),
    'task_track_started': conf.get('celery', 'task_track_started', fallback=True),
    'broker_url': broker_url,
    'broker_transport_options': broker_transport_options,
    'result_backend': conf.get('celery', 'RESULT_BACKEND'),
    'worker_concurrency': conf.getint('celery', 'WORKER_CONCURRENCY'),
}

celery_ssl_active = False
try:
    celery_ssl_active = conf.getboolean('celery', 'SSL_ACTIVE')
except AirflowConfigException:
    log.warning("Celery Executor will run without SSL")

try:
    if celery_ssl_active:
        if 'amqp://' in broker_url:
            broker_use_ssl = {
                'keyfile': conf.get('celery', 'SSL_KEY'),
                'certfile': conf.get('celery', 'SSL_CERT'),
                'ca_certs': conf.get('celery', 'SSL_CACERT'),
                'cert_reqs': ssl.CERT_REQUIRED,
            }
        elif 'redis://' in broker_url:
            broker_use_ssl = {
                'ssl_keyfile': conf.get('celery', 'SSL_KEY'),
                'ssl_certfile': conf.get('celery', 'SSL_CERT'),
                'ssl_ca_certs': conf.get('celery', 'SSL_CACERT'),
                'ssl_cert_reqs': ssl.CERT_REQUIRED,
            }
        else:
            raise AirflowException(
                'The broker you configured does not support SSL_ACTIVE to be True. '
                'Please use RabbitMQ or Redis if you would like to use SSL for broker.'
            )

        DEFAULT_CELERY_CONFIG['broker_use_ssl'] = broker_use_ssl
except AirflowConfigException:
    raise AirflowException(
        'AirflowConfigException: SSL_ACTIVE is True, '
        'please ensure SSL_KEY, '
        'SSL_CERT and SSL_CACERT are set'
    )
except Exception as e:
    raise AirflowException(
        'Exception: There was an unknown Celery SSL Error. '
        'Please ensure you want to use '
        'SSL and/or have all necessary certs and key ({}).'.format(e)
    )

result_backend = DEFAULT_CELERY_CONFIG['result_backend']
if 'amqp://' in result_backend or 'redis://' in result_backend or 'rpc://' in result_backend:
    log.warning(
        "You have configured a result_backend of %s, it is highly recommended "
        "to use an alternative result_backend (i.e. a database).",
        result_backend,
    )
