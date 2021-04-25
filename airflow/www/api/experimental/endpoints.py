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
import logging
from functools import wraps
from typing import Callable, TypeVar, cast

from flask import Blueprint, Response, current_app, g, jsonify, request, url_for

from airflow import models
from airflow.api.common.experimental import delete_dag as delete, pool as pool_api, trigger_dag as trigger
from airflow.api.common.experimental.get_code import get_code
from airflow.api.common.experimental.get_dag_run_state import get_dag_run_state
from airflow.api.common.experimental.get_dag_runs import get_dag_runs
from airflow.api.common.experimental.get_lineage import get_lineage as get_lineage_api
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.exceptions import AirflowException
from airflow.utils import timezone
from airflow.utils.docs import get_docs_url
from airflow.utils.strings import to_boolean
from airflow.version import version

log = logging.getLogger(__name__)

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        return current_app.api_auth.requires_authentication(function)(*args, **kwargs)

    return cast(T, decorated)


api_experimental = Blueprint('api_experimental', __name__)


def add_deprecation_headers(response: Response):
    """
    Add `Deprecation HTTP Header Field
    <https://tools.ietf.org/id/draft-dalal-deprecation-header-03.html>`__.
    """
    response.headers['Deprecation'] = 'true'
    doc_url = get_docs_url("upgrading-to-2.html#migration-guide-from-experimental-api-to-stable-api-v1")
    deprecation_link = f'<{doc_url}>; rel="deprecation"; type="text/html"'
    if 'link' in response.headers:
        response.headers['Link'] += f', {deprecation_link}'
    else:
        response.headers['Link'] = f'{deprecation_link}'

    return response


api_experimental.after_request(add_deprecation_headers)


@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag with an execution date of now unless
    specified in the data.
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    execution_date = None
    if 'execution_date' in data and data['execution_date'] is not None:
        execution_date = data['execution_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date)
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(execution_date)
            )
            log.error(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    replace_microseconds = execution_date is None
    if 'replace_microseconds' in data:
        replace_microseconds = to_boolean(data['replace_microseconds'])

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date, replace_microseconds)
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response

    if getattr(g, 'user', None):
        log.info("User %s created %s", g.user, dr)

    response = jsonify(
        message=f"Created {dr}", execution_date=dr.execution_date.isoformat(), run_id=dr.run_id
    )
    return response


@api_experimental.route('/dags/<string:dag_id>', methods=['DELETE'])
@requires_authentication
def delete_dag(dag_id):
    """Delete all DB records related to the specified Dag."""
    try:
        count = delete.delete_dag(dag_id)
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response
    return jsonify(message=f"Removed {count} record(s)", count=count)


@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['GET'])
@requires_authentication
def dag_runs(dag_id):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :query param state: a query string parameter '?state=queued|running|success...'

    :param dag_id: String identifier of a DAG
    :return: List of DAG runs of a DAG with requested state,
        or all runs if the state is not specified
    """
    try:
        state = request.args.get('state')
        dagruns = get_dag_runs(dag_id, state)
    except AirflowException as err:
        log.info(err)
        response = jsonify(error=f"{err}")
        response.status_code = 400
        return response

    return jsonify(dagruns)


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    """Test endpoint to check authentication"""
    return jsonify(status='OK')


@api_experimental.route('/info', methods=['GET'])
@requires_authentication
def info():
    """Get Airflow Version"""
    return jsonify(version=version)


@api_experimental.route('/dags/<string:dag_id>/code', methods=['GET'])
@requires_authentication
def get_dag_code(dag_id):
    """Return python code of a given dag_id."""
    try:
        return get_code(dag_id)
    except AirflowException as err:
        log.info(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables"""
    try:
        t_info = get_task(dag_id, task_id)
    except AirflowException as err:
        log.info(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v) for k, v in vars(t_info).items() if not k.startswith('_')}
    return jsonify(fields)


# ToDo: Shouldn't this be a PUT method?
@api_experimental.route('/dags/<string:dag_id>/paused/<string:paused>', methods=['GET'])
@requires_authentication
def dag_paused(dag_id, paused):
    """(Un)pauses a dag"""
    is_paused = bool(paused == 'true')

    models.DagModel.get_dagmodel(dag_id).set_is_paused(
        is_paused=is_paused,
    )

    return jsonify({'response': 'ok'})


@api_experimental.route('/dags/<string:dag_id>/paused', methods=['GET'])
@requires_authentication
def dag_is_paused(dag_id):
    """Get paused state of a dag"""
    is_paused = models.DagModel.get_dagmodel(dag_id).is_paused

    return jsonify({'is_paused': is_paused})


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>', methods=['GET']
)
@requires_authentication
def task_instance_info(dag_id, execution_date, task_id):
    """
    Returns a JSON with a task instance's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """
    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(execution_date)
        )
        log.error(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        ti_info = get_task_instance(dag_id, task_id, execution_date)
    except AirflowException as err:
        log.info(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v) for k, v in vars(ti_info).items() if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route('/dags/<string:dag_id>/dag_runs/<string:execution_date>', methods=['GET'])
@requires_authentication
def dag_run_status(dag_id, execution_date):
    """
    Returns a JSON with a dag_run's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """
    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(execution_date)
        )
        log.error(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        dr_info = get_dag_run_state(dag_id, execution_date)
    except AirflowException as err:
        log.info(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response

    return jsonify(dr_info)


@api_experimental.route('/latest_runs', methods=['GET'])
@requires_authentication
def latest_dag_runs():
    """Returns the latest DagRun for each DAG formatted for the UI"""
    from airflow.models import DagRun

    dagruns = DagRun.get_latest_runs()
    payload = []
    for dagrun in dagruns:
        if dagrun.execution_date:
            payload.append(
                {
                    'dag_id': dagrun.dag_id,
                    'execution_date': dagrun.execution_date.isoformat(),
                    'start_date': ((dagrun.start_date or '') and dagrun.start_date.isoformat()),
                    'dag_run_url': url_for(
                        'Airflow.graph', dag_id=dagrun.dag_id, execution_date=dagrun.execution_date
                    ),
                }
            )
    return jsonify(items=payload)  # old flask versions don't support jsonifying arrays


@api_experimental.route('/pools/<string:name>', methods=['GET'])
@requires_authentication
def get_pool(name):
    """Get pool by a given name."""
    try:
        pool = pool_api.get_pool(name=name)
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@api_experimental.route('/pools', methods=['GET'])
@requires_authentication
def get_pools():
    """Get all pools."""
    try:
        pools = pool_api.get_pools()
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response
    else:
        return jsonify([p.to_json() for p in pools])


@api_experimental.route('/pools', methods=['POST'])
@requires_authentication
def create_pool():
    """Create a pool."""
    params = request.get_json(force=True)
    try:
        pool = pool_api.create_pool(**params)
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@api_experimental.route('/pools/<string:name>', methods=['DELETE'])
@requires_authentication
def delete_pool(name):
    """Delete pool."""
    try:
        pool = pool_api.delete_pool(name=name)
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@api_experimental.route('/lineage/<string:dag_id>/<string:execution_date>', methods=['GET'])
@requires_authentication
def get_lineage(dag_id: str, execution_date: str):
    """Get Lineage details for a DagRun"""
    # Convert string datetime into actual datetime
    try:
        execution_dt = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(execution_date)
        )
        log.error(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        lineage = get_lineage_api(dag_id=dag_id, execution_date=execution_dt)
    except AirflowException as err:
        log.error(err)
        response = jsonify(error=f"{err}")
        response.status_code = err.status_code
        return response
    else:
        return jsonify(lineage)
