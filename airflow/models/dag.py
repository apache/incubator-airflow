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

import copy
import functools
import logging
import os
import pickle
import re
import sys
import traceback
import warnings
from collections import OrderedDict
from datetime import datetime, timedelta
from inspect import signature
from typing import (
    TYPE_CHECKING,
    Callable,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import jinja2
import pendulum
from croniter import croniter
from dateutil.relativedelta import relativedelta
from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, Text, func, or_
from sqlalchemy.orm import backref, joinedload, relationship
from sqlalchemy.orm.session import Session

from airflow import settings, utils
from airflow.configuration import conf
from airflow.exceptions import AirflowException, DuplicateTaskIdFound, TaskNotFound
from airflow.models.base import ID_LEN, Base
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagbag import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.dagparam import DagParam
from airflow.models.dagpickle import DagPickle
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import Context, TaskInstance, clear_task_instances
from airflow.security import permissions
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.helpers import validate_key
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import Interval, UtcDateTime, skip_locked, with_row_locks
from airflow.utils.state import State
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from airflow.utils.task_group import TaskGroup


# Before Py 3.7, there is no re.Pattern class
try:
    from re import Pattern as PatternType  # type: ignore
except ImportError:
    PatternType = type(re.compile('', 0))


log = logging.getLogger(__name__)

ScheduleInterval = Union[str, timedelta, relativedelta]
DEFAULT_VIEW_PRESETS = ['tree', 'graph', 'duration', 'gantt', 'landing_times']
ORIENTATION_PRESETS = ['LR', 'TB', 'RL', 'BT']

DagStateChangeCallback = Callable[[Context], None]


def get_last_dagrun(dag_id, session, include_externally_triggered=False):
    """
    Returns the last dag run for a dag, None if there was none.
    Last dag run can be any type of run eg. scheduled or backfilled.
    Overridden DagRuns are ignored.
    """
    DR = DagRun
    query = session.query(DR).filter(DR.dag_id == dag_id)
    if not include_externally_triggered:
        query = query.filter(DR.external_trigger == False)  # noqa pylint: disable=singleton-comparison
    query = query.order_by(DR.execution_date.desc())
    return query.first()


@functools.total_ordering
class DAG(LoggingMixin):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. A dag also has a schedule, a start date and an end date
    (optional). For each schedule, (say daily or hourly), the DAG needs to run
    each individual tasks as their dependencies are met. Certain tasks have
    the property of depending on their own past, meaning that they can't run
    until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :type dag_id: str
    :param description: The description for the DAG to e.g. be shown on the webserver
    :type description: str
    :param schedule_interval: Defines how often that DAG runs, this
        timedelta object gets added to your latest task instance's
        execution_date to figure out the next schedule
    :type schedule_interval: datetime.timedelta or
        dateutil.relativedelta.relativedelta or str that acts as a cron
        expression
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill
    :type start_date: datetime.datetime
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open ended scheduling
    :type end_date: datetime.datetime
    :param template_searchpath: This list of folders (non relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :type template_searchpath: str or list[str]
    :param template_undefined: Template undefined type.
    :type template_undefined: jinja2.StrictUndefined
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :type user_defined_macros: dict
    :param user_defined_filters: a dictionary of filters that will be exposed
        in your jinja templates. For example, passing
        ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
        you to ``{{ 'world' | hello }}`` in all jinja templates related to
        this DAG.
    :type user_defined_filters: dict
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :type default_args: dict
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :type params: dict
    :param concurrency: the number of task instances allowed to run
        concurrently
    :type concurrency: int
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :type max_active_runs: int
    :param dagrun_timeout: specify how long a DagRun should be up before
        timing out / failing, so that new DagRuns can be created. The timeout
        is only enforced for scheduled DagRuns.
    :type dagrun_timeout: datetime.timedelta
    :param sla_miss_callback: specify a function to call when reporting SLA
        timeouts.
    :type sla_miss_callback: types.FunctionType
    :param default_view: Specify DAG default view (tree, graph, duration,
                                                   gantt, landing_times), default tree
    :type default_view: str
    :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT), default LR
    :type orientation: str
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :type catchup: bool
    :param on_failure_callback: A function to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :type on_failure_callback: callable
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :type on_success_callback: callable
    :param access_control: Specify optional DAG-level permissions, e.g.,
        "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit'}}"
    :type access_control: dict
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored. If this optional parameter
        is not specified, the global config setting will be used.
    :type is_paused_upon_creation: bool or None
    :param jinja_environment_kwargs: additional configuration options to be passed to Jinja
        ``Environment`` for template rendering

        **Example**: to avoid Jinja from removing a trailing newline from template strings ::

            DAG(dag_id='my-dag',
                jinja_environment_kwargs={
                    'keep_trailing_newline': True,
                    # some other jinja2 Environment options here
                }
            )

        **See**: `Jinja Environment documentation
        <https://jinja.palletsprojects.com/en/master/api/#jinja2.Environment>`_

    :type jinja_environment_kwargs: dict
    :param tags: List of tags to help filtering DAGS in the UI.
    :type tags: List[str]
    """

    _comps = {
        'dag_id',
        'task_ids',
        'parent_dag',
        'start_date',
        'schedule_interval',
        'full_filepath',
        'template_searchpath',
        'last_loaded',
    }

    __serialized_fields: Optional[FrozenSet[str]] = None

    def __init__(
        self,
        dag_id: str,
        description: Optional[str] = None,
        schedule_interval: Optional[ScheduleInterval] = timedelta(days=1),
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        full_filepath: Optional[str] = None,
        template_searchpath: Optional[Union[str, Iterable[str]]] = None,
        template_undefined: Type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
        user_defined_macros: Optional[Dict] = None,
        user_defined_filters: Optional[Dict] = None,
        default_args: Optional[Dict] = None,
        concurrency: int = conf.getint('core', 'dag_concurrency'),
        max_active_runs: int = conf.getint('core', 'max_active_runs_per_dag'),
        dagrun_timeout: Optional[timedelta] = None,
        sla_miss_callback: Optional[Callable] = None,
        default_view: str = conf.get('webserver', 'dag_default_view').lower(),
        orientation: str = conf.get('webserver', 'dag_orientation'),
        catchup: bool = conf.getboolean('scheduler', 'catchup_by_default'),
        on_success_callback: Optional[DagStateChangeCallback] = None,
        on_failure_callback: Optional[DagStateChangeCallback] = None,
        doc_md: Optional[str] = None,
        params: Optional[Dict] = None,
        access_control: Optional[Dict] = None,
        is_paused_upon_creation: Optional[bool] = None,
        jinja_environment_kwargs: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
    ):
        from airflow.utils.task_group import TaskGroup

        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        self.default_args = copy.deepcopy(default_args or {})
        self.params = params or {}

        # merging potentially conflicting default_args['params'] into params
        if 'params' in self.default_args:
            self.params.update(self.default_args['params'])
            del self.default_args['params']

        validate_key(dag_id)

        self._dag_id = dag_id
        self._full_filepath = full_filepath if full_filepath else ''
        self._concurrency = concurrency
        self._pickle_id: Optional[int] = None

        self._description = description
        # set file location to caller source path
        back = sys._getframe().f_back
        self.fileloc = back.f_code.co_filename if back else ""
        self.task_dict: Dict[str, BaseOperator] = {}

        # set timezone from start_date
        if start_date and start_date.tzinfo:
            self.timezone = start_date.tzinfo
        elif 'start_date' in self.default_args and self.default_args['start_date']:
            if isinstance(self.default_args['start_date'], str):
                self.default_args['start_date'] = timezone.parse(self.default_args['start_date'])
            self.timezone = self.default_args['start_date'].tzinfo

        if not hasattr(self, 'timezone') or not self.timezone:
            self.timezone = settings.TIMEZONE

        # Apply the timezone we settled on to end_date if it wasn't supplied
        if 'end_date' in self.default_args and self.default_args['end_date']:
            if isinstance(self.default_args['end_date'], str):
                self.default_args['end_date'] = timezone.parse(
                    self.default_args['end_date'], timezone=self.timezone
                )

        self.start_date = timezone.convert_to_utc(start_date)
        self.end_date = timezone.convert_to_utc(end_date)

        # also convert tasks
        if 'start_date' in self.default_args:
            self.default_args['start_date'] = timezone.convert_to_utc(self.default_args['start_date'])
        if 'end_date' in self.default_args:
            self.default_args['end_date'] = timezone.convert_to_utc(self.default_args['end_date'])

        self.schedule_interval = schedule_interval
        if isinstance(template_searchpath, str):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.template_undefined = template_undefined
        self.parent_dag: Optional[DAG] = None  # Gets set when DAGs are loaded
        self.last_loaded = timezone.utcnow()
        self.safe_dag_id = dag_id.replace('.', '__dot__')
        self.max_active_runs = max_active_runs
        self.dagrun_timeout = dagrun_timeout
        self.sla_miss_callback = sla_miss_callback
        if default_view in DEFAULT_VIEW_PRESETS:
            self._default_view: str = default_view
        else:
            raise AirflowException(
                f'Invalid values of dag.default_view: only support '
                f'{DEFAULT_VIEW_PRESETS}, but get {default_view}'
            )
        if orientation in ORIENTATION_PRESETS:
            self.orientation = orientation
        else:
            raise AirflowException(
                f'Invalid values of dag.orientation: only support '
                f'{ORIENTATION_PRESETS}, but get {orientation}'
            )
        self.catchup = catchup
        self.is_subdag = False  # DagBag.bag_dag() will set this to True if appropriate

        self.partial = False
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback

        # To keep it in parity with Serialized DAGs
        # and identify if DAG has on_*_callback without actually storing them in Serialized JSON
        self.has_on_success_callback = self.on_success_callback is not None
        self.has_on_failure_callback = self.on_failure_callback is not None

        self.doc_md = doc_md

        self._access_control = DAG._upgrade_outdated_dag_access_control(access_control)
        self.is_paused_upon_creation = is_paused_upon_creation

        self.jinja_environment_kwargs = jinja_environment_kwargs
        self.tags = tags
        self._task_group = TaskGroup.create_root(self)

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    def __eq__(self, other):
        if type(self) == type(other):
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            return all(getattr(self, c, None) == getattr(other, c, None) for c in self._comps)
        return False

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            # task_ids returns a list and lists can't be hashed
            if c == 'task_ids':
                val = tuple(self.task_dict.keys())
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    # Context Manager -----------------------------------------------
    def __enter__(self):
        DagContext.push_context_managed_dag(self)
        return self

    def __exit__(self, _type, _value, _tb):
        DagContext.pop_context_managed_dag()

    # /Context Manager ----------------------------------------------

    @staticmethod
    def _upgrade_outdated_dag_access_control(access_control=None):
        """
        Looks for outdated dag level permissions (can_dag_read and can_dag_edit) in DAG
        access_controls (for example, {'role1': {'can_dag_read'}, 'role2': {'can_dag_read', 'can_dag_edit'}})
        and replaces them with updated permissions (can_read and can_edit).
        """
        if not access_control:
            return None
        new_perm_mapping = {
            permissions.DEPRECATED_ACTION_CAN_DAG_READ: permissions.ACTION_CAN_READ,
            permissions.DEPRECATED_ACTION_CAN_DAG_EDIT: permissions.ACTION_CAN_EDIT,
        }
        updated_access_control = {}
        for role, perms in access_control.items():
            updated_access_control[role] = {new_perm_mapping.get(perm, perm) for perm in perms}

        if access_control != updated_access_control:
            warnings.warn(
                "The 'can_dag_read' and 'can_dag_edit' permissions are deprecated. "
                "Please use 'can_read' and 'can_edit', respectively.",
                DeprecationWarning,
                stacklevel=3,
            )

        return updated_access_control

    def date_range(
        self,
        start_date: datetime,
        num: Optional[int] = None,
        end_date: Optional[datetime] = timezone.utcnow(),
    ) -> List[datetime]:
        if num is not None:
            end_date = None
        return utils_date_range(
            start_date=start_date, end_date=end_date, num=num, delta=self.normalized_schedule_interval
        )

    def is_fixed_time_schedule(self):
        """
        Figures out if the DAG schedule has a fixed time (e.g. 3 AM).

        :return: True if the schedule has a fixed time, False if not.
        """
        now = datetime.now()
        cron = croniter(self.normalized_schedule_interval, now)

        start = cron.get_next(datetime)
        cron_next = cron.get_next(datetime)

        if cron_next.minute == start.minute and cron_next.hour == start.hour:
            return True

        return False

    def following_schedule(self, dttm):
        """
        Calculates the following schedule for this dag in UTC.

        :param dttm: utc datetime
        :return: utc datetime
        """
        if isinstance(self.normalized_schedule_interval, str):
            # we don't want to rely on the transitions created by
            # croniter as they are not always correct
            dttm = pendulum.instance(dttm)
            naive = timezone.make_naive(dttm, self.timezone)
            cron = croniter(self.normalized_schedule_interval, naive)

            # We assume that DST transitions happen on the minute/hour
            if not self.is_fixed_time_schedule():
                # relative offset (eg. every 5 minutes)
                delta = cron.get_next(datetime) - naive
                following = dttm.in_timezone(self.timezone) + delta
            else:
                # absolute (e.g. 3 AM)
                naive = cron.get_next(datetime)
                tz = pendulum.timezone(self.timezone.name)
                following = timezone.make_aware(naive, tz)
            return timezone.convert_to_utc(following)
        elif self.normalized_schedule_interval is not None:
            return timezone.convert_to_utc(dttm + self.normalized_schedule_interval)

    def previous_schedule(self, dttm):
        """
        Calculates the previous schedule for this dag in UTC

        :param dttm: utc datetime
        :return: utc datetime
        """
        if isinstance(self.normalized_schedule_interval, str):
            # we don't want to rely on the transitions created by
            # croniter as they are not always correct
            dttm = pendulum.instance(dttm)
            naive = timezone.make_naive(dttm, self.timezone)
            cron = croniter(self.normalized_schedule_interval, naive)

            # We assume that DST transitions happen on the minute/hour
            if not self.is_fixed_time_schedule():
                # relative offset (eg. every 5 minutes)
                delta = naive - cron.get_prev(datetime)
                previous = dttm.in_timezone(self.timezone) - delta
            else:
                # absolute (e.g. 3 AM)
                naive = cron.get_prev(datetime)
                tz = pendulum.timezone(self.timezone.name)
                previous = timezone.make_aware(naive, tz)
            return timezone.convert_to_utc(previous)
        elif self.normalized_schedule_interval is not None:
            return timezone.convert_to_utc(dttm - self.normalized_schedule_interval)

    def next_dagrun_info(
        self,
        date_last_automated_dagrun: Optional[pendulum.DateTime],
    ) -> Tuple[Optional[pendulum.DateTime], Optional[pendulum.DateTime]]:
        """
        Get information about the next DagRun of this dag after ``date_last_automated_dagrun`` -- the
        execution date, and the earliest it could be scheduled

        :param date_last_automated_dagrun: The max(execution_date) of existing
            "automated" DagRuns for this dag (scheduled or backfill, but not
            manual)
        """
        if (
            self.schedule_interval == "@once" and date_last_automated_dagrun
        ) or self.schedule_interval is None:
            # Manual trigger, or already created the run for @once, can short circuit
            return (None, None)
        next_execution_date = self.next_dagrun_after_date(date_last_automated_dagrun)

        if next_execution_date is None:
            return (None, None)

        if self.schedule_interval == "@once":
            # For "@once" it can be created "now"
            return (next_execution_date, next_execution_date)

        return (next_execution_date, self.following_schedule(next_execution_date))

    def next_dagrun_after_date(self, date_last_automated_dagrun: Optional[pendulum.DateTime]):
        """
        Get the next execution date after the given ``date_last_automated_dagrun``, according to
        schedule_interval, start_date, end_date etc.  This doesn't check max active run or any other
        "concurrency" type limits, it only performs calculations based on the various date and interval fields
        of this dag and it's tasks.

        :param date_last_automated_dagrun: The execution_date of the last scheduler or
            backfill triggered run for this dag
        :type date_last_automated_dagrun: pendulum.Pendulum
        """
        if not self.schedule_interval or self.is_subdag:
            return None

        # don't schedule @once again
        if self.schedule_interval == '@once' and date_last_automated_dagrun:
            return None

        # don't do scheduler catchup for dag's that don't have dag.catchup = True
        if not (self.catchup or self.schedule_interval == '@once'):
            # The logic is that we move start_date up until
            # one period before, so that timezone.utcnow() is AFTER
            # the period end, and the job can be created...
            now = timezone.utcnow()
            next_start = self.following_schedule(now)
            last_start = self.previous_schedule(now)
            if next_start <= now or isinstance(self.schedule_interval, timedelta):
                new_start = last_start
            else:
                new_start = self.previous_schedule(last_start)

            if self.start_date:
                if new_start >= self.start_date:
                    self.start_date = new_start
            else:
                self.start_date = new_start

        next_run_date = None
        if not date_last_automated_dagrun:
            # First run
            task_start_dates = [t.start_date for t in self.tasks if t.start_date]
            if task_start_dates:
                next_run_date = self.normalize_schedule(min(task_start_dates))
                self.log.debug("Next run date based on tasks %s", next_run_date)
        else:
            next_run_date = self.following_schedule(date_last_automated_dagrun)

        if date_last_automated_dagrun and next_run_date:
            while next_run_date <= date_last_automated_dagrun:
                next_run_date = self.following_schedule(next_run_date)

        # don't ever schedule prior to the dag's start_date
        if self.start_date:
            next_run_date = self.start_date if not next_run_date else max(next_run_date, self.start_date)
            if next_run_date == self.start_date:
                next_run_date = self.normalize_schedule(self.start_date)

            self.log.debug("Dag start date: %s. Next run date: %s", self.start_date, next_run_date)

        # Don't schedule a dag beyond its end_date (as specified by the dag param)
        if next_run_date and self.end_date and next_run_date > self.end_date:
            return None

        # Don't schedule a dag beyond its end_date (as specified by the task params)
        # Get the min task end date, which may come from the dag.default_args
        task_end_dates = [t.end_date for t in self.tasks if t.end_date]
        if task_end_dates and next_run_date:
            min_task_end_date = min(task_end_dates)
            if next_run_date > min_task_end_date:
                return None

        return next_run_date

    def get_run_dates(self, start_date, end_date=None):
        """
        Returns a list of dates between the interval received as parameter using this
        dag's schedule interval. Returned dates can be used for execution dates.

        :param start_date: the start date of the interval
        :type start_date: datetime
        :param end_date: the end date of the interval, defaults to timezone.utcnow()
        :type end_date: datetime
        :return: a list of dates within the interval following the dag's schedule
        :rtype: list
        """
        run_dates = []

        using_start_date = start_date
        using_end_date = end_date

        # dates for dag runs
        using_start_date = using_start_date or min([t.start_date for t in self.tasks])
        using_end_date = using_end_date or timezone.utcnow()

        # next run date for a subdag isn't relevant (schedule_interval for subdags
        # is ignored) so we use the dag run's start date in the case of a subdag
        next_run_date = self.normalize_schedule(using_start_date) if not self.is_subdag else using_start_date

        while next_run_date and next_run_date <= using_end_date:
            run_dates.append(next_run_date)
            next_run_date = self.following_schedule(next_run_date)

        return run_dates

    def normalize_schedule(self, dttm):
        """Returns dttm + interval unless dttm is first interval then it returns dttm"""
        following = self.following_schedule(dttm)

        # in case of @once
        if not following:
            return dttm
        if self.previous_schedule(following) != dttm:
            return following

        return dttm

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_externally_triggered=include_externally_triggered
        )

    @provide_session
    def has_dag_runs(self, session=None, include_externally_triggered=True) -> bool:
        return (
            get_last_dagrun(
                self.dag_id, session=session, include_externally_triggered=include_externally_triggered
            )
            is not None
        )

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @dag_id.setter
    def dag_id(self, value: str) -> None:
        self._dag_id = value

    @property
    def full_filepath(self) -> str:
        return self._full_filepath

    @full_filepath.setter
    def full_filepath(self, value) -> None:
        self._full_filepath = value

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @concurrency.setter
    def concurrency(self, value: int):
        self._concurrency = value

    @property
    def access_control(self):
        return self._access_control

    @access_control.setter
    def access_control(self, value):
        self._access_control = DAG._upgrade_outdated_dag_access_control(value)

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def default_view(self) -> str:
        return self._default_view

    @property
    def pickle_id(self) -> Optional[int]:
        return self._pickle_id

    @pickle_id.setter
    def pickle_id(self, value: int) -> None:
        self._pickle_id = value

    def param(self, name: str, default=None) -> DagParam:
        """
        Return a DagParam object for current dag.

        :param name: dag parameter name.
        :param default: fallback value for dag parameter.
        :return: DagParam instance for specified name and current dag.
        """
        return DagParam(current_dag=self, name=name, default=default)

    @property
    def tasks(self) -> List[BaseOperator]:
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError('DAG.tasks can not be modified. Use dag.add_task() instead.')

    @property
    def task_ids(self) -> List[str]:
        return list(self.task_dict.keys())

    @property
    def task_group(self) -> "TaskGroup":
        return self._task_group

    @property
    def filepath(self) -> str:
        """File location of where the dag object is instantiated"""
        fn = self.full_filepath.replace(settings.DAGS_FOLDER + '/', '')
        fn = fn.replace(os.path.dirname(__file__) + '/', '')
        return fn

    @property
    def folder(self) -> str:
        """Folder location of where the DAG object is instantiated."""
        return os.path.dirname(self.full_filepath)

    @property
    def owner(self) -> str:
        """
        Return list of all owners found in DAG tasks.

        :return: Comma separated list of owners in DAG tasks
        :rtype: str
        """
        return ", ".join({t.owner for t in self.tasks})

    @property
    def allow_future_exec_dates(self) -> bool:
        return settings.ALLOW_FUTURE_EXEC_DATES and self.schedule_interval is None

    @provide_session
    def get_concurrency_reached(self, session=None) -> bool:
        """
        Returns a boolean indicating whether the concurrency limit for this DAG
        has been reached
        """
        TI = TaskInstance
        qry = session.query(func.count(TI.task_id)).filter(
            TI.dag_id == self.dag_id,
            TI.state == State.RUNNING,
        )
        return qry.scalar() >= self.concurrency

    @property
    def concurrency_reached(self):
        """This attribute is deprecated. Please use `airflow.models.DAG.get_concurrency_reached` method."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_concurrency_reached` method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_concurrency_reached()

    @provide_session
    def get_is_paused(self, session=None) -> Optional[None]:
        """Returns a boolean indicating whether this DAG is paused"""
        qry = session.query(DagModel).filter(DagModel.dag_id == self.dag_id)
        return qry.value(DagModel.is_paused)

    @property
    def is_paused(self):
        """This attribute is deprecated. Please use `airflow.models.DAG.get_is_paused` method."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_is_paused` method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_is_paused()

    @property
    def normalized_schedule_interval(self) -> Optional[ScheduleInterval]:
        """
        Returns Normalized Schedule Interval. This is used internally by the Scheduler to
        schedule DAGs.

        1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
        2. If Schedule Interval is "@once" return "None"
        3. If not (1) or (2) returns schedule_interval
        """
        if isinstance(self.schedule_interval, str) and self.schedule_interval in cron_presets:
            _schedule_interval = cron_presets.get(self.schedule_interval)  # type: Optional[ScheduleInterval]
        elif self.schedule_interval == '@once':
            _schedule_interval = None
        else:
            _schedule_interval = self.schedule_interval
        return _schedule_interval

    @provide_session
    def handle_callback(self, dagrun, success=True, reason=None, session=None):
        """
        Triggers the appropriate callback depending on the value of success, namely the
        on_failure_callback or on_success_callback. This method gets the context of a
        single TaskInstance part of this DagRun and passes that to the callable along
        with a 'reason', primarily to differentiate DagRun failures.

        .. note: The logs end up in
            ``$AIRFLOW_HOME/logs/scheduler/latest/PROJECT/DAG_FILE.py.log``

        :param dagrun: DagRun object
        :param success: Flag to specify if failure or success callback should be called
        :param reason: Completion reason
        :param session: Database session
        """
        callback = self.on_success_callback if success else self.on_failure_callback
        if callback:
            self.log.info('Executing dag callback function: %s', callback)
            tis = dagrun.get_task_instances()
            ti = tis[-1]  # get first TaskInstance of DagRun
            ti.task = self.get_task(ti.task_id)
            context = ti.get_template_context(session=session)
            context.update({'reason': reason})
            try:
                callback(context)
            except Exception:
                self.log.exception("failed to invoke dag state update callback")
                Stats.incr("dag.callback_exceptions")

    def get_active_runs(self):
        """
        Returns a list of dag run execution dates currently running

        :return: List of execution dates
        """
        runs = DagRun.find(dag_id=self.dag_id, state=State.RUNNING)

        active_dates = []
        for run in runs:
            active_dates.append(run.execution_date)

        return active_dates

    @provide_session
    def get_num_active_runs(self, external_trigger=None, session=None):
        """
        Returns the number of active "running" dag runs

        :param external_trigger: True for externally triggered active dag runs
        :type external_trigger: bool
        :param session:
        :return: number greater than 0 for active dag runs
        """
        # .count() is inefficient
        query = (
            session.query(func.count())
            .filter(DagRun.dag_id == self.dag_id)
            .filter(DagRun.state == State.RUNNING)
        )

        if external_trigger is not None:
            query = query.filter(DagRun.external_trigger == external_trigger)

        return query.scalar()

    @provide_session
    def get_dagrun(self, execution_date, session=None):
        """
        Returns the dag run for a given execution date if it exists, otherwise
        none.

        :param execution_date: The execution date of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        dagrun = (
            session.query(DagRun)
            .filter(DagRun.dag_id == self.dag_id, DagRun.execution_date == execution_date)
            .first()
        )

        return dagrun

    @provide_session
    def get_dagruns_between(self, start_date, end_date, session=None):
        """
        Returns the list of dag runs between start_date (inclusive) and end_date (inclusive).

        :param start_date: The starting execution date of the DagRun to find.
        :param end_date: The ending execution date of the DagRun to find.
        :param session:
        :return: The list of DagRuns found.
        """
        dagruns = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date >= start_date,
                DagRun.execution_date <= end_date,
            )
            .all()
        )

        return dagruns

    @provide_session
    def get_latest_execution_date(self, session=None):
        """Returns the latest date for which at least one dag run exists"""
        return session.query(func.max(DagRun.execution_date)).filter(DagRun.dag_id == self.dag_id).scalar()

    @property
    def latest_execution_date(self):
        """This attribute is deprecated. Please use `airflow.models.DAG.get_latest_execution_date` method."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_latest_execution_date` method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_latest_execution_date()

    @property
    def subdags(self):
        """Returns a list of the subdag objects associated to this DAG"""
        # Check SubDag for class but don't check class directly
        from airflow.operators.subdag import SubDagOperator

        subdag_lst = []
        for task in self.tasks:
            if (
                isinstance(task, SubDagOperator)
                or
                # TODO remove in Airflow 2.0
                type(task).__name__ == 'SubDagOperator'
                or task.task_type == 'SubDagOperator'
            ):
                subdag_lst.append(task.subdag)
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self) -> jinja2.Environment:
        """Build a Jinja2 environment."""
        # Collect directories to search for template files
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        # Default values (for backward compatibility)
        jinja_env_options = {
            'loader': jinja2.FileSystemLoader(searchpath),
            'undefined': self.template_undefined,
            'extensions': ["jinja2.ext.do"],
            'cache_size': 0,
        }
        if self.jinja_environment_kwargs:
            jinja_env_options.update(self.jinja_environment_kwargs)

        env = jinja2.Environment(**jinja_env_options)  # type: ignore

        # Add any user defined items. Safe to edit globals as long as no templates are rendered yet.
        # http://jinja.pocoo.org/docs/2.10/api/#jinja2.Environment.globals
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)
        if self.user_defined_filters:
            env.filters.update(self.user_defined_filters)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """
        Simple utility method to set dependency between two tasks that
        already have been added to the DAG using add_task()
        """
        self.get_task(upstream_task_id).set_downstream(self.get_task(downstream_task_id))

    @provide_session
    def get_task_instances(self, start_date=None, end_date=None, state=None, session=None):
        if not start_date:
            start_date = (timezone.utcnow() - timedelta(30)).date()
            start_date = timezone.make_aware(datetime.combine(start_date, datetime.min.time()))

        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.execution_date >= start_date,
            TaskInstance.task_id.in_([t.task_id for t in self.tasks]),
        )
        # This allows allow_trigger_in_future config to take affect, rather than mandating exec_date <= UTC
        if end_date or not self.allow_future_exec_dates:
            end_date = end_date or timezone.utcnow()
            tis = tis.filter(TaskInstance.execution_date <= end_date)

        if state:
            if isinstance(state, str):
                tis = tis.filter(TaskInstance.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.filter(TaskInstance.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.filter(
                            or_(TaskInstance.state.in_(not_none_state), TaskInstance.state.is_(None))
                        )
                else:
                    tis = tis.filter(TaskInstance.state.in_(state))
        tis = tis.order_by(TaskInstance.execution_date).all()
        return tis

    @property
    def roots(self) -> List[BaseOperator]:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self) -> List[BaseOperator]:
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def topological_sort(self, include_subdag_tasks: bool = False):
        """
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :param include_subdag_tasks: whether to include tasks in subdags, default to False
        :return: list of tasks in topological order
        """
        from airflow.operators.subdag import SubDagOperator  # Avoid circular import

        # convert into an OrderedDict to speedup lookup while keeping order the same
        graph_unsorted = OrderedDict((task.task_id, task) for task in self.tasks)

        graph_sorted = []  # type: List[BaseOperator]

        # special case
        if len(self.tasks) == 0:
            return tuple(graph_sorted)

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted
            # graph. If a set of edges doesn't contain any nodes that
            # haven't been resolved, that is, that are still in the
            # unsorted graph, remove the pair from the unsorted graph,
            # and append it to the sorted graph. Note here that by using
            # using the items() method for iterating, a copy of the
            # unsorted graph is used, allowing us to modify the unsorted
            # graph as we move through it. We also keep a flag for
            # checking that graph is acyclic, which is true if any
            # nodes are resolved during each pass through the graph. If
            # not, we need to exit as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted.values()):
                for edge in node.upstream_list:
                    if edge.task_id in graph_unsorted:
                        break
                # no edges in upstream tasks
                else:
                    acyclic = True
                    del graph_unsorted[node.task_id]
                    graph_sorted.append(node)
                    if include_subdag_tasks and isinstance(node, SubDagOperator):
                        graph_sorted.extend(node.subdag.topological_sort(include_subdag_tasks=True))

            if not acyclic:
                raise AirflowException(f"A cyclic dependency occurred in dag: {self.dag_id}")

        return tuple(graph_sorted)

    @provide_session
    def set_dag_runs_state(
        self,
        state: str = State.RUNNING,
        session: Session = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        dag_ids: List[str] = None,
    ) -> None:
        dag_ids = dag_ids or [self.dag_id]
        query = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))
        if start_date:
            query = query.filter(DagRun.execution_date >= start_date)
        if end_date:
            query = query.filter(DagRun.execution_date <= end_date)
        query.update({DagRun.state: state}, synchronize_session='fetch')

    @provide_session
    def clear(
        self,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=True,
        include_parentdag=True,
        dag_run_state: str = State.RUNNING,
        dry_run=False,
        session=None,
        get_tis=False,
        recursion_depth=0,
        max_recursion_depth=None,
        dag_bag=None,
        visited_external_tis=None,
    ):
        """
        Clears a set of task instances associated with the current dag for
        a specified date range.

        :param start_date: The minimum execution_date to clear
        :type start_date: datetime.datetime or None
        :param end_date: The maximum execution_date to clear
        :type end_date: datetime.datetime or None
        :param only_failed: Only clear failed tasks
        :type only_failed: bool
        :param only_running: Only clear running tasks.
        :type only_running: bool
        :param confirm_prompt: Ask for confirmation
        :type confirm_prompt: bool
        :param include_subdags: Clear tasks in subdags and clear external tasks
            indicated by ExternalTaskMarker
        :type include_subdags: bool
        :param include_parentdag: Clear tasks in the parent dag of the subdag.
        :type include_parentdag: bool
        :param dag_run_state: state to set DagRun to
        :param dry_run: Find the tasks to clear but don't clear them.
        :type dry_run: bool
        :param session: The sqlalchemy session to use
        :type session: sqlalchemy.orm.session.Session
        :param get_tis: Return the sqlalchemy query for finding the TaskInstance without clearing the tasks
        :type get_tis: bool
        :param recursion_depth: The recursion depth of nested calls to DAG.clear().
        :type recursion_depth: int
        :param max_recursion_depth: The maximum recursion depth allowed. This is determined by the
            first encountered ExternalTaskMarker. Default is None indicating no ExternalTaskMarker
            has been encountered.
        :type max_recursion_depth: int
        :param dag_bag: The DagBag used to find the dags
        :type dag_bag: airflow.models.dagbag.DagBag
        :param visited_external_tis: A set used internally to keep track of the visited TaskInstance when
            clearing tasks across multiple DAGs linked by ExternalTaskMarker to avoid redundant work.
        :type visited_external_tis: set
        """
        TI = TaskInstance
        tis = session.query(TI)
        dag_ids = []
        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in self.subdags + [self]:
                conditions.append((TI.dag_id == dag.dag_id) & TI.task_id.in_(dag.task_ids))
                dag_ids.append(dag.dag_id)
            tis = tis.filter(or_(*conditions))
        else:
            tis = session.query(TI).filter(TI.dag_id == self.dag_id)
            tis = tis.filter(TI.task_id.in_(self.task_ids))

        if include_parentdag and self.is_subdag and self.parent_dag is not None:
            p_dag = self.parent_dag.sub_dag(
                task_ids_or_regex=r"^{}$".format(self.dag_id.split('.')[1]),
                include_upstream=False,
                include_downstream=True,
            )

            tis = tis.union(
                p_dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    confirm_prompt=confirm_prompt,
                    include_subdags=include_subdags,
                    include_parentdag=False,
                    dag_run_state=dag_run_state,
                    get_tis=True,
                    session=session,
                    recursion_depth=recursion_depth,
                    max_recursion_depth=max_recursion_depth,
                    dag_bag=dag_bag,
                    visited_external_tis=visited_external_tis,
                )
            )

        if start_date:
            tis = tis.filter(TI.execution_date >= start_date)
        if end_date:
            tis = tis.filter(TI.execution_date <= end_date)
        if only_failed:
            tis = tis.filter(or_(TI.state == State.FAILED, TI.state == State.UPSTREAM_FAILED))
        if only_running:
            tis = tis.filter(TI.state == State.RUNNING)

        if include_subdags:
            from airflow.sensors.external_task import ExternalTaskMarker

            # Recursively find external tasks indicated by ExternalTaskMarker
            instances = tis.all()
            for ti in instances:
                if ti.operator == ExternalTaskMarker.__name__:
                    if visited_external_tis is None:
                        visited_external_tis = set()
                    ti_key = ti.key.primary
                    if ti_key not in visited_external_tis:
                        # Only clear this ExternalTaskMarker if it's not already visited by the
                        # recursive calls to dag.clear().
                        task: ExternalTaskMarker = cast(
                            ExternalTaskMarker, copy.copy(self.get_task(ti.task_id))
                        )
                        ti.task = task

                        if recursion_depth == 0:
                            # Maximum recursion depth allowed is the recursion_depth of the first
                            # ExternalTaskMarker in the tasks to be cleared.
                            max_recursion_depth = task.recursion_depth

                        if recursion_depth + 1 > max_recursion_depth:
                            # Prevent cycles or accidents.
                            raise AirflowException(
                                "Maximum recursion depth {} reached for {} {}. "
                                "Attempted to clear too many tasks "
                                "or there may be a cyclic dependency.".format(
                                    max_recursion_depth, ExternalTaskMarker.__name__, ti.task_id
                                )
                            )
                        ti.render_templates()
                        external_tis = session.query(TI).filter(
                            TI.dag_id == task.external_dag_id,
                            TI.task_id == task.external_task_id,
                            TI.execution_date == pendulum.parse(task.execution_date),
                        )

                        for tii in external_tis:
                            if not dag_bag:
                                dag_bag = DagBag(read_dags_from_db=True)
                            external_dag = dag_bag.get_dag(tii.dag_id)
                            if not external_dag:
                                raise AirflowException(f"Could not find dag {tii.dag_id}")
                            downstream = external_dag.sub_dag(
                                task_ids_or_regex=fr"^{tii.task_id}$",
                                include_upstream=False,
                                include_downstream=True,
                            )
                            tis = tis.union(
                                downstream.clear(
                                    start_date=tii.execution_date,
                                    end_date=tii.execution_date,
                                    only_failed=only_failed,
                                    only_running=only_running,
                                    confirm_prompt=confirm_prompt,
                                    include_subdags=include_subdags,
                                    include_parentdag=False,
                                    dag_run_state=dag_run_state,
                                    get_tis=True,
                                    session=session,
                                    recursion_depth=recursion_depth + 1,
                                    max_recursion_depth=max_recursion_depth,
                                    dag_bag=dag_bag,
                                    visited_external_tis=visited_external_tis,
                                )
                            )
                        visited_external_tis.add(ti_key)

        if get_tis:
            return tis

        tis = tis.all()

        if dry_run:
            session.expunge_all()
            return tis

        # Do not use count() here, it's actually much slower than just retrieving all the rows when
        # tis has multiple UNION statements.
        count = len(tis)
        do_it = True
        if count == 0:
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in tis])
            question = (
                "You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? (yes/no): "
            ).format(count=count, ti_list=ti_list)
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            clear_task_instances(
                tis,
                session,
                dag=self,
                activate_dag_runs=False,  # We will set DagRun state later.
            )

            self.set_dag_runs_state(
                session=session,
                start_date=start_date,
                end_date=end_date,
                state=dag_run_state,
                dag_ids=dag_ids,
            )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")

        session.commit()
        return count

    @classmethod
    def clear_dags(
        cls,
        dags,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=True,
        include_parentdag=False,
        dag_run_state=State.RUNNING,
        dry_run=False,
    ):
        all_tis = []
        for dag in dags:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=False,
                include_subdags=include_subdags,
                include_parentdag=include_parentdag,
                dag_run_state=dag_run_state,
                dry_run=True,
            )
            all_tis.extend(tis)

        if dry_run:
            return all_tis

        count = len(all_tis)
        do_it = True
        if count == 0:
            print("Nothing to clear.")
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in all_tis])
            question = f"You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? (yes/no): "
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            for dag in dags:
                dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    confirm_prompt=False,
                    include_subdags=include_subdags,
                    dag_run_state=dag_run_state,
                    dry_run=False,
                )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")
        return count

    def __deepcopy__(self, memo):
        # Switcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k not in ('user_defined_macros', 'user_defined_filters', 'params', '_log'):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        result.params = self.params
        if hasattr(self, '_log'):
            result._log = self._log
        return result

    def sub_dag(self, *args, **kwargs):
        """This method is deprecated in favor of partial_subset"""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use partial_subset",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.partial_subset(*args, **kwargs)

    def partial_subset(
        self,
        task_ids_or_regex: Union[str, PatternType, Iterable[str]],
        include_downstream=False,
        include_upstream=True,
        include_direct_upstream=False,
    ):
        """
        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.

        :param task_ids_or_regex: Either a list of task_ids, or a regex to
            match against task ids (as a string, or compiled regex pattern).
        :type task_ids_or_regex: [str] or str or re.Pattern
        :param include_downstream: Include all downstream tasks of matched
            tasks, in addition to matched tasks.
        :param include_upstream: Include all upstream tasks of matched tasks,
            in addition to matched tasks.
        """
        # deep-copying self.task_dict and self._task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        task_dict = self.task_dict
        task_group = self._task_group
        self.task_dict = {}
        self._task_group = None  # type: ignore
        dag = copy.deepcopy(self)
        self.task_dict = task_dict
        self._task_group = task_group

        if isinstance(task_ids_or_regex, (str, PatternType)):
            matched_tasks = [t for t in self.tasks if re.findall(task_ids_or_regex, t.task_id)]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids_or_regex]

        also_include = []
        for t in matched_tasks:
            if include_downstream:
                also_include += t.get_flat_relatives(upstream=False)
            if include_upstream:
                also_include += t.get_flat_relatives(upstream=True)
            elif include_direct_upstream:
                also_include += t.upstream_list

        # Compiling the unique list of tasks that made the cut
        # Make sure to not recursively deepcopy the dag while copying the task
        dag.task_dict = {
            t.task_id: copy.deepcopy(t, {id(t.dag): dag})  # type: ignore
            for t in matched_tasks + also_include
        }

        def filter_task_group(group, parent_group):
            """Exclude tasks not included in the subdag from the given TaskGroup."""
            copied = copy.copy(group)
            copied.used_group_ids = set(copied.used_group_ids)
            copied._parent_group = parent_group

            copied.children = {}

            for child in group.children.values():
                if isinstance(child, BaseOperator):
                    if child.task_id in dag.task_dict:
                        copied.children[child.task_id] = dag.task_dict[child.task_id]
                else:
                    filtered_child = filter_task_group(child, copied)

                    # Only include this child TaskGroup if it is non-empty.
                    if filtered_child.children:
                        copied.children[child.group_id] = filtered_child

            return copied

        dag._task_group = filter_task_group(self._task_group, None)

        # Removing upstream/downstream references to tasks and TaskGroups that did not make
        # the cut.
        subdag_task_groups = dag.task_group.get_task_group_dict()
        for group in subdag_task_groups.values():
            group.upstream_group_ids = group.upstream_group_ids.intersection(subdag_task_groups.keys())
            group.downstream_group_ids = group.downstream_group_ids.intersection(subdag_task_groups.keys())
            group.upstream_task_ids = group.upstream_task_ids.intersection(dag.task_dict.keys())
            group.downstream_task_ids = group.downstream_task_ids.intersection(dag.task_dict.keys())

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t._upstream_task_ids = t.upstream_task_ids.intersection(dag.task_dict.keys())
            t._downstream_task_ids = t.downstream_task_ids.intersection(dag.task_dict.keys())

        if len(dag.tasks) < len(self.tasks):
            dag.partial = True

        return dag

    def has_task(self, task_id: str):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id: str, include_subdags: bool = False) -> BaseOperator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        if include_subdags:
            for dag in self.subdags:
                if task_id in dag.task_dict:
                    return dag.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    def pickle_info(self):
        d = {}
        d['is_picklable'] = True
        try:
            dttm = timezone.utcnow()
            pickled = pickle.dumps(self)
            d['pickle_len'] = len(pickled)
            d['pickling_duration'] = str(timezone.utcnow() - dttm)
        except Exception as e:
            self.log.debug(e)
            d['is_picklable'] = False
            d['stacktrace'] = traceback.format_exc()
        return d

    @provide_session
    def pickle(self, session=None) -> DagPickle:
        dag = session.query(DagModel).filter(DagModel.dag_id == self.dag_id).first()
        dp = None
        if dag and dag.pickle_id:
            dp = session.query(DagPickle).filter(DagPickle.id == dag.pickle_id).first()
        if not dp or dp.pickle != self:
            dp = DagPickle(dag=self)
            session.add(dp)
            self.last_pickled = timezone.utcnow()
            session.commit()
            self.pickle_id = dp.id

        return dp

    def tree_view(self) -> None:
        """Print an ASCII tree representation of the DAG."""

        def get_downstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.downstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    @property
    def task(self):
        from airflow.decorators import task

        return functools.partial(task, dag=self)

    def add_task(self, task):
        """
        Add a task to the DAG

        :param task: the task you want to add
        :type task: task
        """
        if not self.start_date and not task.start_date:
            raise AirflowException("Task is missing the start_date parameter")
        # if the task has no start date, assign it the same as the DAG
        elif not task.start_date:
            task.start_date = self.start_date
        # otherwise, the task will start on the later of its own start date and
        # the DAG's start date
        elif self.start_date:
            task.start_date = max(task.start_date, self.start_date)

        # if the task has no end date, assign it the same as the dag
        if not task.end_date:
            task.end_date = self.end_date
        # otherwise, the task will end on the earlier of its own end date and
        # the DAG's end date
        elif task.end_date and self.end_date:
            task.end_date = min(task.end_date, self.end_date)

        if (
            task.task_id in self.task_dict and self.task_dict[task.task_id] is not task
        ) or task.task_id in self._task_group.used_group_ids:
            raise DuplicateTaskIdFound(f"Task id '{task.task_id}' has already been added to the DAG")
        else:
            self.task_dict[task.task_id] = task
            task.dag = self
            # Add task_id to used_group_ids to prevent group_id and task_id collisions.
            self._task_group.used_group_ids.add(task.task_id)

        self.task_count = len(self.task_dict)

    def add_tasks(self, tasks):
        """
        Add a list of tasks to the DAG

        :param tasks: a lit of tasks you want to add
        :type tasks: list of tasks
        """
        for task in tasks:
            self.add_task(task)

    def run(
        self,
        start_date=None,
        end_date=None,
        mark_success=False,
        local=False,
        executor=None,
        donot_pickle=conf.getboolean('core', 'donot_pickle'),
        ignore_task_deps=False,
        ignore_first_depends_on_past=True,
        pool=None,
        delay_on_limit_secs=1.0,
        verbose=False,
        conf=None,
        rerun_failed_tasks=False,
        run_backwards=False,
    ):
        """
        Runs the DAG.

        :param start_date: the start date of the range to run
        :type start_date: datetime.datetime
        :param end_date: the end date of the range to run
        :type end_date: datetime.datetime
        :param mark_success: True to mark jobs as succeeded without running them
        :type mark_success: bool
        :param local: True to run the tasks using the LocalExecutor
        :type local: bool
        :param executor: The executor instance to run the tasks
        :type executor: airflow.executor.base_executor.BaseExecutor
        :param donot_pickle: True to avoid pickling DAG object and send to workers
        :type donot_pickle: bool
        :param ignore_task_deps: True to skip upstream tasks
        :type ignore_task_deps: bool
        :param ignore_first_depends_on_past: True to ignore depends_on_past
            dependencies for the first set of tasks only
        :type ignore_first_depends_on_past: bool
        :param pool: Resource pool to use
        :type pool: str
        :param delay_on_limit_secs: Time in seconds to wait before next attempt to run
            dag run when max_active_runs limit has been reached
        :type delay_on_limit_secs: float
        :param verbose: Make logging output more verbose
        :type verbose: bool
        :param conf: user defined dictionary passed from CLI
        :type conf: dict
        :param rerun_failed_tasks:
        :type: bool
        :param run_backwards:
        :type: bool

        """
        from airflow.jobs.backfill_job import BackfillJob

        if not executor and local:
            from airflow.executors.local_executor import LocalExecutor

            executor = LocalExecutor()
        elif not executor:
            from airflow.executors.executor_loader import ExecutorLoader

            executor = ExecutorLoader.get_default_executor()
        job = BackfillJob(
            self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            executor=executor,
            donot_pickle=donot_pickle,
            ignore_task_deps=ignore_task_deps,
            ignore_first_depends_on_past=ignore_first_depends_on_past,
            pool=pool,
            delay_on_limit_secs=delay_on_limit_secs,
            verbose=verbose,
            conf=conf,
            rerun_failed_tasks=rerun_failed_tasks,
            run_backwards=run_backwards,
        )
        job.run()

    def cli(self):
        """Exposes a CLI specific to this DAG"""
        from airflow.cli import cli_parser

        parser = cli_parser.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @provide_session
    def create_dagrun(
        self,
        state: State,
        execution_date: Optional[datetime] = None,
        run_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        external_trigger: Optional[bool] = False,
        conf: Optional[dict] = None,
        run_type: Optional[DagRunType] = None,
        session=None,
        dag_hash: Optional[str] = None,
        creating_job_id: Optional[int] = None,
    ):
        """
        Creates a dag run from this dag including the tasks associated with this dag.
        Returns the dag run.

        :param run_id: defines the run id for this dag run
        :type run_id: str
        :param run_type: type of DagRun
        :type run_type: airflow.utils.types.DagRunType
        :param execution_date: the execution date of this dag run
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.State
        :param start_date: the date this dag run should be evaluated
        :type start_date: datetime
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param conf: Dict containing configuration/parameters to pass to the DAG
        :type conf: dict
        :param creating_job_id: id of the job creating this DagRun
        :type creating_job_id: int
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dag_hash: Hash of Serialized DAG
        :type dag_hash: str
        """
        if run_id and not run_type:
            if not isinstance(run_id, str):
                raise ValueError(f"`run_id` expected to be a str is {type(run_id)}")
            run_type: DagRunType = DagRunType.from_run_id(run_id)
        elif run_type and execution_date:
            if not isinstance(run_type, DagRunType):
                raise ValueError(f"`run_type` expected to be a DagRunType is {type(run_type)}")
            run_id = DagRun.generate_run_id(run_type, execution_date)
        elif not run_id:
            raise AirflowException(
                "Creating DagRun needs either `run_id` or both `run_type` and `execution_date`"
            )

        run = DagRun(
            dag_id=self.dag_id,
            run_id=run_id,
            execution_date=execution_date,
            start_date=start_date,
            external_trigger=external_trigger,
            conf=conf,
            state=state,
            run_type=run_type,
            dag_hash=dag_hash,
            creating_job_id=creating_job_id,
        )
        session.add(run)
        session.flush()

        run.dag = self

        # create the associated task instances
        # state is None at the moment of creation
        run.verify_integrity(session=session)

        return run

    @classmethod
    @provide_session
    def bulk_sync_to_db(cls, dags: Collection["DAG"], session=None):
        """This method is deprecated in favor of bulk_write_to_db"""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use bulk_write_to_db",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.bulk_write_to_db(dags, session)

    @classmethod
    @provide_session
    def bulk_write_to_db(cls, dags: Collection["DAG"], session=None):
        """
        Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB, including
        calculated fields.

        Note that this method can be called for both DAGs and SubDAGs. A SubDag is actually a SubDagOperator.

        :param dags: the DAG objects to save to the DB
        :type dags: List[airflow.models.dag.DAG]
        :return: None
        """
        if not dags:
            return

        log.info("Sync %s DAGs", len(dags))
        dag_by_ids = {dag.dag_id: dag for dag in dags}
        dag_ids = set(dag_by_ids.keys())
        query = (
            session.query(DagModel)
            .options(joinedload(DagModel.tags, innerjoin=False))
            .filter(DagModel.dag_id.in_(dag_ids))
        )
        orm_dags = with_row_locks(query, of=DagModel, session=session).all()

        existing_dag_ids = {orm_dag.dag_id for orm_dag in orm_dags}
        missing_dag_ids = dag_ids.difference(existing_dag_ids)

        for missing_dag_id in missing_dag_ids:
            orm_dag = DagModel(dag_id=missing_dag_id)
            dag = dag_by_ids[missing_dag_id]
            if dag.is_paused_upon_creation is not None:
                orm_dag.is_paused = dag.is_paused_upon_creation
            orm_dag.tags = []
            log.info("Creating ORM DAG for %s", dag.dag_id)
            session.add(orm_dag)
            orm_dags.append(orm_dag)

        # Get the latest dag run for each existing dag as a single query (avoid n+1 query)
        most_recent_dag_runs = dict(
            session.query(DagRun.dag_id, func.max_(DagRun.execution_date))
            .filter(
                DagRun.dag_id.in_(existing_dag_ids),
                or_(
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                    DagRun.run_type == DagRunType.SCHEDULED,
                ),
            )
            .group_by(DagRun.dag_id)
            .all()
        )

        # Get number of active dagruns for all dags we are processing as a single query.
        num_active_runs = dict(
            session.query(DagRun.dag_id, func.count('*'))
            .filter(
                DagRun.dag_id.in_(existing_dag_ids),
                DagRun.state == State.RUNNING,  # pylint: disable=comparison-with-callable
                DagRun.external_trigger.is_(False),
            )
            .group_by(DagRun.dag_id)
            .all()
        )

        for orm_dag in sorted(orm_dags, key=lambda d: d.dag_id):
            dag = dag_by_ids[orm_dag.dag_id]
            if dag.is_subdag:
                orm_dag.is_subdag = True
                orm_dag.fileloc = dag.parent_dag.fileloc  # type: ignore
                orm_dag.root_dag_id = dag.parent_dag.dag_id  # type: ignore
                orm_dag.owners = dag.parent_dag.owner  # type: ignore
            else:
                orm_dag.is_subdag = False
                orm_dag.fileloc = dag.fileloc
                orm_dag.owners = dag.owner
            orm_dag.is_active = True
            orm_dag.last_parsed_time = timezone.utcnow()
            orm_dag.default_view = dag.default_view
            orm_dag.description = dag.description
            orm_dag.schedule_interval = dag.schedule_interval
            orm_dag.concurrency = dag.concurrency
            orm_dag.has_task_concurrency_limits = any(t.task_concurrency is not None for t in dag.tasks)

            orm_dag.calculate_dagrun_date_fields(
                dag,
                most_recent_dag_runs.get(dag.dag_id),
                num_active_runs.get(dag.dag_id, 0),
            )

            for orm_tag in list(orm_dag.tags):
                if orm_tag.name not in orm_dag.tags:
                    session.delete(orm_tag)
                orm_dag.tags.remove(orm_tag)
            if dag.tags:
                orm_tag_names = [t.name for t in orm_dag.tags]
                for dag_tag in list(dag.tags):
                    if dag_tag not in orm_tag_names:
                        dag_tag_orm = DagTag(name=dag_tag, dag_id=dag.dag_id)
                        orm_dag.tags.append(dag_tag_orm)
                        session.add(dag_tag_orm)

        if settings.STORE_DAG_CODE:
            DagCode.bulk_sync_to_db([dag.fileloc for dag in orm_dags])

        # Issue SQL/finish "Unit of Work", but let @provide_session commit (or if passed a session, let caller
        # decide when to commit
        session.flush()

        for dag in dags:
            cls.bulk_write_to_db(dag.subdags, session=session)

    @provide_session
    def sync_to_db(self, session=None):
        """
        Save attributes about this DAG to the DB. Note that this method
        can be called for both DAGs and SubDAGs. A SubDag is actually a
        SubDagOperator.

        :return: None
        """
        self.bulk_write_to_db([self], session)

    def get_default_view(self):
        """This is only there for backward compatible jinja2 templates"""
        if self.default_view is None:
            return conf.get('webserver', 'dag_default_view').lower()
        else:
            return self.default_view

    @staticmethod
    @provide_session
    def deactivate_unknown_dags(active_dag_ids, session=None):
        """
        Given a list of known DAGs, deactivate any other DAGs that are
        marked as active in the ORM

        :param active_dag_ids: list of DAG IDs that are active
        :type active_dag_ids: list[unicode]
        :return: None
        """
        if len(active_dag_ids) == 0:
            return
        for dag in session.query(DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()

    @staticmethod
    @provide_session
    def deactivate_stale_dags(expiration_date, session=None):
        """
        Deactivate any DAGs that were last touched by the scheduler before
        the expiration date. These DAGs were likely deleted.

        :param expiration_date: set inactive DAGs that were touched before this
            time
        :type expiration_date: datetime
        :return: None
        """
        for dag in (
            session.query(DagModel)
            .filter(DagModel.last_parsed_time < expiration_date, DagModel.is_active)
            .all()
        ):
            log.info(
                "Deactivating DAG ID %s since it was last touched by the scheduler at %s",
                dag.dag_id,
                dag.last_parsed_time.isoformat(),
            )
            dag.is_active = False
            session.merge(dag)
            session.commit()

    @staticmethod
    @provide_session
    def get_num_task_instances(dag_id, task_ids=None, states=None, session=None):
        """
        Returns the number of task instances in the given DAG.

        :param session: ORM session
        :param dag_id: ID of the DAG to get the task concurrency of
        :type dag_id: unicode
        :param task_ids: A list of valid task IDs for the given DAG
        :type task_ids: list[unicode]
        :param states: A list of states to filter by if supplied
        :type states: list[state]
        :return: The number of running tasks
        :rtype: int
        """
        qry = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == dag_id,
        )
        if task_ids:
            qry = qry.filter(
                TaskInstance.task_id.in_(task_ids),
            )

        if states:
            if None in states:
                if all(x is None for x in states):
                    qry = qry.filter(TaskInstance.state.is_(None))
                else:
                    not_none_states = [state for state in states if state]
                    qry = qry.filter(
                        or_(TaskInstance.state.in_(not_none_states), TaskInstance.state.is_(None))
                    )
            else:
                qry = qry.filter(TaskInstance.state.in_(states))
        return qry.scalar()

    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        if not cls.__serialized_fields:
            cls.__serialized_fields = frozenset(vars(DAG(dag_id='test')).keys()) - {
                'parent_dag',
                '_old_context_manager_dags',
                'safe_dag_id',
                'last_loaded',
                '_full_filepath',
                'user_defined_filters',
                'user_defined_macros',
                'partial',
                '_pickle_id',
                '_log',
                'is_subdag',
                'task_dict',
                'template_searchpath',
                'sla_miss_callback',
                'on_success_callback',
                'on_failure_callback',
                'template_undefined',
                'jinja_environment_kwargs',
                # has_on_*_callback are only stored if the value is True, as the default is False
                'has_on_success_callback',
                'has_on_failure_callback',
            }
        return cls.__serialized_fields


class DagTag(Base):
    """A tag name per dag, to allow quick filtering in the DAG view."""

    __tablename__ = "dag_tag"
    name = Column(String(100), primary_key=True)
    dag_id = Column(String(ID_LEN), ForeignKey('dag.dag_id'), primary_key=True)

    def __repr__(self):
        return self.name


class DagModel(Base):
    """Table containing DAG properties"""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    root_dag_id = Column(String(ID_LEN))
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = conf.getboolean('core', 'dags_are_paused_at_creation')
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether the DAG is a subdag
    is_subdag = Column(Boolean, default=False)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_parsed_time = Column(UtcDateTime)
    # Last time this DAG was pickled
    last_pickled = Column(UtcDateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(UtcDateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = Column(Boolean)
    # Foreign key to the latest pickle_id
    pickle_id = Column(Integer)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    fileloc = Column(String(2000))
    # String representing the owners
    owners = Column(String(2000))
    # Description of the dag
    description = Column(Text)
    # Default view of the inside the webserver
    default_view = Column(String(25))
    # Schedule interval
    schedule_interval = Column(Interval)
    # Tags for view filter
    tags = relationship('DagTag', cascade='all,delete-orphan', backref=backref('dag'))

    concurrency = Column(Integer, nullable=False)

    has_task_concurrency_limits = Column(Boolean, nullable=False)

    # The execution_date of the next dag run
    next_dagrun = Column(UtcDateTime)
    # Earliest time at which this ``next_dagrun`` can be created
    next_dagrun_create_after = Column(UtcDateTime)

    __table_args__ = (
        Index('idx_root_dag_id', root_dag_id, unique=False),
        Index('idx_next_dagrun_create_after', next_dagrun_create_after, unique=False),
    )

    NUM_DAGS_PER_DAGRUN_QUERY = conf.getint('scheduler', 'max_dagruns_to_create_per_loop', fallback=10)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.concurrency is None:
            self.concurrency = conf.getint('core', 'dag_concurrency')
        if self.has_task_concurrency_limits is None:
            # Be safe -- this will be updated later once the DAG is parsed
            self.has_task_concurrency_limits = True

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @property
    def timezone(self):
        return settings.TIMEZONE

    @staticmethod
    @provide_session
    def get_dagmodel(dag_id, session=None):
        return session.query(DagModel).filter(DagModel.dag_id == dag_id).first()

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_externally_triggered=include_externally_triggered
        )

    @staticmethod
    @provide_session
    def get_paused_dag_ids(dag_ids: List[str], session: Session = None) -> Set[str]:
        """
        Given a list of dag_ids, get a set of Paused Dag Ids

        :param dag_ids: List of Dag ids
        :param session: ORM Session
        :return: Paused Dag_ids
        """
        paused_dag_ids = (
            session.query(DagModel.dag_id)
            .filter(DagModel.is_paused.is_(True))
            .filter(DagModel.dag_id.in_(dag_ids))
            .all()
        )

        paused_dag_ids = {paused_dag_id for paused_dag_id, in paused_dag_ids}
        return paused_dag_ids

    def get_default_view(self) -> str:
        """
        Get the Default DAG View, returns the default config value if DagModel does not
        have a value
        """
        # This is for backwards-compatibility with old dags that don't have None as default_view
        return self.default_view or conf.get('webserver', 'dag_default_view').lower()

    @property
    def safe_dag_id(self):
        return self.dag_id.replace('.', '__dot__')

    @provide_session
    def set_is_paused(self, is_paused: bool, including_subdags: bool = True, session=None) -> None:
        """
        Pause/Un-pause a DAG.

        :param is_paused: Is the DAG paused
        :param including_subdags: whether to include the DAG's subdags
        :param session: session
        """
        filter_query = [
            DagModel.dag_id == self.dag_id,
        ]
        if including_subdags:
            filter_query.append(DagModel.root_dag_id == self.dag_id)
        session.query(DagModel).filter(or_(*filter_query)).update(
            {DagModel.is_paused: is_paused}, synchronize_session='fetch'
        )
        session.commit()

    @classmethod
    @provide_session
    def deactivate_deleted_dags(cls, alive_dag_filelocs: List[str], session=None):
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.
        Additionally change ``is_active=False`` to ``True`` if the DAG file exists.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        log.debug("Deactivating DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__)

        dag_models = session.query(cls).all()
        try:
            for dag_model in dag_models:
                if dag_model.fileloc is not None:
                    if correct_maybe_zipped(dag_model.fileloc) not in alive_dag_filelocs:
                        dag_model.is_active = False
                    else:
                        # If is_active is set as False and the DAG File still exists
                        # Change is_active=True
                        if not dag_model.is_active:
                            dag_model.is_active = True
                else:
                    continue
            session.commit()
        except Exception:
            session.rollback()
            raise

    @classmethod
    def dags_needing_dagruns(cls, session: Session):
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows  that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.
        """
        # TODO[HA]: Bake this query, it is run _A lot_
        # We limit so that _one_ scheduler doesn't try to do all the creation
        # of dag runs
        query = (
            session.query(cls)
            .filter(
                cls.is_paused.is_(False),
                cls.is_active.is_(True),
                cls.next_dagrun_create_after <= func.now(),
            )
            .order_by(cls.next_dagrun_create_after)
            .limit(cls.NUM_DAGS_PER_DAGRUN_QUERY)
        )

        return with_row_locks(query, of=cls, session=session, **skip_locked(session=session))

    def calculate_dagrun_date_fields(
        self, dag: DAG, most_recent_dag_run: Optional[pendulum.DateTime], active_runs_of_dag: int
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``

        :param dag: The DAG object
        :param most_recent_dag_run: DateTime of most recent run of this dag, or none if not yet scheduled.
        :param active_runs_of_dag: Number of currently active runs of this dag
        """
        self.next_dagrun, self.next_dagrun_create_after = dag.next_dagrun_info(most_recent_dag_run)

        if dag.max_active_runs and active_runs_of_dag >= dag.max_active_runs:
            # Since this happens every time the dag is parsed it would be quite spammy at info
            log.debug(
                "DAG %s is at (or above) max_active_runs (%d of %d), not creating any more runs",
                dag.dag_id,
                active_runs_of_dag,
                dag.max_active_runs,
            )
            self.next_dagrun_create_after = None

        log.info("Setting next_dagrun for %s to %s", dag.dag_id, self.next_dagrun)


def dag(*dag_args, **dag_kwargs):
    """
    Python dag decorator. Wraps a function into an Airflow DAG.
    Accepts kwargs for operator kwarg. Can be used to parametrize DAGs.

    :param dag_args: Arguments for DAG object
    :type dag_args: list
    :param dag_kwargs: Kwargs for DAG object.
    :type dag_kwargs: dict
    """

    def wrapper(f: Callable):
        # Get dag initializer signature and bind it to validate that dag_args, and dag_kwargs are correct
        dag_sig = signature(DAG.__init__)
        dag_bound_args = dag_sig.bind_partial(*dag_args, **dag_kwargs)

        @functools.wraps(f)
        def factory(*args, **kwargs):
            # Generate signature for decorated function and bind the arguments when called
            # we do this to extract parameters so we can annotate them on the DAG object.
            # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
            f_sig = signature(f).bind(*args, **kwargs)
            # Apply defaults to capture default values if set.
            f_sig.apply_defaults()

            # Set function name as dag_id if not set
            dag_id = dag_bound_args.arguments.get('dag_id', f.__name__)
            dag_bound_args.arguments['dag_id'] = dag_id

            # Initialize DAG with bound arguments
            with DAG(*dag_bound_args.args, **dag_bound_args.kwargs) as dag_obj:
                # Set DAG documentation from function documentation.
                if f.__doc__:
                    dag_obj.doc_md = f.__doc__

                # Generate DAGParam for each function arg/kwarg and replace it for calling the function.
                # All args/kwargs for function will be DAGParam object and replaced on execution time.
                f_kwargs = {}
                for name, value in f_sig.arguments.items():
                    f_kwargs[name] = dag_obj.param(name, value)

                # Invoke function to create operators in the DAG scope.
                f(**f_kwargs)

            # Return dag object such that it's accessible in Globals.
            return dag_obj

        return factory

    return wrapper


STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover

    from airflow.models.serialized_dag import SerializedDagModel

    DagModel.serialized_dag = relationship(SerializedDagModel)


class DagContext:
    """
    DAG context is used to keep the current DAG when DAG is used as ContextManager.

    You can use DAG as context:

    .. code-block:: python

        with DAG(
            dag_id='example_dag',
            default_args=default_args,
            schedule_interval='0 0 * * *',
            dagrun_timeout=timedelta(minutes=60)
        ) as dag:

    If you do this the context stores the DAG and whenever new task is created, it will use
    such stored DAG as the parent DAG.

    """

    _context_managed_dag: Optional[DAG] = None
    _previous_context_managed_dags: List[DAG] = []

    @classmethod
    def push_context_managed_dag(cls, dag: DAG):
        if cls._context_managed_dag:
            cls._previous_context_managed_dags.append(cls._context_managed_dag)
        cls._context_managed_dag = dag

    @classmethod
    def pop_context_managed_dag(cls) -> Optional[DAG]:
        old_dag = cls._context_managed_dag
        if cls._previous_context_managed_dags:
            cls._context_managed_dag = cls._previous_context_managed_dags.pop()
        else:
            cls._context_managed_dag = None
        return old_dag

    @classmethod
    def get_current_dag(cls) -> Optional[DAG]:
        return cls._context_managed_dag
