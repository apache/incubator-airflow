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
import contextlib
import hashlib
import logging
import math
import os
import pickle
import signal
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from typing import IO, Any, Dict, Iterable, List, NamedTuple, Optional, Tuple, Union
from urllib.parse import quote

import dill
import jinja2
import lazy_object_proxy
import pendulum
from jinja2 import TemplateAssertionError, UndefinedError
from sqlalchemy import Column, Float, Index, Integer, PickleType, String, and_, func, or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import reconstructor, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.elements import BooleanClauseList

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSkipException,
    AirflowSmartSensorException,
    AirflowTaskTimeout,
)
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.models.log import Log
from airflow.models.taskfail import TaskFail
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.variable import Variable
from airflow.models.xcom import XCOM_RETURN_KEY, XCom
from airflow.plugins_manager import integrate_macros_plugins
from airflow.sentry import Sentry
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.utils import timezone
from airflow.utils.email import send_email
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.platform import getuser
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
from airflow.utils.state import State
from airflow.utils.timeout import timeout

try:
    from kubernetes.client.api_client import ApiClient

    from airflow.kubernetes.kube_config import KubeConfig
    from airflow.kubernetes.pod_generator import PodGenerator
except ImportError:
    ApiClient = None

TR = TaskReschedule
Context = Dict[str, Any]

_CURRENT_CONTEXT: List[Context] = []
log = logging.getLogger(__name__)


@contextlib.contextmanager
def set_current_context(context: Context):
    """
    Sets the current execution context to the provided context object.
    This method should be called once per Task execution, before calling operator.execute.
    """
    _CURRENT_CONTEXT.append(context)
    try:
        yield context
    finally:
        expected_state = _CURRENT_CONTEXT.pop()
        if expected_state != context:
            log.warning(
                "Current context is not equal to the state at context stack. Expected=%s, got=%s",
                context,
                expected_state,
            )


def load_error_file(fd: IO[bytes]) -> Optional[Union[str, Exception]]:
    """Load and return error from error file"""
    fd.seek(0, os.SEEK_SET)
    data = fd.read()
    if not data:
        return None
    try:
        return pickle.loads(data)
    except Exception:  # pylint: disable=broad-except
        return "Failed to load task run error"


def set_error_file(error_file: str, error: Union[str, Exception]) -> None:
    """Write error into error file by path"""
    with open(error_file, "wb") as fd:
        try:
            pickle.dump(error, fd)
        except Exception:  # pylint: disable=broad-except
            # local class objects cannot be pickled, so we fallback
            # to store the string representation instead
            pickle.dump(str(error), fd)


def clear_task_instances(
    tis,
    session,
    activate_dag_runs=True,
    dag=None,
):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids = []
    task_id_by_key = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                ti.refresh_from_task(task)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the last attempted try number.
                ti.max_tries = max(ti.max_tries, ti.prev_attempted_tries)
            ti.state = State.NONE
            session.merge(ti)

        task_id_by_key[ti.dag_id][ti.execution_date][ti.try_number].add(ti.task_id)

    if task_id_by_key:
        # Clear all reschedules related to the ti to clear

        # This is an optimization for the common case where all tis are for a small number
        # of dag_id, execution_date and try_number. Use a nested dict of dag_id,
        # execution_date, try_number and task_id to construct the where clause in a
        # hierarchical manner. This speeds up the delete statement by more than 40x for
        # large number of tis (50k+).
        conditions = or_(
            and_(
                TR.dag_id == dag_id,
                or_(
                    and_(
                        TR.execution_date == execution_date,
                        or_(
                            and_(TR.try_number == try_number, TR.task_id.in_(task_ids))
                            for try_number, task_ids in task_tries.items()
                        ),
                    )
                    for execution_date, task_tries in dates.items()
                ),
            )
            for dag_id, dates in task_id_by_key.items()
        )

        delete_qry = TR.__table__.delete().where(conditions)
        session.execute(delete_qry)

    if job_ids:
        from airflow.jobs.base_job import BaseJob

        for job in session.query(BaseJob).filter(BaseJob.id.in_(job_ids)).all():  # noqa
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        from airflow.models.dagrun import DagRun  # Avoid circular import

        drs = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id.in_({ti.dag_id for ti in tis}),
                DagRun.execution_date.in_({ti.execution_date for ti in tis}),
            )
            .all()
        )
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()


class TaskInstanceKey(NamedTuple):
    """Key used to identify task instance."""

    dag_id: str
    task_id: str
    execution_date: datetime
    try_number: int

    @property
    def primary(self) -> Tuple[str, str, datetime]:
        """Return task instance primary key part of the key"""
        return self.dag_id, self.task_id, self.execution_date

    @property
    def reduced(self) -> 'TaskInstanceKey':
        """Remake the key by subtracting 1 from try number to match in memory information"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.execution_date, max(1, self.try_number - 1))

    def with_try_number(self, try_number: int) -> 'TaskInstanceKey':
        """Returns TaskInstanceKey with provided ``try_number``"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.execution_date, try_number)


class TaskInstance(Base, LoggingMixin):  # pylint: disable=R0902,R0904
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(256), nullable=False)
    pool_slots = Column(Integer, default=1, nullable=False)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    queued_by_job_id = Column(Integer)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))

    external_executor_id = Column(String(ID_LEN, **COLLATION_ARGS))
    # If adding new fields here then remember to add them to
    # refresh_from_db() or they won't display in the UI correctly

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_dag_date', dag_id, execution_date),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
    )

    dag_model = relationship(
        "DagModel",
        primaryjoin="TaskInstance.dag_id == DagModel.dag_id",
        foreign_keys=dag_id,
        uselist=False,
        innerjoin=True,
    )

    def __init__(self, task, execution_date: datetime, state: Optional[str] = None):
        super().__init__()
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.task = task
        self.refresh_from_task(task)
        self._log = logging.getLogger("airflow.task")

        # make sure we have a localized execution_date stored in UTC
        if execution_date and not timezone.is_localized(execution_date):
            self.log.warning(
                "execution date %s has no timezone information. Using default from dag or system",
                execution_date,
            )
            if self.task.has_dag():
                execution_date = timezone.make_aware(execution_date, self.task.dag.timezone)
            else:
                execution_date = timezone.make_aware(execution_date)

            execution_date = timezone.convert_to_utc(execution_date)

        self.execution_date = execution_date

        self.try_number = 0
        self.unixname = getuser()
        if state:
            self.state = state
        self.hostname = ''
        self.init_on_load()
        # Is this TaskInstance being currently running within `airflow tasks run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False
        # can be changed when calling 'run'
        self.test_mode = False

    @reconstructor
    def init_on_load(self):
        """Initialize the attributes that aren't stored in the DB"""
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.

        If the TaskInstance is currently running, this will match the column in the
        database, in all other cases this will be incremented.
        """
        # This is designed so that task logs end up in the right file.
        # TODO: whether we need sensing here or not (in sensor and task_instance state machine)
        if self.state in State.running:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    @property
    def prev_attempted_tries(self):
        """
        Based on this instance's try_number, this will calculate
        the number of previously attempted tries, defaulting to 0.
        """
        # Expose this for the Task Tries and Gantt graph views.
        # Using `try_number` throws off the counts for non-running tasks.
        # Also useful in error logging contexts to get
        # the try number for the last try that was attempted.
        # https://issues.apache.org/jira/browse/AIRFLOW-2143

        return self._try_number

    @property
    def next_try_number(self):
        """Setting Next Try Number"""
        return self._try_number + 1

    def command_as_list(  # pylint: disable=too-many-arguments
        self,
        mark_success=False,
        ignore_all_deps=False,
        ignore_task_deps=False,
        ignore_depends_on_past=False,
        ignore_ti_state=False,
        local=False,
        pickle_id=None,
        raw=False,
        job_id=None,
        pool=None,
        cfg_path=None,
    ):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag

        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = f"DAGS_FOLDER/{dag.filepath}"
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath
        else:
            path = None

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path,
        )

    @staticmethod
    def generate_command(
        dag_id: str,  # pylint: disable=too-many-arguments
        task_id: str,
        execution_date: datetime,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        local: bool = False,
        pickle_id: Optional[int] = None,
        file_path: Optional[str] = None,
        raw: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        cfg_path: Optional[str] = None,
    ) -> List[str]:
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: str
        :param task_id: Task ID
        :type task_id: str
        :param execution_date: Execution date for the task
        :type execution_date: datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: bool
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: bool
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: bool
        :param local: Whether to run the task locally
        :type local: bool
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: Optional[int]
        :param file_path: path to the file containing the DAG definition
        :type file_path: Optional[str]
        :param raw: raw mode (needs more details)
        :type raw: Optional[bool]
        :param job_id: job ID (needs more details)
        :type job_id: Optional[int]
        :param pool: the Airflow pool that the task should run in
        :type pool: Optional[str]
        :param cfg_path: the Path to the configuration file
        :type cfg_path: Optional[str]
        :return: shell command that can be used to run the task instance
        :rtype: list[str]
        """
        iso = execution_date.isoformat()
        cmd = ["airflow", "tasks", "run", dag_id, task_id, iso]
        if mark_success:
            cmd.extend(["--mark-success"])
        if pickle_id:
            cmd.extend(["--pickle", str(pickle_id)])
        if job_id:
            cmd.extend(["--job-id", str(job_id)])
        if ignore_all_deps:
            cmd.extend(["--ignore-all-dependencies"])
        if ignore_task_deps:
            cmd.extend(["--ignore-dependencies"])
        if ignore_depends_on_past:
            cmd.extend(["--ignore-depends-on-past"])
        if ignore_ti_state:
            cmd.extend(["--force"])
        if local:
            cmd.extend(["--local"])
        if pool:
            cmd.extend(["--pool", pool])
        if raw:
            cmd.extend(["--raw"])
        if file_path:
            cmd.extend(["--subdir", file_path])
        if cfg_path:
            cmd.extend(["--cfg-path", cfg_path])
        return cmd

    @property
    def log_filepath(self):
        """Filepath for TaskInstance"""
        iso = self.execution_date.isoformat()
        the_log = os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER'))
        return f"{the_log}/{self.dag_id}/{self.task_id}/{iso}.log"

    @property
    def log_url(self):
        """Log URL for TaskInstance"""
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + f"/log?execution_date={iso}&task_id={self.task_id}&dag_id={self.dag_id}"

    @property
    def mark_success_url(self):
        """URL to mark TI success"""
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/success"
            f"?task_id={self.task_id}"
            f"&dag_id={self.dag_id}"
            f"&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        )

    @provide_session
    def current_state(self, session=None) -> str:
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.

        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.execution_date == self.execution_date,
            )
            .all()
        )
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session=None):
        """
        Forces the task instance's state to FAILED in the database.

        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False) -> None:
        """
        Refreshes the task instance from the database based on the primary key

        :param session: SQLAlchemy ORM Session
        :type session: Session
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        :type lock_for_update: bool
        """
        self.log.debug("Refreshing TaskInstance %s from DB", self)

        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.execution_date == self.execution_date,
        )

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            # Fields ordered per model definition
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.duration = ti.duration
            self.state = ti.state
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremented by one already.
            self.try_number = ti._try_number  # noqa pylint: disable=protected-access
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.unixname = ti.unixname
            self.job_id = ti.job_id
            self.pool = ti.pool
            self.pool_slots = ti.pool_slots or 1
            self.queue = ti.queue
            self.priority_weight = ti.priority_weight
            self.operator = ti.operator
            self.queued_dttm = ti.queued_dttm
            self.pid = ti.pid
        else:
            self.state = None

        self.log.debug("Refreshed TaskInstance %s", self)

    def refresh_from_task(self, task, pool_override=None):
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :type task: airflow.models.BaseOperator
        :param pool_override: Use the pool_override instead of task's pool
        :type pool_override: str
        """
        self.queue = task.queue
        self.pool = pool_override or task.pool
        self.pool_slots = task.pool_slots
        self.priority_weight = task.priority_weight_total
        self.run_as_user = task.run_as_user
        self.max_tries = task.retries
        self.executor_config = task.executor_config
        self.operator = task.task_type

    @provide_session
    def clear_xcom_data(self, session=None):
        """
        Clears all XCom data from the database for the task instance

        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        self.log.debug("Clearing XCom data")
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date,
        ).delete()
        session.commit()
        self.log.debug("XCom data cleared")

    @property
    def key(self) -> TaskInstanceKey:
        """Returns a tuple that identifies the task instance uniquely"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.execution_date, self.try_number)

    @provide_session
    def set_state(self, state: str, session=None):
        """
        Set TaskInstance state.

        :param state: State to set for the TI
        :type state: str
        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        current_time = timezone.utcnow()
        self.log.debug("Setting task state for %s to %s", self, state)
        self.state = state
        self.start_date = self.start_date or current_time
        if self.state in State.finished or self.state == State.UP_FOR_RETRY:
            self.end_date = self.end_date or current_time
            self.duration = (self.end_date - self.start_date).total_seconds()
        session.merge(self)

    @property
    def is_premature(self):
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session=None):
        """
        Checks whether the immediate dependents of this task instance have succeeded or have been skipped.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.

        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state.in_([State.SKIPPED, State.SUCCESS]),
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @provide_session
    def get_previous_ti(
        self, state: Optional[str] = None, session: Session = None
    ) -> Optional['TaskInstance']:
        """
        The task instance for the task that ran before this task instance.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        dag = self.task.dag
        if dag:
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TaskInstance is NOT being run from a DR, but from a catchup
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None

                return TaskInstance(task=self.task, execution_date=previous_scheduled_date)

            dr.dag = dag

            # We always ignore schedule in dagrun lookup when `state` is given or `schedule_interval is None`.
            # For legacy reasons, when `catchup=True`, we use `get_previous_scheduled_dagrun` unless
            # `ignore_schedule` is `True`.
            ignore_schedule = state is not None or dag.schedule_interval is None
            if dag.catchup is True and not ignore_schedule:
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                last_dagrun = dr.get_previous_dagrun(session=session, state=state)

            if last_dagrun:
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @property
    def previous_ti(self):
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_ti()

    @property
    def previous_ti_success(self) -> Optional['TaskInstance']:
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_ti(state=State.SUCCESS)

    @provide_session
    def get_previous_execution_date(
        self,
        state: Optional[str] = None,
        session: Session = None,
    ) -> Optional[pendulum.DateTime]:
        """
        The execution date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        self.log.debug("previous_execution_date was called")
        prev_ti = self.get_previous_ti(state=state, session=session)
        return prev_ti and pendulum.instance(prev_ti.execution_date)

    @provide_session
    def get_previous_start_date(
        self, state: Optional[str] = None, session: Session = None
    ) -> Optional[pendulum.DateTime]:
        """
        The start date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        self.log.debug("previous_start_date was called")
        prev_ti = self.get_previous_ti(state=state, session=session)
        # prev_ti may not exist and prev_ti.start_date may be None.
        return prev_ti and prev_ti.start_date and pendulum.instance(prev_ti.start_date)

    @property
    def previous_start_date_success(self) -> Optional[pendulum.DateTime]:
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_start_date(state=State.SUCCESS)

    @provide_session
    def are_dependencies_met(self, dep_context=None, session=None, verbose=False):
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :type dep_context: DepContext
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        :type verbose: bool
        """
        dep_context = dep_context or DepContext()
        failed = False
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        for dep_status in self.get_failed_dep_statuses(dep_context=dep_context, session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self,
                dep_status.dep_name,
                dep_status.reason,
            )

        if failed:
            return False

        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(self, dep_context=None, session=None):
        """Get failed Dependencies"""
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(self, session, dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self,
                    dep_status.dep_name,
                    dep_status.passed,
                    dep_status.reason,
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        return f"<TaskInstance: {self.dag_id}.{self.task_id} {self.execution_date} [{self.state}]>"

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
            # we must round up prior to converting to an int, otherwise a divide by zero error
            # will occur in the modded_hash calculation.
            min_backoff = int(math.ceil(delay.total_seconds() * (2 ** (self.try_number - 2))))
            # deterministic per task instance
            ti_hash = int(
                hashlib.sha1(
                    "{}#{}#{}#{}".format(
                        self.dag_id, self.task_id, self.execution_date, self.try_number  # noqa
                    ).encode('utf-8')
                ).hexdigest(),
                16,
            )
            # between 1 and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + ti_hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(modded_hash, timedelta.max.total_seconds() - 1)
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return self.state == State.UP_FOR_RETRY and self.next_retry_datetime() < timezone.utcnow()

    @provide_session
    def get_dagrun(self, session: Session = None):
        """
        Returns the DagRun for this TaskInstance

        :param session: SQLAlchemy ORM Session
        :return: DagRun
        """
        from airflow.models.dagrun import DagRun  # Avoid circular import

        dr = (
            session.query(DagRun)
            .filter(DagRun.dag_id == self.dag_id, DagRun.execution_date == self.execution_date)
            .first()
        )

        return dr

    @provide_session
    def check_and_change_state_before_execution(  # pylint: disable=too-many-arguments
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        session=None,
    ) -> bool:
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: bool
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Don't check the dependencies of this TaskInstance's task
        :type ignore_task_deps: bool
        :param ignore_ti_state: Disregards previous task instance state
        :type ignore_ti_state: bool
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param job_id: Job (BackfillJob / LocalTaskJob / SchedulerJob) ID
        :type job_id: str
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :param session: SQLAlchemy ORM Session
        :type session: Session
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.refresh_from_task(task, pool_override=pool)
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        # TODO: Logging needs cleanup, not clear what is being printed
        hr_line_break = "\n" + ("-" * 80)  # Line break

        if not mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_ti_state=ignore_ti_state,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
            )
            if not self.are_dependencies_met(
                dep_context=non_requeueable_dep_context, session=session, verbose=True
            ):
                session.commit()
                return False

            # For reporting purposes, we report based on 1-indexed,
            # not 0-indexed lists (i.e. Attempt 1 instead of
            # Attempt 0 for the first attempt).
            # Set the task start date. In case it was re-scheduled use the initial
            # start date that is recorded in task_reschedule table
            self.start_date = timezone.utcnow()
            if self.state == State.UP_FOR_RESCHEDULE:
                task_reschedule: TR = TR.query_for_task_instance(self, session=session).first()
                if task_reschedule:
                    self.start_date = task_reschedule.start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state,
            )
            if not self.are_dependencies_met(dep_context=dep_context, session=session, verbose=True):
                self.state = State.NONE
                self.log.warning(hr_line_break)
                self.log.warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to NONE.",
                    self.try_number,
                    self.max_tries + 1,
                )
                self.log.warning(hr_line_break)
                self.queued_dttm = timezone.utcnow()
                session.merge(self)
                session.commit()
                return False

        # print status message
        self.log.info(hr_line_break)
        self.log.info("Starting attempt %s of %s", self.try_number, self.max_tries + 1)
        self.log.info(hr_line_break)
        self._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()  # type: ignore
        if verbose:
            if mark_success:
                self.log.info("Marking success for %s on %s", self.task, self.execution_date)
            else:
                self.log.info("Executing %s on %s", self.task, self.execution_date)
        return True

    def _date_or_empty(self, attr):
        if hasattr(self, attr):
            date = getattr(self, attr)
            if date:
                return date.strftime('%Y%m%dT%H%M%S')
        return ''

    @provide_session
    @Sentry.enrich_errors
    def _run_raw_task(
        self,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        error_file: Optional[str] = None,
        session=None,
    ) -> None:
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :param session: SQLAlchemy ORM Session
        :type session: Session
        """
        task = self.task
        self.test_mode = test_mode
        self.refresh_from_task(task, pool_override=pool)
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.pid = os.getpid()
        session.merge(self)
        session.commit()
        actual_start_date = timezone.utcnow()
        Stats.incr(f'ti.start.{task.dag_id}.{task.task_id}')
        try:
            if not mark_success:
                context = self.get_template_context()
                self._prepare_and_execute_task_with_callbacks(context, task)
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSmartSensorException as e:
            self.log.info(e)
            return
        except AirflowSkipException as e:
            # Recording SKIP
            # log only if exception has any arguments to prevent log flooding
            if e.args:
                self.log.info(e)
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
            self.log.info(
                'Marking task as SKIPPED. '
                'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                self.dag_id,
                self.task_id,
                self._date_or_empty('execution_date'),
                self._date_or_empty('start_date'),
                self._date_or_empty('end_date'),
            )
        except AirflowRescheduleException as reschedule_exception:
            self.refresh_from_db()
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode)
            return
        except AirflowFailException as e:
            self.refresh_from_db()
            self.handle_failure(e, test_mode, force_fail=True, error_file=error_file)
            raise
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                return
            else:
                self.handle_failure(e, test_mode, error_file=error_file)
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, error_file=error_file)
            raise
        finally:
            Stats.incr(f'ti.finish.{task.dag_id}.{task.task_id}.{self.state}')

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.log.info(
            'Marking task as SUCCESS. '
            'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
            self.dag_id,
            self.task_id,
            self._date_or_empty('execution_date'),
            self._date_or_empty('start_date'),
            self._date_or_empty('end_date'),
        )
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)

        session.commit()

        if not test_mode:
            self._run_mini_scheduler_on_child_tasks(session)

    @provide_session
    @Sentry.enrich_errors
    def _run_mini_scheduler_on_child_tasks(self, session=None) -> None:
        if conf.getboolean('scheduler', 'schedule_after_task_execution', fallback=True):
            from airflow.models.dagrun import DagRun  # Avoid circular import

            try:
                # Re-select the row with a lock
                dag_run = with_row_locks(
                    session.query(DagRun).filter_by(
                        dag_id=self.dag_id,
                        execution_date=self.execution_date,
                    ),
                    session=session,
                ).one()

                # Get a partial dag with just the specific tasks we want to
                # examine. In order for dep checks to work correctly, we
                # include ourself (so TriggerRuleDep can check the state of the
                # task we just executed)
                partial_dag = self.task.dag.partial_subset(
                    self.task.downstream_task_ids,
                    include_downstream=False,
                    include_upstream=False,
                    include_direct_upstream=True,
                )

                dag_run.dag = partial_dag
                info = dag_run.task_instance_scheduling_decisions(session)

                skippable_task_ids = {
                    task_id
                    for task_id in partial_dag.task_ids
                    if task_id not in self.task.downstream_task_ids
                }

                schedulable_tis = [ti for ti in info.schedulable_tis if ti.task_id not in skippable_task_ids]
                for schedulable_ti in schedulable_tis:
                    if not hasattr(schedulable_ti, "task"):
                        schedulable_ti.task = self.task.dag.get_task(schedulable_ti.task_id)

                num = dag_run.schedule_tis(schedulable_tis)
                self.log.info("%d downstream tasks scheduled from follow-on schedule check", num)

                session.commit()
            except OperationalError as e:
                # Any kind of DB error here is _non fatal_ as this block is just an optimisation.
                self.log.info(
                    f"Skipping mini scheduling run due to exception: {e.statement}",
                    exc_info=True,
                )
                session.rollback()

    def _prepare_and_execute_task_with_callbacks(self, context, task):
        """Prepare Task for Execution"""
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        task_copy = task.prepare_for_execution()
        self.task = task_copy

        def signal_handler(signum, frame):  # pylint: disable=unused-argument
            self.log.error("Received SIGTERM. Terminating subprocesses.")
            task_copy.on_kill()
            raise AirflowException("Task received SIGTERM signal")

        signal.signal(signal.SIGTERM, signal_handler)

        # Don't clear Xcom until the task is certain to execute
        self.clear_xcom_data()
        with Stats.timer(f'dag.{task_copy.dag_id}.{task_copy.task_id}.duration'):

            self.render_templates(context=context)
            RenderedTaskInstanceFields.write(RenderedTaskInstanceFields(ti=self, render_templates=False))
            RenderedTaskInstanceFields.delete_old_records(self.task_id, self.dag_id)

            # Export context to make it available for operators to use.
            airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
            self.log.info(
                "Exporting the following env vars:\n%s",
                '\n'.join([f"{k}={v}" for k, v in airflow_context_vars.items()]),
            )

            os.environ.update(airflow_context_vars)

            # Run pre_execute callback
            task_copy.pre_execute(context=context)

            # Run on_execute callback
            self._run_execute_callback(context, task)

            if task_copy.is_smart_sensor_compatible():
                # Try to register it in the smart sensor service.
                registered = False
                try:
                    registered = task_copy.register_in_sensor_service(self, context)
                except Exception:  # pylint: disable=broad-except
                    self.log.warning(
                        "Failed to register in sensor service."
                        " Continue to run task in non smart sensor mode.",
                        exc_info=True,
                    )

                if registered:
                    # Will raise AirflowSmartSensorException to avoid long running execution.
                    self._update_ti_state_for_sensing()

            # Execute the task
            with set_current_context(context):
                result = self._execute_task(context, task_copy)

            # Run post_execute callback
            task_copy.post_execute(context=context, result=result)

        Stats.incr(f'operator_successes_{self.task.task_type}', 1, 1)
        Stats.incr('ti_successes')

    @provide_session
    def _update_ti_state_for_sensing(self, session=None):
        self.log.info('Submitting %s to sensor service', self)
        self.state = State.SENSING
        self.start_date = timezone.utcnow()
        session.merge(self)
        session.commit()
        # Raise exception for sensing state
        raise AirflowSmartSensorException("Task successfully registered in smart sensor.")

    def _execute_task(self, context, task_copy):
        """Executes Task (optionally with a Timeout) and pushes Xcom results"""
        # If a timeout is specified for the task, make it fail
        # if it goes beyond
        if task_copy.execution_timeout:
            try:
                with timeout(task_copy.execution_timeout.total_seconds()):
                    result = task_copy.execute(context=context)
            except AirflowTaskTimeout:
                task_copy.on_kill()
                raise
        else:
            result = task_copy.execute(context=context)
        # If the task returns a result, push an XCom containing it
        if task_copy.do_xcom_push and result is not None:
            self.xcom_push(key=XCOM_RETURN_KEY, value=result)
        return result

    def _run_execute_callback(self, context: Context, task):
        """Functions that need to be run before a Task is executed"""
        try:
            if task.on_execute_callback:
                task.on_execute_callback(context)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Failed when executing execute callback")

    def _run_finished_callback(self, error: Optional[Union[str, Exception]] = None) -> None:
        """
        Call callback defined for finished state change.

        NOTE: Only invoke this function from caller of self._run_raw_task or
        self.run
        """
        if self.state == State.FAILED:
            task = self.task
            if task.on_failure_callback is not None:
                context = self.get_template_context()
                context["exception"] = error
                task.on_failure_callback(context)
        elif self.state == State.SUCCESS:
            task = self.task
            if task.on_success_callback is not None:
                context = self.get_template_context()
                task.on_success_callback(context)
        elif self.state == State.UP_FOR_RETRY:
            task = self.task
            if task.on_retry_callback is not None:
                context = self.get_template_context()
                context["exception"] = error
                task.on_retry_callback(context)

    @provide_session
    def run(  # pylint: disable=too-many-arguments
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        session=None,
    ) -> None:
        """Run TaskInstance"""
        res = self.check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session,
        )
        if not res:
            return

        try:
            error_fd = NamedTemporaryFile(delete=True)
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                error_file=error_fd.name,
                session=session,
            )
        finally:
            error = None if self.state == State.SUCCESS else load_error_file(error_fd)
            error_fd.close()
            self._run_finished_callback(error=error)

    def dry_run(self):
        """Only Renders Templates for the TI"""
        task = self.task
        task_copy = task.prepare_for_execution()
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def _handle_reschedule(self, actual_start_date, reschedule_exception, test_mode=False, session=None):
        # Don't record reschedule request in test mode
        if test_mode:
            return

        self.end_date = timezone.utcnow()
        self.set_duration()

        # Log reschedule request
        session.add(
            TaskReschedule(
                self.task,
                self.execution_date,
                self._try_number,
                actual_start_date,
                self.end_date,
                reschedule_exception.reschedule_date,
            )
        )

        # set state
        self.state = State.UP_FOR_RESCHEDULE

        # Decrement try_number so subsequent runs will use the same try number and write
        # to same log file.
        self._try_number -= 1

        session.merge(self)
        session.commit()
        self.log.info('Rescheduling task, marking task as UP_FOR_RESCHEDULE')

    @provide_session
    def handle_failure(
        self,
        error: Union[str, Exception],
        test_mode: Optional[bool] = None,
        force_fail: bool = False,
        error_file: Optional[str] = None,
        session=None,
    ) -> None:
        """Handle Failure for the TaskInstance"""
        if test_mode is None:
            test_mode = self.test_mode

        if error:
            if isinstance(error, Exception):
                self.log.exception("Task failed with exception")
            else:
                self.log.error("%s", error)
            # external monitoring process provides pickle file so _run_raw_task
            # can send its runtime errors for access by failure callback
            if error_file:
                set_error_file(error_file, error)

        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr(f'operator_failures_{task.task_type}', 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        # Set state correctly and figure out how to log it and decide whether
        # to email

        # Note, callback invocation needs to be handled by caller of
        # _run_raw_task to avoid race conditions which could lead to duplicate
        # invocations or miss invocation.

        # Since this function is called only when the TaskInstance state is running,
        # try_number contains the current try_number (not the next). We
        # only mark task instance as FAILED if the next task instance
        # try_number exceeds the max_tries ... or if force_fail is truthy

        if force_fail or not self.is_eligible_to_retry():
            self.state = State.FAILED
            if force_fail:
                log_message = "Immediate failure requested. Marking task as FAILED."
            else:
                log_message = "Marking task as FAILED."
            email_for_state = task.email_on_failure
        else:
            self.state = State.UP_FOR_RETRY
            log_message = "Marking task as UP_FOR_RETRY."
            email_for_state = task.email_on_retry

        self.log.info(
            '%s dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
            log_message,
            self.dag_id,
            self.task_id,
            self._safe_date('execution_date', '%Y%m%dT%H%M%S'),
            self._safe_date('start_date', '%Y%m%dT%H%M%S'),
            self._safe_date('end_date', '%Y%m%dT%H%M%S'),
        )
        if email_for_state and task.email:
            try:
                self.email_alert(error)
            except Exception:  # pylint: disable=broad-except
                self.log.exception('Failed to send email to: %s', task.email)

        if not test_mode:
            session.merge(self)
        session.commit()

    @provide_session
    def handle_failure_with_callback(
        self,
        error: Union[str, Exception],
        test_mode: Optional[bool] = None,
        force_fail: bool = False,
        session=None,
    ) -> None:
        self.handle_failure(error=error, test_mode=test_mode, force_fail=force_fail, session=session)
        self._run_finished_callback(error=error)

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry"""
        return self.task.retries and self.try_number <= self.max_tries

    def _safe_date(self, date_attr, fmt):
        result = getattr(self, date_attr, None)
        if result is not None:
            return result.strftime(fmt)
        return ''

    @provide_session
    def get_template_context(self, session=None) -> Context:  # pylint: disable=too-many-locals
        """Return TI Context"""
        task = self.task
        from airflow import macros

        integrate_macros_plugins()

        params = {}  # type: Dict[str, Any]
        run_id = ''
        dag_run = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            from airflow.models.dagrun import DagRun  # Avoid circular import

            dag_run = (
                session.query(DagRun)
                .filter_by(dag_id=task.dag.dag_id, execution_date=self.execution_date)
                .first()
            )
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        ds = self.execution_date.strftime('%Y-%m-%d')
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (self.execution_date + timedelta(1)).strftime('%Y-%m-%d')

        # For manually triggered dagruns that aren't run on a schedule, next/previous
        # schedule dates don't make sense, and should be set to execution date for
        # consistency with how execution_date is set for manually triggered tasks, i.e.
        # triggered_date == execution_date.
        if dag_run and dag_run.external_trigger:
            prev_execution_date = self.execution_date
            next_execution_date = self.execution_date
        else:
            prev_execution_date = task.dag.previous_schedule(self.execution_date)
            next_execution_date = task.dag.following_schedule(self.execution_date)

        next_ds = None
        next_ds_nodash = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')
            next_ds_nodash = next_ds.replace('-', '')
            next_execution_date = pendulum.instance(next_execution_date)

        prev_ds = None
        prev_ds_nodash = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')
            prev_ds_nodash = prev_ds.replace('-', '')
            prev_execution_date = pendulum.instance(prev_execution_date)

        ds_nodash = ds.replace('-', '')
        ts_nodash = self.execution_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')

        ti_key_str = f"{task.dag_id}__{task.task_id}__{ds_nodash}"

        if task.params:
            params.update(task.params)

        if conf.getboolean('core', 'dag_run_conf_overrides_params'):
            self.overwrite_params_with_dag_run_conf(params=params, dag_run=dag_run)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in
            templates by using ``{{ var.value.variable_name }}`` or
            ``{{ var.value.get('variable_name', 'fallback') }}``.
            """

            def __init__(self):
                self.var = None

            def __getattr__(
                self,
                item: str,
            ):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

            @staticmethod
            def get(
                item: str,
                # pylint: disable=protected-access
                default_var: Any = Variable._Variable__NO_DEFAULT_SENTINEL,  # noqa
            ):
                """Get Airflow Variable value"""
                return Variable.get(item, default_var=default_var)

        class VariableJsonAccessor:
            """
            Wrapper around Variable. This way you can get variables in
            templates by using ``{{ var.json.variable_name }}`` or
            ``{{ var.json.get('variable_name', {'fall': 'back'}) }}``.
            """

            def __init__(self):
                self.var = None

            def __getattr__(
                self,
                item: str,
            ):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

            @staticmethod
            def get(
                item: str,
                # pylint: disable=protected-access
                default_var: Any = Variable._Variable__NO_DEFAULT_SENTINEL,  # noqa
            ):
                """Get Airflow Variable after deserializing JSON value"""
                return Variable.get(item, default_var=default_var, deserialize_json=True)

        return {
            'conf': conf,
            'dag': task.dag,
            'dag_run': dag_run,
            'ds': ds,
            'ds_nodash': ds_nodash,
            'execution_date': pendulum.instance(self.execution_date),
            'inlets': task.inlets,
            'macros': macros,
            'next_ds': next_ds,
            'next_ds_nodash': next_ds_nodash,
            'next_execution_date': next_execution_date,
            'outlets': task.outlets,
            'params': params,
            'prev_ds': prev_ds,
            'prev_ds_nodash': prev_ds_nodash,
            'prev_execution_date': prev_execution_date,
            'prev_execution_date_success': lazy_object_proxy.Proxy(
                lambda: self.get_previous_execution_date(state=State.SUCCESS)
            ),
            'prev_start_date_success': lazy_object_proxy.Proxy(
                lambda: self.get_previous_start_date(state=State.SUCCESS)
            ),
            'run_id': run_id,
            'task': task,
            'task_instance': self,
            'task_instance_key_str': ti_key_str,
            'test_mode': self.test_mode,
            'ti': self,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'var': {
                'json': VariableJsonAccessor(),
                'value': VariableAccessor(),
            },
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
        }

    def get_rendered_template_fields(self):
        """Fetch rendered template fields from DB"""
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        rendered_task_instance_fields = RenderedTaskInstanceFields.get_templated_fields(self)
        if rendered_task_instance_fields:
            for field_name, rendered_value in rendered_task_instance_fields.items():
                setattr(self.task, field_name, rendered_value)
        else:
            try:
                self.render_templates()
            except (TemplateAssertionError, UndefinedError) as e:
                raise AirflowException(
                    "Webserver does not have access to User-defined Macros or Filters "
                    "when Dag Serialization is enabled. Hence for the task that have not yet "
                    "started running, please use 'airflow tasks render' for debugging the "
                    "rendering of template_fields."
                ) from e

    def get_rendered_k8s_spec(self):
        """Fetch rendered template fields from DB"""
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        rendered_k8s_spec = RenderedTaskInstanceFields.get_k8s_pod_yaml(self)
        if not rendered_k8s_spec:
            try:
                rendered_k8s_spec = self.render_k8s_pod_yaml()
            except (TemplateAssertionError, UndefinedError) as e:
                raise AirflowException(f"Unable to render a k8s spec for this taskinstance: {e}") from e
        return rendered_k8s_spec

    def overwrite_params_with_dag_run_conf(self, params, dag_run):
        """Overwrite Task Params with DagRun.conf"""
        if dag_run and dag_run.conf:
            self.log.debug("Updating task params (%s) with DagRun.conf (%s)", params, dag_run.conf)
            params.update(dag_run.conf)

    def render_templates(self, context: Optional[Context] = None) -> None:
        """Render templates in the operator fields."""
        if not context:
            context = self.get_template_context()

        self.task.render_template_fields(context)

    def render_k8s_pod_yaml(self) -> Optional[dict]:
        """Render k8s pod yaml"""
        from airflow.kubernetes.kubernetes_helper_functions import create_pod_id  # Circular import

        kube_config = KubeConfig()
        pod = PodGenerator.construct_pod(
            dag_id=self.dag_id,
            task_id=self.task_id,
            pod_id=create_pod_id(self.dag_id, self.task_id),
            try_number=self.try_number,
            kube_image=kube_config.kube_image,
            date=self.execution_date,
            args=self.command_as_list(),
            pod_override_object=PodGenerator.from_obj(self.executor_config),
            scheduler_job_id="worker-config",
            namespace=kube_config.executor_namespace,
            base_worker_pod=PodGenerator.deserialize_model_file(kube_config.pod_template_file),
        )
        settings.pod_mutation_hook(pod)
        sanitized_pod = ApiClient().sanitize_for_serialization(pod)
        return sanitized_pod

    def get_email_subject_content(self, exception):
        """Get the email subject content for exceptions."""
        # For a ti from DB (without ti.task), return the default value
        # Reuse it for smart sensor to send default email alert
        use_default = not hasattr(self, 'task')
        exception_html = str(exception).replace('\n', '<br>')

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_html_content = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>{{exception_html}}<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        default_html_content_err = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>Failed attempt to attach error logs<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        if use_default:
            jinja_context = {'ti': self}
            # This function is called after changing the state
            # from State.RUNNING so need to subtract 1 from self.try_number.
            jinja_context.update(
                dict(
                    exception=exception,
                    exception_html=exception_html,
                    try_number=self.try_number - 1,
                    max_tries=self.max_tries,
                )
            )
            if self.dag.render_template_as_native_obj:
                jinja_env = jinja2.nativetypes.NativeEnvironment(
                    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)), autoescape=True
                )
            else:
                jinja_env = jinja2.Environment(
                    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)), autoescape=True
                )
            subject = jinja_env.from_string(default_subject).render(**jinja_context)
            html_content = jinja_env.from_string(default_html_content).render(**jinja_context)
            html_content_err = jinja_env.from_string(default_html_content_err).render(**jinja_context)

        else:
            jinja_context = self.get_template_context()

            jinja_context.update(
                dict(
                    exception=exception,
                    exception_html=exception_html,
                    try_number=self.try_number - 1,
                    max_tries=self.max_tries,
                )
            )

            jinja_env = self.task.get_template_env()

            def render(key, content):
                if conf.has_option('email', key):
                    path = conf.get('email', key)
                    with open(path) as f:
                        content = f.read()
                return jinja_env.from_string(content).render(**jinja_context)

            subject = render('subject_template', default_subject)
            html_content = render('html_content_template', default_html_content)
            html_content_err = render('html_content_template', default_html_content_err)

        return subject, html_content, html_content_err

    def email_alert(self, exception):
        """Send alert email with exception information."""
        subject, html_content, html_content_err = self.get_email_subject_content(exception)
        try:
            send_email(self.task.email, subject, html_content)
        except Exception:
            send_email(self.task.email, subject, html_content_err)

    def set_duration(self) -> None:
        """Set TI duration"""
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None
        self.log.debug("Task Duration set to %s", self.duration)

    @provide_session
    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None,
        session: Session = None,
    ) -> None:
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: str
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any picklable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        :param session: Sqlalchemy ORM Session
        :type session: Session
        """
        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(self.execution_date, execution_date)
            )

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date,
            session=session,
        )

    @provide_session
    def xcom_pull(  # pylint: disable=inconsistent-return-statements
        self,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[str] = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session = None,
    ) -> Any:
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: str
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: str
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        :param session: Sqlalchemy ORM Session
        :type session: Session
        """
        if dag_id is None:
            dag_id = self.dag_id

        query = XCom.get_many(
            execution_date=self.execution_date,
            key=key,
            dag_ids=dag_id,
            task_ids=task_ids,
            include_prior_dates=include_prior_dates,
            session=session,
        )

        # Since we're only fetching the values field, and not the
        # whole class, the @recreate annotation does not kick in.
        # Therefore we need to deserialize the fields by ourselves.
        if is_container(task_ids):
            vals_kv = {
                result.task_id: XCom.deserialize_value(result)
                for result in query.with_entities(XCom.task_id, XCom.value)
            }

            values_ordered_by_id = [vals_kv.get(task_id) for task_id in task_ids]
            return values_ordered_by_id
        else:
            xcom = query.with_entities(XCom.value).first()
            if xcom:
                return XCom.deserialize_value(xcom)

    @provide_session
    def get_num_running_task_instances(self, session):
        """Return Number of running TIs from the DB"""
        # .count() is inefficient
        return (
            session.query(func.count())
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.state == State.RUNNING,
            )
            .scalar()
        )

    def init_run_context(self, raw=False):
        """Sets the log context."""
        self.raw = raw
        self._set_context(self)

    @staticmethod
    def filter_for_tis(tis: Iterable[Union["TaskInstance", TaskInstanceKey]]) -> Optional[BooleanClauseList]:
        """Returns SQLAlchemy filter to query selected task instances"""
        if not tis:
            return None

        # DictKeys type, (what we often pass here from the scheduler) is not directly indexable :(
        first = list(tis)[0]

        dag_id = first.dag_id
        execution_date = first.execution_date
        first_task_id = first.task_id
        # Common path optimisations: when all TIs are for the same dag_id and execution_date, or same dag_id
        # and task_id -- this can be over 150x for huge numbers of TIs (20k+)
        if all(t.dag_id == dag_id and t.execution_date == execution_date for t in tis):
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.execution_date == execution_date,
                TaskInstance.task_id.in_(t.task_id for t in tis),
            )
        if all(t.dag_id == dag_id and t.task_id == first_task_id for t in tis):
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.execution_date.in_(t.execution_date for t in tis),
                TaskInstance.task_id == first_task_id,
            )
        return or_(
            and_(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.execution_date == ti.execution_date,
            )
            for ti in tis
        )


# State of the task instance.
# Stores string version of the task state.
TaskInstanceStateType = Tuple[TaskInstanceKey, str]


class SimpleTaskInstance:
    """
    Simplified Task Instance.

    Used to send data between processes via Queues.
    """

    def __init__(self, ti: TaskInstance):
        self._dag_id: str = ti.dag_id
        self._task_id: str = ti.task_id
        self._execution_date: datetime = ti.execution_date
        self._start_date: datetime = ti.start_date
        self._end_date: datetime = ti.end_date
        self._try_number: int = ti.try_number
        self._state: str = ti.state
        self._executor_config: Any = ti.executor_config
        self._run_as_user: Optional[str] = None
        if hasattr(ti, 'run_as_user'):
            self._run_as_user = ti.run_as_user
        self._pool: str = ti.pool
        self._priority_weight: Optional[int] = None
        if hasattr(ti, 'priority_weight'):
            self._priority_weight = ti.priority_weight
        self._queue: str = ti.queue
        self._key = ti.key

    # pylint: disable=missing-docstring
    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def execution_date(self) -> datetime:
        return self._execution_date

    @property
    def start_date(self) -> datetime:
        return self._start_date

    @property
    def end_date(self) -> datetime:
        return self._end_date

    @property
    def try_number(self) -> int:
        return self._try_number

    @property
    def state(self) -> str:
        return self._state

    @property
    def pool(self) -> str:
        return self._pool

    @property
    def priority_weight(self) -> Optional[int]:
        return self._priority_weight

    @property
    def queue(self) -> str:
        return self._queue

    @property
    def key(self) -> TaskInstanceKey:
        return self._key

    @property
    def executor_config(self):
        return self._executor_config

    @provide_session
    def construct_task_instance(self, session=None, lock_for_update=False) -> TaskInstance:
        """
        Construct a TaskInstance from the database based on the primary key

        :param session: DB session.
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        :return: the task instance constructed
        """
        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self._dag_id,
            TaskInstance.task_id == self._task_id,
            TaskInstance.execution_date == self._execution_date,
        )

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        return ti


STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover

    from airflow.job.base_job import BaseJob
    from airflow.models.dagrun import DagRun

    TaskInstance.dag_run = relationship(DagRun)
    TaskInstance.queued_by_job = relationship(BaseJob)
