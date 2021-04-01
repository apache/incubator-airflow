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
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable, List, NamedTuple, Optional, Tuple, Union

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    PickleType,
    String,
    UniqueConstraint,
    and_,
    func,
    or_,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import backref, relationship, synonym
from sqlalchemy.orm.session import Session

from airflow import settings
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException, TaskNotFound
from airflow.models.base import ID_LEN, Base
from airflow.models.taskinstance import TaskInstance as TI
from airflow.settings import task_instance_mutation_hook
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_states import SCHEDULEABLE_STATES
from airflow.utils import callback_requests, timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime, nulls_first, skip_locked, with_row_locks
from airflow.utils.state import State
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from airflow.models.dag import DAG


class TISchedulingDecision(NamedTuple):
    """Type of return for DagRun.task_instance_scheduling_decisions"""

    tis: List[TI]
    schedulable_tis: List[TI]
    changed_tis: bool
    unfinished_tasks: List[TI]
    finished_tasks: List[TI]


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """

    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime, default=timezone.utcnow)
    start_date = Column(UtcDateTime, default=timezone.utcnow)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    creating_job_id = Column(Integer)
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String(50), nullable=False)
    conf = Column(PickleType)
    # When a scheduler last attempted to schedule TIs for this DagRun
    last_scheduling_decision = Column(UtcDateTime)
    dag_hash = Column(String(32))

    dag = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
        Index('idx_last_scheduling_decision', last_scheduling_decision),
    )

    task_instances = relationship(
        TI,
        primaryjoin=and_(TI.dag_id == dag_id, TI.execution_date == execution_date),  # type: ignore
        foreign_keys=(dag_id, execution_date),
        backref=backref('dag_run', uselist=False),
    )

    DEFAULT_DAGRUNS_TO_EXAMINE = airflow_conf.getint(
        'scheduler',
        'max_dagruns_per_loop_to_schedule',
        fallback=20,
    )

    def __init__(
        self,
        dag_id: Optional[str] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
        external_trigger: Optional[bool] = None,
        conf: Optional[Any] = None,
        state: Optional[str] = None,
        run_type: Optional[str] = None,
        dag_hash: Optional[str] = None,
        creating_job_id: Optional[int] = None,
    ):
        self.dag_id = dag_id
        self.run_id = run_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.external_trigger = external_trigger
        self.conf = conf or {}
        self.state = state
        self.run_type = run_type
        self.dag_hash = dag_hash
        self.creating_job_id = creating_job_id
        super().__init__()

    def __repr__(self):
        return (
            '<DagRun {dag_id} @ {execution_date}: {run_id}, externally triggered: {external_trigger}>'
        ).format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            external_trigger=self.external_trigger,
        )

    def get_state(self):
        return self._state

    def set_state(self, state):
        if self._state != state:
            self._state = state
            self.end_date = timezone.utcnow() if self._state in State.finished else None

    @declared_attr
    def state(self):
        return synonym('_state', descriptor=property(self.get_state, self.set_state))

    @provide_session
    def refresh_from_db(self, session: Session = None):
        """
        Reloads the current dagrun from the database

        :param session: database session
        :type session: Session
        """
        DR = DagRun

        exec_date = func.cast(self.execution_date, DateTime)

        dr = (
            session.query(DR)
            .filter(
                DR.dag_id == self.dag_id,
                func.cast(DR.execution_date, DateTime) == exec_date,
                DR.run_id == self.run_id,
            )
            .one()
        )

        self.id = dr.id
        self.state = dr.state

    @classmethod
    def next_dagruns_to_examine(
        cls,
        session: Session,
        max_number: Optional[int] = None,
    ):
        """
        Return the next DagRuns that the scheduler should attempt to schedule.

        This will return zero or more DagRun rows that are row-level-locked with a "SELECT ... FOR UPDATE"
        query, you should ensure that any scheduling decisions are made in a single transaction -- as soon as
        the transaction is committed it will be unlocked.

        :rtype: list[airflow.models.DagRun]
        """
        from airflow.models.dag import DagModel

        if max_number is None:
            max_number = cls.DEFAULT_DAGRUNS_TO_EXAMINE

        # TODO: Bake this query, it is run _A lot_
        query = (
            session.query(cls)
            .filter(cls.state == State.RUNNING, cls.run_type != DagRunType.BACKFILL_JOB)
            .join(
                DagModel,
                DagModel.dag_id == cls.dag_id,
            )
            .filter(
                DagModel.is_paused.is_(False),
                DagModel.is_active.is_(True),
            )
            .order_by(
                nulls_first(cls.last_scheduling_decision, session=session),
                cls.execution_date,
            )
        )

        if not settings.ALLOW_FUTURE_EXEC_DATES:
            query = query.filter(DagRun.execution_date <= func.now())

        return with_row_locks(
            query.limit(max_number), of=cls, session=session, **skip_locked(session=session)
        )

    @staticmethod
    @provide_session
    def find(
        dag_id: Optional[Union[str, List[str]]] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        state: Optional[str] = None,
        external_trigger: Optional[bool] = None,
        no_backfills: bool = False,
        run_type: Optional[DagRunType] = None,
        session: Session = None,
        execution_start_date: Optional[datetime] = None,
        execution_end_date: Optional[datetime] = None,
    ) -> List["DagRun"]:
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id or list of dag_id to find dag runs for
        :type dag_id: str or list[str]
        :param run_id: defines the run id for this dag run
        :type run_id: str
        :param run_type: type of DagRun
        :type run_type: airflow.utils.types.DagRunType
        :param execution_date: the execution date
        :type execution_date: datetime.datetime or list[datetime.datetime]
        :param state: the state of the dag run
        :type state: str
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param execution_start_date: dag run that was executed from this date
        :type execution_start_date: datetime.datetime
        :param execution_end_date: dag run that was executed until this date
        :type execution_end_date: datetime.datetime
        """
        DR = DagRun

        qry = session.query(DR)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.filter(DR.dag_id.in_(dag_ids))
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.filter(DR.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.filter(DR.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.filter(DR.execution_date <= execution_end_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if run_type:
            qry = qry.filter(DR.run_type == run_type)
        if no_backfills:
            qry = qry.filter(DR.run_type != DagRunType.BACKFILL_JOB)

        return qry.order_by(DR.execution_date).all()

    @staticmethod
    def generate_run_id(run_type: DagRunType, execution_date: datetime) -> str:
        """Generate Run ID based on Run Type and Execution Date"""
        return f"{run_type}__{execution_date.isoformat()}"

    @provide_session
    def get_task_instances(self, state=None, session=None) -> Iterable[TI]:
        """Returns the task instances for this dag run"""
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
        )

        if state:
            if isinstance(state, str):
                tis = tis.filter(TI.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.filter(TI.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.filter(or_(TI.state.in_(not_none_state), TI.state.is_(None)))
                else:
                    tis = tis.filter(TI.state.in_(state))

        if self.dag and self.dag.partial:
            tis = tis.filter(TI.task_id.in_(self.dag.task_ids))
        return tis.all()

    @provide_session
    def get_task_instance(self, task_id: str, session: Session = None) -> Optional[TI]:
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        :type task_id: str
        :param session: Sqlalchemy ORM Session
        :type session: Session
        """
        return (
            session.query(TI)
            .filter(TI.dag_id == self.dag_id, TI.execution_date == self.execution_date, TI.task_id == task_id)
            .first()
        )

    def get_dag(self) -> "DAG":
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException(f"The DAG (.dag) for {self} needs to be set")

        return self.dag

    @provide_session
    def get_previous_dagrun(self, state: Optional[str] = None, session: Session = None) -> Optional['DagRun']:
        """The previous DagRun, if there is one"""
        filters = [
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date,
        ]
        if state is not None:
            filters.append(DagRun.state == state)
        return session.query(DagRun).filter(*filters).order_by(DagRun.execution_date.desc()).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session: Session = None) -> Optional['DagRun']:
        """The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date == dag.previous_schedule(self.execution_date),
            )
            .first()
        )

    @provide_session
    def update_state(
        self, session: Session = None, execute_callbacks: bool = True
    ) -> Tuple[List[TI], Optional[callback_requests.DagCallbackRequest]]:
        """
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :param session: Sqlalchemy ORM Session
        :type session: Session
        :param execute_callbacks: Should dag callbacks (success/failure, SLA etc) be invoked
            directly (default: true) or recorded as a pending request in the ``callback`` property
        :type execute_callbacks: bool
        :return: Tuple containing tis that can be scheduled in the current loop & `callback` that
            needs to be executed
        """
        # Callback to execute in case of Task Failures
        callback: Optional[callback_requests.DagCallbackRequest] = None

        start_dttm = timezone.utcnow()
        self.last_scheduling_decision = start_dttm
        with Stats.timer(f"dagrun.dependency-check.{self.dag_id}"):
            dag = self.get_dag()
            info = self.task_instance_scheduling_decisions(session)

            tis = info.tis
            schedulable_tis = info.schedulable_tis
            changed_tis = info.changed_tis
            finished_tasks = info.finished_tasks
            unfinished_tasks = info.unfinished_tasks

            none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tasks)
            none_task_concurrency = all(t.task.task_concurrency is None for t in unfinished_tasks)

            if unfinished_tasks and none_depends_on_past and none_task_concurrency:
                # small speed up
                are_runnable_tasks = (
                    schedulable_tis
                    or self._are_premature_tis(unfinished_tasks, finished_tasks, session)
                    or changed_tis
                )

        leaf_task_ids = {t.task_id for t in dag.leaves}
        leaf_tis = [ti for ti in tis if ti.task_id in leaf_task_ids]

        # if all roots finished and at least one failed, the run failed
        if not unfinished_tasks and any(leaf_ti.state in State.failed_states for leaf_ti in leaf_tis):
            self.log.error('Marking run %s failed', self)
            self.set_state(State.FAILED)
            if execute_callbacks:
                dag.handle_callback(self, success=False, reason='task_failure', session=session)
            elif dag.has_on_failure_callback:
                callback = callback_requests.DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    execution_date=self.execution_date,
                    is_failure_callback=True,
                    msg='task_failure',
                )

        # if all leaves succeeded and no unfinished tasks, the run succeeded
        elif not unfinished_tasks and all(leaf_ti.state in State.success_states for leaf_ti in leaf_tis):
            self.log.info('Marking run %s successful', self)
            self.set_state(State.SUCCESS)
            if execute_callbacks:
                dag.handle_callback(self, success=True, reason='success', session=session)
            elif dag.has_on_success_callback:
                callback = callback_requests.DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    execution_date=self.execution_date,
                    is_failure_callback=False,
                    msg='success',
                )

        # if *all tasks* are deadlocked, the run failed
        elif unfinished_tasks and none_depends_on_past and none_task_concurrency and not are_runnable_tasks:
            self.log.error('Deadlock; marking run %s failed', self)
            self.set_state(State.FAILED)
            if execute_callbacks:
                dag.handle_callback(self, success=False, reason='all_tasks_deadlocked', session=session)
            elif dag.has_on_failure_callback:
                callback = callback_requests.DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    execution_date=self.execution_date,
                    is_failure_callback=True,
                    msg='all_tasks_deadlocked',
                )

        # finally, if the roots aren't done, the dag is still running
        else:
            self.set_state(State.RUNNING)

        self._emit_true_scheduling_delay_stats_for_finished_state(finished_tasks)
        self._emit_duration_stats_for_finished_state()

        session.merge(self)

        return schedulable_tis, callback

    @provide_session
    def task_instance_scheduling_decisions(self, session: Session = None) -> TISchedulingDecision:

        schedulable_tis: List[TI] = []
        changed_tis = False

        tis = list(self.get_task_instances(session=session, state=State.task_states + (State.SHUTDOWN,)))
        self.log.debug("number of tis tasks for %s: %s task(s)", self, len(tis))
        for ti in tis:
            try:
                ti.task = self.get_dag().get_task(ti.task_id)
            except TaskNotFound:
                self.log.warning(
                    "Failed to get task '%s' for dag '%s'. Marking it as removed.", ti, ti.dag_id
                )
                ti.state = State.REMOVED
                session.flush()

        unfinished_tasks = [t for t in tis if t.state in State.unfinished]
        finished_tasks = [t for t in tis if t.state in State.finished]
        if unfinished_tasks:
            scheduleable_tasks = [ut for ut in unfinished_tasks if ut.state in SCHEDULEABLE_STATES]
            self.log.debug("number of scheduleable tasks for %s: %s task(s)", self, len(scheduleable_tasks))
            schedulable_tis, changed_tis = self._get_ready_tis(scheduleable_tasks, finished_tasks, session)

        return TISchedulingDecision(
            tis=tis,
            schedulable_tis=schedulable_tis,
            changed_tis=changed_tis,
            unfinished_tasks=unfinished_tasks,
            finished_tasks=finished_tasks,
        )

    def _get_ready_tis(
        self,
        scheduleable_tasks: List[TI],
        finished_tasks: List[TI],
        session: Session,
    ) -> Tuple[List[TI], bool]:
        old_states = {}
        ready_tis: List[TI] = []
        changed_tis = False

        if not scheduleable_tasks:
            return ready_tis, changed_tis

        # Check dependencies
        for st in scheduleable_tasks:
            old_state = st.state
            if st.are_dependencies_met(
                dep_context=DepContext(flag_upstream_failed=True, finished_tasks=finished_tasks),
                session=session,
            ):
                ready_tis.append(st)
            else:
                old_states[st.key] = old_state

        # Check if any ti changed state
        tis_filter = TI.filter_for_tis(old_states.keys())
        if tis_filter is not None:
            fresh_tis = session.query(TI).filter(tis_filter).all()
            changed_tis = any(ti.state != old_states[ti.key] for ti in fresh_tis)

        return ready_tis, changed_tis

    def _are_premature_tis(
        self,
        unfinished_tasks: List[TI],
        finished_tasks: List[TI],
        session: Session,
    ) -> bool:
        # there might be runnable tasks that are up for retry and for some reason(retry delay, etc) are
        # not ready yet so we set the flags to count them in
        for ut in unfinished_tasks:
            if ut.are_dependencies_met(
                dep_context=DepContext(
                    flag_upstream_failed=True,
                    ignore_in_retry_period=True,
                    ignore_in_reschedule_period=True,
                    finished_tasks=finished_tasks,
                ),
                session=session,
            ):
                return True
        return False

    def _emit_true_scheduling_delay_stats_for_finished_state(self, finished_tis):
        """
        This is a helper method to emit the true scheduling delay stats, which is defined as
        the time when the first task in DAG starts minus the expected DAG run datetime.
        This method will be used in the update_state method when the state of the DagRun
        is updated to a completed status (either success or failure). The method will find the first
        started task within the DAG and calculate the expected DagRun start time (based on
        dag.execution_date & dag.schedule_interval), and minus these two values to get the delay.
        The emitted data may contains outlier (e.g. when the first task was cleared, so
        the second task's start_date will be used), but we can get rid of the outliers
        on the stats side through the dashboards tooling built.
        Note, the stat will only be emitted if the DagRun is a scheduler triggered one
        (i.e. external_trigger is False).
        """
        if self.state == State.RUNNING:
            return
        if self.external_trigger:
            return
        if not finished_tis:
            return

        try:
            dag = self.get_dag()

            if not self.dag.schedule_interval or self.dag.schedule_interval == "@once":
                # We can't emit this metric if there is no following schedule to calculate from!
                return

            ordered_tis_by_start_date = [ti for ti in finished_tis if ti.start_date]
            ordered_tis_by_start_date.sort(key=lambda ti: ti.start_date, reverse=False)
            first_start_date = ordered_tis_by_start_date[0].start_date
            if first_start_date:
                # dag.following_schedule calculates the expected start datetime for a scheduled dagrun
                # i.e. a daily flow for execution date 1/1/20 actually runs on 1/2/20 hh:mm:ss,
                # and ti.start_date will be 1/2/20 hh:mm:ss so the following schedule is comparison
                true_delay = first_start_date - dag.following_schedule(self.execution_date)
                if true_delay.total_seconds() > 0:
                    Stats.timing(f'dagrun.{dag.dag_id}.first_task_scheduling_delay', true_delay)
        except Exception as e:
            self.log.warning(f'Failed to record first_task_scheduling_delay metric:\n{e}')

    def _emit_duration_stats_for_finished_state(self):
        if self.state == State.RUNNING:
            return
        if self.start_date is None:
            self.log.warning('Failed to record duration of %s: start_date is not set.', self)
            return
        if self.end_date is None:
            self.log.warning('Failed to record duration of %s: end_date is not set.', self)
            return

        duration = self.end_date - self.start_date
        if self.state == State.SUCCESS:
            Stats.timing(f'dagrun.duration.success.{self.dag_id}', duration)
        elif self.state == State.FAILED:
            Stats.timing(f'dagrun.duration.failed.{self.dag_id}', duration)

    @provide_session
    def verify_integrity(self, session: Session = None):
        """
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.

        :param session: Sqlalchemy ORM Session
        :type session: Session
        """
        dag = self.get_dag()
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = set()
        for ti in tis:
            task_instance_mutation_hook(ti)
            task_ids.add(ti.task_id)
            task = None
            try:
                task = dag.get_task(ti.task_id)
            except AirflowException:
                if ti.state == State.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state != State.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '%s' for dag '%s'. Marking it as removed.", ti, dag)
                    Stats.incr(f"task_removed_from_dag.{dag.dag_id}", 1, 1)
                    ti.state = State.REMOVED

            should_restore_task = (task is not None) and ti.state == State.REMOVED
            if should_restore_task:
                self.log.info("Restoring task '%s' which was previously removed from DAG '%s'", ti, dag)
                Stats.incr(f"task_restored_to_dag.{dag.dag_id}", 1, 1)
                ti.state = State.NONE
            session.merge(ti)

        # check for missing tasks
        for task in dag.task_dict.values():
            if task.start_date > self.execution_date and not self.is_backfill:
                continue

            if task.task_id not in task_ids:
                Stats.incr(f"task_instance_created-{task.task_type}", 1, 1)
                ti = TI(task, self.execution_date)
                task_instance_mutation_hook(ti)
                session.add(ti)

        try:
            session.flush()
        except IntegrityError as err:
            self.log.info(str(err))
            self.log.info(
                'Hit IntegrityError while creating the TIs for ' f'{dag.dag_id} - {self.execution_date}.'
            )
            self.log.info('Doing session rollback.')
            # TODO[HA]: We probably need to savepoint this so we can keep the transaction alive.
            session.rollback()

    @staticmethod
    def get_run(session: Session, dag_id: str, execution_date: datetime) -> Optional['DagRun']:
        """
        Get a single DAG Run

        :param session: Sqlalchemy ORM Session
        :type session: Session
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: DagRun corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.DagRun
        """
        return (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.external_trigger == False,  # noqa pylint: disable=singleton-comparison
                DagRun.execution_date == execution_date,
            )
            .first()
        )

    @property
    def is_backfill(self) -> bool:
        return self.run_type == DagRunType.BACKFILL_JOB

    @classmethod
    @provide_session
    def get_latest_runs(cls, session=None) -> List['DagRun']:
        """Returns the latest DagRun for each DAG"""
        subquery = (
            session.query(cls.dag_id, func.max(cls.execution_date).label('execution_date'))
            .group_by(cls.dag_id)
            .subquery()
        )
        return (
            session.query(cls)
            .join(
                subquery,
                and_(cls.dag_id == subquery.c.dag_id, cls.execution_date == subquery.c.execution_date),
            )
            .all()
        )

    @provide_session
    def schedule_tis(self, schedulable_tis: Iterable[TI], session: Session = None) -> int:
        """
        Set the given task instances in to the scheduled state.

        Each element of ``schedulable_tis`` should have it's ``task`` attribute already set.

        Any DummyOperator without callbacks is instead set straight to the success state.

        All the TIs should belong to this DagRun, but this code is in the hot-path, this is not checked -- it
        is the caller's responsibility to call this function only with TIs from a single dag run.
        """
        # Get list of TI IDs that do not need to executed, these are
        # tasks using DummyOperator and without on_execute_callback / on_success_callback
        dummy_ti_ids = []
        schedulable_ti_ids = []
        for ti in schedulable_tis:
            if (
                ti.task.inherits_from_dummy_operator
                and not ti.task.on_execute_callback
                and not ti.task.on_success_callback
            ):
                dummy_ti_ids.append(ti.task_id)
            else:
                schedulable_ti_ids.append(ti.task_id)

        count = 0

        if schedulable_ti_ids:
            count += (
                session.query(TI)
                .filter(
                    TI.dag_id == self.dag_id,
                    TI.execution_date == self.execution_date,
                    TI.task_id.in_(schedulable_ti_ids),
                )
                .update({TI.state: State.SCHEDULED}, synchronize_session=False)
            )

        # Tasks using DummyOperator should not be executed, mark them as success
        if dummy_ti_ids:
            count += (
                session.query(TI)
                .filter(
                    TI.dag_id == self.dag_id,
                    TI.execution_date == self.execution_date,
                    TI.task_id.in_(dummy_ti_ids),
                )
                .update(
                    {
                        TI.state: State.SUCCESS,
                        TI.start_date: timezone.utcnow(),
                        TI.end_date: timezone.utcnow(),
                        TI.duration: 0,
                    },
                    synchronize_session=False,
                )
            )

        return count
