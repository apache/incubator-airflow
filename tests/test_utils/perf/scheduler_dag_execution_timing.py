#!/usr/bin/env python3
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
import gc
import os
import statistics
import sys
import textwrap
import time
from argparse import Namespace
from operator import attrgetter

import click

MAX_DAG_RUNS_ALLOWED = 1


class ShortCircuitExecutorMixin:
    """
    Mixin class to manage the scheduler state during the performance test run.
    """

    def __init__(self, dag_ids_to_watch, num_runs):
        super().__init__()
        self.num_runs_per_dag = num_runs
        self.reset(dag_ids_to_watch)

    def reset(self, dag_ids_to_watch):
        """
        Capture the value that will determine when the scheduler is reset.
        """
        self.dags_to_watch = {
            dag_id: Namespace(
                waiting_for=self.num_runs_per_dag,
                # A "cache" of DagRun row, so we don't have to look it up each
                # time. This is to try and reduce the impact of our
                # benchmarking code on runtime,
                runs={},
            )
            for dag_id in dag_ids_to_watch
        }

    def change_state(self, key, state, info=None):
        """
        Change the state of scheduler by waiting till the tasks is complete
        and then shut down the scheduler after the task is complete
        """
        from airflow.utils.state import State

        super().change_state(key, state, info=info)

        dag_id, _, execution_date, __ = key
        if dag_id not in self.dags_to_watch:
            return

        # This fn is called before the DagRun state is updated, so we can't
        # check the DR.state - so instead we need to check the state of the
        # tasks in that run

        run = self.dags_to_watch[dag_id].runs.get(execution_date)
        if not run:
            import airflow.models

            # odd `list()` is to work across Airflow versions.
            run = list(airflow.models.DagRun.find(dag_id=dag_id, execution_date=execution_date))[0]
            self.dags_to_watch[dag_id].runs[execution_date] = run

        if run and all(t.state == State.SUCCESS for t in run.get_task_instances()):
            self.dags_to_watch[dag_id].runs.pop(execution_date)
            self.dags_to_watch[dag_id].waiting_for -= 1

            if self.dags_to_watch[dag_id].waiting_for == 0:
                self.dags_to_watch.pop(dag_id)

            if not self.dags_to_watch:
                self.log.warning("STOPPING SCHEDULER -- all runs complete")
                self.scheduler_job.processor_agent._done = True  # pylint: disable=protected-access
                return
        self.log.warning(
            "WAITING ON %d RUNS", sum(map(attrgetter('waiting_for'), self.dags_to_watch.values()))
        )


def get_executor_under_test(dotted_path):
    """
    Create and return a MockExecutor
    """

    from airflow.executors.executor_loader import ExecutorLoader

    if dotted_path == "MockExecutor":
        from tests.test_utils.mock_executor import MockExecutor as executor

    else:
        executor = ExecutorLoader.load_executor(dotted_path)
        executor_cls = type(executor)

    # Change this to try other executors
    class ShortCircuitExecutor(ShortCircuitExecutorMixin, executor_cls):
        """
        Placeholder class that implements the inheritance hierarchy
        """

        scheduler_job = None

    return ShortCircuitExecutor


def reset_dag(dag, session):
    """
    Delete all dag and task instances and then un_pause the Dag.
    """
    import airflow.models

    DR = airflow.models.DagRun
    DM = airflow.models.DagModel
    TI = airflow.models.TaskInstance
    TF = airflow.models.TaskFail
    dag_id = dag.dag_id

    session.query(DM).filter(DM.dag_id == dag_id).update({'is_paused': False})
    session.query(DR).filter(DR.dag_id == dag_id).delete()
    session.query(TI).filter(TI.dag_id == dag_id).delete()
    session.query(TF).filter(TF.dag_id == dag_id).delete()


def pause_all_dags(session):
    """
    Pause all Dags
    """
    from airflow.models.dag import DagModel

    session.query(DagModel).update({'is_paused': True})


def create_dag_runs(dag, num_runs, session):
    """
    Create  `num_runs` of dag runs for sub-sequent schedules
    """
    from airflow.utils import timezone
    from airflow.utils.state import State

    try:
        from airflow.utils.types import DagRunType

        id_prefix = f'{DagRunType.SCHEDULED.value}__'
    except ImportError:
        from airflow.models.dagrun import DagRun

        id_prefix = DagRun.ID_PREFIX  # pylint: disable=no-member

    next_run_date = dag.normalize_schedule(dag.start_date or min(t.start_date for t in dag.tasks))

    for _ in range(num_runs):
        dag.create_dagrun(
            run_id=id_prefix + next_run_date.isoformat(),
            execution_date=next_run_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session,
        )
        next_run_date = dag.following_schedule(next_run_date)


@click.command()
@click.option('--num-runs', default=1, help='number of DagRun, to run for each DAG')
@click.option('--repeat', default=3, help='number of times to run test, to reduce variance')
@click.option(
    '--pre-create-dag-runs',
    is_flag=True,
    default=False,
    help='''Pre-create the dag runs and stop the scheduler creating more.

        Warning: this makes the scheduler do (slightly) less work so may skew your numbers. Use sparingly!
        ''',
)
@click.option(
    '--executor-class',
    default='MockExecutor',
    help=textwrap.dedent(
        '''
          Dotted path Executor class to test, for example
          'airflow.executors.local_executor.LocalExecutor'. Defaults to MockExecutor which doesn't run tasks.
      '''
    ),  # pylint: disable=too-many-locals
)
@click.argument('dag_ids', required=True, nargs=-1)
def main(num_runs, repeat, pre_create_dag_runs, executor_class, dag_ids):
    """
    This script can be used to measure the total "scheduler overhead" of Airflow.

    By overhead we mean if the tasks executed instantly as soon as they are
    executed (i.e. they do nothing) how quickly could we schedule them.

    It will monitor the task completion of the Mock/stub executor (no actual
    tasks are run) and after the required number of dag runs for all the
    specified dags have completed all their tasks, it will cleanly shut down
    the scheduler.

    The dags you run with need to have an early enough start_date to create the
    desired number of runs.

    Care should be taken that other limits (DAG concurrency, pool size etc) are
    not the bottleneck. This script doesn't help you in that regard.

    It is recommended to repeat the test at least 3 times (`--repeat=3`, the
    default) so that you can get somewhat-accurate variance on the reported
    timing numbers, but this can be disabled for longer runs if needed.
    """

    # Turn on unit test mode so that we don't do any sleep() in the scheduler
    # loop - not needed on master, but this script can run against older
    # releases too!
    os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'

    os.environ['AIRFLOW__CORE__DAG_CONCURRENCY'] = '500'

    # Set this so that dags can dynamically configure their end_date
    os.environ['AIRFLOW_BENCHMARK_MAX_DAG_RUNS'] = str(num_runs)
    os.environ['PERF_MAX_RUNS'] = str(num_runs)

    if pre_create_dag_runs:
        os.environ['AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE'] = 'False'

    from airflow.jobs.scheduler_job import SchedulerJob
    from airflow.models.dagbag import DagBag
    from airflow.utils import db

    dagbag = DagBag()

    dags = []

    with db.create_session() as session:
        pause_all_dags(session)
        for dag_id in dag_ids:
            dag = dagbag.get_dag(dag_id)
            dag.sync_to_db(session=session)
            dags.append(dag)
            reset_dag(dag, session)

            next_run_date = dag.normalize_schedule(dag.start_date or min(t.start_date for t in dag.tasks))

            for _ in range(num_runs - 1):
                next_run_date = dag.following_schedule(next_run_date)

            end_date = dag.end_date or dag.default_args.get('end_date')
            if end_date != next_run_date:
                message = (
                    f"DAG {dag_id} has incorrect end_date ({end_date}) for number of runs! "
                    f"It should be "
                    f" {next_run_date}"
                )
                sys.exit(message)

            if pre_create_dag_runs:
                create_dag_runs(dag, num_runs, session)

    ShortCircuitExecutor = get_executor_under_test(executor_class)

    executor = ShortCircuitExecutor(dag_ids_to_watch=dag_ids, num_runs=num_runs)
    scheduler_job = SchedulerJob(dag_ids=dag_ids, do_pickle=False, executor=executor)
    executor.scheduler_job = scheduler_job

    total_tasks = sum(len(dag.tasks) for dag in dags)

    if 'PYSPY' in os.environ:
        pid = str(os.getpid())
        filename = os.environ.get('PYSPY_O', 'flame-' + pid + '.html')
        os.spawnlp(os.P_NOWAIT, 'sudo', 'sudo', 'py-spy', 'record', '-o', filename, '-p', pid, '--idle')

    times = []

    # Need a lambda to refer to the _latest_ value fo scheduler_job, not just
    # the initial one
    code_to_test = lambda: scheduler_job.run()  # pylint: disable=unnecessary-lambda

    for count in range(repeat):
        gc.disable()
        start = time.perf_counter()

        code_to_test()
        times.append(time.perf_counter() - start)
        gc.enable()
        print("Run %d time: %.5f" % (count + 1, times[-1]))

        if count + 1 != repeat:
            with db.create_session() as session:
                for dag in dags:
                    reset_dag(dag, session)

            executor.reset(dag_ids)
            scheduler_job = SchedulerJob(dag_ids=dag_ids, do_pickle=False, executor=executor)
            executor.scheduler_job = scheduler_job

    print()
    print()
    msg = "Time for %d dag runs of %d dags with %d total tasks: %.4fs"

    if len(times) > 1:
        print(
            (msg + " (±%.3fs)")
            % (num_runs, len(dags), total_tasks, statistics.mean(times), statistics.stdev(times))
        )
    else:
        print(msg % (num_runs, len(dags), total_tasks, times[0]))

    print()
    print()


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
