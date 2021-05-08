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

import hashlib
import importlib
import importlib.machinery
import importlib.util
import os
import sys
import textwrap
import traceback
import warnings
import zipfile
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Union

from croniter import CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError, croniter
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from tabulate import tabulate

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowClusterPolicyViolation,
    AirflowDagCycleException,
    AirflowDagDuplicatedIdException,
    SerializedDagNotFound,
)
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.dag_cycle_tester import test_cycle
from airflow.utils.file import correct_maybe_zipped, list_py_file_paths, might_contain_dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import MAX_DB_RETRIES, run_with_db_retries
from airflow.utils.session import provide_session
from airflow.utils.timeout import timeout

if TYPE_CHECKING:
    import pathlib


class FileLoadStat(NamedTuple):
    """Information about single file"""

    file: str
    duration: timedelta
    dag_num: int
    task_num: int
    dags: str


class DagBag(LoggingMixin):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :type dag_folder: unicode
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :type include_examples: bool
    :param include_smart_sensor: whether to include the smart sensor native
        DAGs that create the smart sensor operators for whole cluster
    :type include_smart_sensor: bool
    :param read_dags_from_db: Read DAGs from DB if ``True`` is passed.
        If ``False`` DAGs are read from python files.
    :type read_dags_from_db: bool
    :param load_op_links: Should the extra operator link be loaded via plugins when
        de-serializing the DAG? This flag is set to False in Scheduler so that Extra Operator links
        are not loaded to not run User code in Scheduler.
    :type load_op_links: bool
    """

    DAGBAG_IMPORT_TIMEOUT = conf.getfloat('core', 'DAGBAG_IMPORT_TIMEOUT')
    SCHEDULER_ZOMBIE_TASK_THRESHOLD = conf.getint('scheduler', 'scheduler_zombie_task_threshold')

    def __init__(
        self,
        dag_folder: Union[str, "pathlib.Path", None] = None,
        include_examples: bool = conf.getboolean('core', 'LOAD_EXAMPLES'),
        include_smart_sensor: bool = conf.getboolean('smart_sensor', 'USE_SMART_SENSOR'),
        safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'),
        read_dags_from_db: bool = False,
        store_serialized_dags: Optional[bool] = None,
        load_op_links: bool = True,
    ):
        # Avoid circular import
        from airflow.models.dag import DAG

        super().__init__()

        if store_serialized_dags:
            warnings.warn(
                "The store_serialized_dags parameter has been deprecated. "
                "You should pass the read_dags_from_db parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            read_dags_from_db = store_serialized_dags

        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.dag_folder = dag_folder
        self.dags: Dict[str, DAG] = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed: Dict[str, datetime] = {}
        self.import_errors: Dict[str, str] = {}
        self.has_logged = False
        self.read_dags_from_db = read_dags_from_db
        # Only used by read_dags_from_db=True
        self.dags_last_fetched: Dict[str, datetime] = {}
        # Only used by SchedulerJob to compare the dag_hash to identify change in DAGs
        self.dags_hash: Dict[str, str] = {}

        self.dagbag_import_error_tracebacks = conf.getboolean('core', 'dagbag_import_error_tracebacks')
        self.dagbag_import_error_traceback_depth = conf.getint('core', 'dagbag_import_error_traceback_depth')
        self.collect_dags(
            dag_folder=dag_folder,
            include_examples=include_examples,
            include_smart_sensor=include_smart_sensor,
            safe_mode=safe_mode,
        )
        # Should the extra operator link be loaded via plugins?
        # This flag is set to False in Scheduler so that Extra Operator links are not loaded
        self.load_op_links = load_op_links

    def size(self) -> int:
        """:return: the amount of dags contained in this dagbag"""
        return len(self.dags)

    @property
    def store_serialized_dags(self) -> bool:
        """Whether or not to read dags from DB"""
        warnings.warn(
            "The store_serialized_dags property has been deprecated. Use read_dags_from_db instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.read_dags_from_db

    @property
    def dag_ids(self) -> List[str]:
        """
        :return: a list of DAG IDs in this bag
        :rtype: List[unicode]
        """
        return list(self.dags.keys())

    @provide_session
    def get_dag(self, dag_id, session: Session = None):
        """
        Gets the DAG out of the dictionary, and refreshes it if expired

        :param dag_id: DAG Id
        :type dag_id: str
        """
        # Avoid circular import
        from airflow.models.dag import DagModel

        if self.read_dags_from_db:
            # Import here so that serialized dag is only imported when serialization is enabled
            from airflow.models.serialized_dag import SerializedDagModel

            if dag_id not in self.dags:
                # Load from DB if not (yet) in the bag
                self._add_dag_from_db(dag_id=dag_id, session=session)
                return self.dags.get(dag_id)

            # If DAG is in the DagBag, check the following
            # 1. if time has come to check if DAG is updated (controlled by min_serialized_dag_fetch_secs)
            # 2. check the last_updated column in SerializedDag table to see if Serialized DAG is updated
            # 3. if (2) is yes, fetch the Serialized DAG.
            min_serialized_dag_fetch_secs = timedelta(seconds=settings.MIN_SERIALIZED_DAG_FETCH_INTERVAL)
            if (
                dag_id in self.dags_last_fetched
                and timezone.utcnow() > self.dags_last_fetched[dag_id] + min_serialized_dag_fetch_secs
            ):
                sd_last_updated_datetime = SerializedDagModel.get_last_updated_datetime(
                    dag_id=dag_id,
                    session=session,
                )
                if sd_last_updated_datetime and sd_last_updated_datetime > self.dags_last_fetched[dag_id]:
                    self._add_dag_from_db(dag_id=dag_id, session=session)

            return self.dags.get(dag_id)

        # If asking for a known subdag, we want to refresh the parent
        dag = None
        root_dag_id = dag_id
        if dag_id in self.dags:
            dag = self.dags[dag_id]
            if dag.is_subdag:
                root_dag_id = dag.parent_dag.dag_id  # type: ignore

        # If DAG Model is absent, we can't check last_expired property. Is the DAG not yet synchronized?
        orm_dag = DagModel.get_current(root_dag_id, session=session)
        if not orm_dag:
            return self.dags.get(dag_id)

        # If the dag corresponding to root_dag_id is absent or expired
        is_missing = root_dag_id not in self.dags
        is_expired = orm_dag.last_expired and dag and dag.last_loaded < orm_dag.last_expired
        if is_expired:
            # Remove associated dags so we can re-add them.
            self.dags = {
                key: dag
                for key, dag in self.dags.items()
                if root_dag_id != key and not (dag.is_subdag and root_dag_id == dag.parent_dag.dag_id)
            }
        if is_missing or is_expired:
            # Reprocess source file.
            found_dags = self.process_file(
                filepath=correct_maybe_zipped(orm_dag.fileloc), only_if_updated=False
            )

            # If the source file no longer exports `dag_id`, delete it from self.dags
            if found_dags and dag_id in [found_dag.dag_id for found_dag in found_dags]:
                return self.dags[dag_id]
            elif dag_id in self.dags:
                del self.dags[dag_id]
        return self.dags.get(dag_id)

    def _add_dag_from_db(self, dag_id: str, session: Session):
        """Add DAG to DagBag from DB"""
        from airflow.models.serialized_dag import SerializedDagModel

        row = SerializedDagModel.get(dag_id, session)
        if not row:
            raise SerializedDagNotFound(f"DAG '{dag_id}' not found in serialized_dag table")

        row.load_op_links = self.load_op_links
        dag = row.dag
        for subdag in dag.subdags:
            self.dags[subdag.dag_id] = subdag
        self.dags[dag.dag_id] = dag
        self.dags_last_fetched[dag.dag_id] = timezone.utcnow()
        self.dags_hash[dag.dag_id] = row.dag_hash

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return []

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(filepath))
            if (
                only_if_updated
                and filepath in self.file_last_changed
                and file_last_changed_on_disk == self.file_last_changed[filepath]
            ):
                return []
        except Exception as e:  # pylint: disable=broad-except
            self.log.exception(e)
            return []

        if not zipfile.is_zipfile(filepath):
            mods = self._load_modules_from_file(filepath, safe_mode)
        else:
            mods = self._load_modules_from_zip(filepath, safe_mode)

        found_dags = self._process_modules(filepath, mods, file_last_changed_on_disk)

        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_dags

    def _load_modules_from_file(self, filepath, safe_mode):
        if not might_contain_dag(filepath, safe_mode):
            # Don't want to spam user with skip messages
            if not self.has_logged:
                self.has_logged = True
                self.log.info("File %s assumed to contain no DAGs. Skipping.", filepath)
            return []

        self.log.debug("Importing %s", filepath)
        org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
        path_hash = hashlib.sha1(filepath.encode('utf-8')).hexdigest()
        mod_name = f'unusual_prefix_{path_hash}_{org_mod_name}'

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        timeout_msg = f"DagBag import timeout for {filepath} after {self.DAGBAG_IMPORT_TIMEOUT}s"
        with timeout(self.DAGBAG_IMPORT_TIMEOUT, error_message=timeout_msg):
            try:
                loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
                spec = importlib.util.spec_from_loader(mod_name, loader)
                new_module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = new_module
                loader.exec_module(new_module)
                return [new_module]
            except Exception as e:  # pylint: disable=broad-except
                self.log.exception("Failed to import: %s", filepath)
                if self.dagbag_import_error_tracebacks:
                    self.import_errors[filepath] = traceback.format_exc(
                        limit=-self.dagbag_import_error_traceback_depth
                    )
                else:
                    self.import_errors[filepath] = str(e)
        return []

    def _load_modules_from_zip(self, filepath, safe_mode):
        mods = []
        with zipfile.ZipFile(filepath) as current_zip_file:
            for zip_info in current_zip_file.infolist():
                head, _ = os.path.split(zip_info.filename)
                mod_name, ext = os.path.splitext(zip_info.filename)
                if ext not in [".py", ".pyc"]:
                    continue
                if head:
                    continue

                if mod_name == '__init__':
                    self.log.warning("Found __init__.%s at root of %s", ext, filepath)

                self.log.debug("Reading %s from %s", zip_info.filename, filepath)

                if not might_contain_dag(zip_info.filename, safe_mode, current_zip_file):
                    # todo: create ignore list
                    # Don't want to spam user with skip messages
                    if not self.has_logged:
                        self.has_logged = True
                        self.log.info(
                            "File %s:%s assumed to contain no DAGs. Skipping.", filepath, zip_info.filename
                        )
                    continue

                if mod_name in sys.modules:
                    del sys.modules[mod_name]

                try:
                    sys.path.insert(0, filepath)
                    current_module = importlib.import_module(mod_name)
                    mods.append(current_module)
                except Exception as e:  # pylint: disable=broad-except
                    self.log.exception("Failed to import: %s", filepath)
                    if self.dagbag_import_error_tracebacks:
                        self.import_errors[filepath] = traceback.format_exc(
                            limit=-self.dagbag_import_error_traceback_depth
                        )
                    else:
                        self.import_errors[filepath] = str(e)
        return mods

    def _process_modules(self, filepath, mods, file_last_changed_on_disk):
        from airflow.models.dag import DAG  # Avoid circular import

        is_zipfile = zipfile.is_zipfile(filepath)
        top_level_dags = [o for m in mods for o in list(m.__dict__.values()) if isinstance(o, DAG)]

        found_dags = []

        for dag in top_level_dags:
            if not dag.full_filepath:
                dag.full_filepath = filepath
                if dag.fileloc != filepath and not is_zipfile:
                    dag.fileloc = filepath
            try:
                dag.is_subdag = False
                if isinstance(dag.normalized_schedule_interval, str):
                    croniter(dag.normalized_schedule_interval)
                self.bag_dag(dag=dag, root_dag=dag)
                found_dags.append(dag)
                found_dags += dag.subdags
            except (CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError) as cron_e:
                self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                self.import_errors[dag.full_filepath] = f"Invalid Cron expression: {cron_e}"
                self.file_last_changed[dag.full_filepath] = file_last_changed_on_disk
            except (
                AirflowDagCycleException,
                AirflowDagDuplicatedIdException,
                AirflowClusterPolicyViolation,
            ) as exception:
                self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                self.import_errors[dag.full_filepath] = str(exception)
                self.file_last_changed[dag.full_filepath] = file_last_changed_on_disk
        return found_dags

    def bag_dag(self, dag, root_dag):
        """
        Adds the DAG into the bag, recurses into sub dags.

        :raises: AirflowDagCycleException if a cycle is detected in this dag or its subdags.
        :raises: AirflowDagDuplicatedIdException if this dag or its subdags already exists in the bag.
        """
        self._bag_dag(dag=dag, root_dag=root_dag, recursive=True)

    def _bag_dag(self, *, dag, root_dag, recursive):
        """Actual implementation of bagging a dag.

        The only purpose of this is to avoid exposing ``recursive`` in ``bag_dag()``,
        intended to only be used by the ``_bag_dag()`` implementation.
        """
        test_cycle(dag)  # throws if a task cycle is found

        dag.resolve_template_files()
        dag.last_loaded = timezone.utcnow()

        # Check policies
        settings.dag_policy(dag)

        for task in dag.tasks:
            settings.task_policy(task)

        subdags = dag.subdags

        try:
            # DAG.subdags automatically performs DFS search, so we don't recurse
            # into further _bag_dag() calls.
            if recursive:
                for subdag in subdags:
                    subdag.full_filepath = dag.full_filepath
                    subdag.parent_dag = dag
                    subdag.is_subdag = True
                    self._bag_dag(dag=subdag, root_dag=root_dag, recursive=False)

            prev_dag = self.dags.get(dag.dag_id)
            if prev_dag and prev_dag.full_filepath != dag.full_filepath:
                raise AirflowDagDuplicatedIdException(
                    dag_id=dag.dag_id,
                    incoming=dag.full_filepath,
                    existing=self.dags[dag.dag_id].full_filepath,
                )
            self.dags[dag.dag_id] = dag
            self.log.debug('Loaded DAG %s', dag)
        except (AirflowDagCycleException, AirflowDagDuplicatedIdException):
            # There was an error in bagging the dag. Remove it from the list of dags
            self.log.exception('Exception bagging dag: %s', dag.dag_id)
            # Only necessary at the root level since DAG.subdags automatically
            # performs DFS to search through all subdags
            if recursive:
                for subdag in subdags:
                    if subdag.dag_id in self.dags:
                        del self.dags[subdag.dag_id]
            raise

    def collect_dags(
        self,
        dag_folder: Union[str, "pathlib.Path", None] = None,
        only_if_updated: bool = True,
        include_examples: bool = conf.getboolean('core', 'LOAD_EXAMPLES'),
        include_smart_sensor: bool = conf.getboolean('smart_sensor', 'USE_SMART_SENSOR'),
        safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'),
    ):
        """
        Given a file path or a folder, this method looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a ``.airflowignore`` file is found while processing
        the directory, it will behave much like a ``.gitignore``,
        ignoring files that match any of the regex patterns specified
        in the file.

        **Note**: The patterns in .airflowignore are treated as
        un-anchored regexes, not shell-like glob patterns.
        """
        if self.read_dags_from_db:
            return

        self.log.info("Filling up the DagBag from %s", dag_folder)
        dag_folder = dag_folder or self.dag_folder
        # Used to store stats around DagBag processing
        stats = []

        # Ensure dag_folder is a str -- it may have been a pathlib.Path
        dag_folder = correct_maybe_zipped(str(dag_folder))
        for filepath in list_py_file_paths(
            dag_folder,
            safe_mode=safe_mode,
            include_examples=include_examples,
            include_smart_sensor=include_smart_sensor,
        ):
            try:
                file_parse_start_dttm = timezone.utcnow()
                found_dags = self.process_file(filepath, only_if_updated=only_if_updated, safe_mode=safe_mode)

                file_parse_end_dttm = timezone.utcnow()
                stats.append(
                    FileLoadStat(
                        file=filepath.replace(settings.DAGS_FOLDER, ''),
                        duration=file_parse_end_dttm - file_parse_start_dttm,
                        dag_num=len(found_dags),
                        task_num=sum([len(dag.tasks) for dag in found_dags]),
                        dags=str([dag.dag_id for dag in found_dags]),
                    )
                )
            except Exception as e:  # pylint: disable=broad-except
                self.log.exception(e)

        self.dagbag_stats = sorted(stats, key=lambda x: x.duration, reverse=True)

    def collect_dags_from_db(self):
        """Collects DAGs from database."""
        from airflow.models.serialized_dag import SerializedDagModel

        with Stats.timer('collect_db_dags'):
            self.log.info("Filling up the DagBag from database")

            # The dagbag contains all rows in serialized_dag table. Deleted DAGs are deleted
            # from the table by the scheduler job.
            self.dags = SerializedDagModel.read_all_dags()

            # Adds subdags.
            # DAG post-processing steps such as self.bag_dag and croniter are not needed as
            # they are done by scheduler before serialization.
            subdags = {}
            for dag in self.dags.values():
                for subdag in dag.subdags:
                    subdags[subdag.dag_id] = subdag
            self.dags.update(subdags)

    def dagbag_report(self):
        """Prints a report around DagBag loading stats"""
        stats = self.dagbag_stats
        dag_folder = self.dag_folder
        duration = sum([o.duration for o in stats], timedelta()).total_seconds()
        dag_num = sum([o.dag_num for o in stats])
        task_num = sum([o.task_num for o in stats])
        table = tabulate(stats, headers="keys")

        report = textwrap.dedent(
            f"""\n
        -------------------------------------------------------------------
        DagBag loading stats for {dag_folder}
        -------------------------------------------------------------------
        Number of DAGs: {dag_num}
        Total task number: {task_num}
        DagBag parsing time: {duration}
        {table}
        """
        )
        return report

    @provide_session
    def sync_to_db(self, session: Optional[Session] = None):
        """Save attributes about list of DAG to the DB."""
        # To avoid circular import - airflow.models.dagbag -> airflow.models.dag -> airflow.models.dagbag
        from airflow.models.dag import DAG
        from airflow.models.serialized_dag import SerializedDagModel

        def _serialize_dag_capturing_errors(dag, session):
            """
            Try to serialize the dag to the DB, but make a note of any errors.

            We can't place them directly in import_errors, as this may be retried, and work the next time
            """
            if dag.is_subdag:
                return []
            try:
                # We can't use bulk_write_to_db as we want to capture each error individually
                dag_was_updated = SerializedDagModel.write_dag(
                    dag,
                    min_update_interval=settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
                    session=session,
                )
                if dag_was_updated:
                    self._sync_perm_for_dag(dag, session=session)
                return []
            except OperationalError:
                raise
            except Exception:  # pylint: disable=broad-except
                return [(dag.fileloc, traceback.format_exc(limit=-self.dagbag_import_error_traceback_depth))]

        # Retry 'DAG.bulk_write_to_db' & 'SerializedDagModel.bulk_sync_to_db' in case
        # of any Operational Errors
        # In case of failures, provide_session handles rollback
        for attempt in run_with_db_retries(logger=self.log):
            with attempt:
                serialize_errors = []
                self.log.debug(
                    "Running dagbag.sync_to_db with retries. Try %d of %d",
                    attempt.retry_state.attempt_number,
                    MAX_DB_RETRIES,
                )
                self.log.debug("Calling the DAG.bulk_sync_to_db method")
                try:
                    # Write Serialized DAGs to DB, capturing errors
                    for dag in self.dags.values():
                        serialize_errors.extend(_serialize_dag_capturing_errors(dag, session))

                    DAG.bulk_write_to_db(self.dags.values(), session=session)
                except OperationalError:
                    session.rollback()
                    raise
                # Only now we are "complete" do we update import_errors - don't want to record errors from
                # previous failed attempts
                self.import_errors.update(dict(serialize_errors))

    @provide_session
    def _sync_perm_for_dag(self, dag, session: Optional[Session] = None):
        """Sync DAG specific permissions, if necessary"""
        from flask_appbuilder.security.sqla import models as sqla_models

        from airflow.security.permissions import DAG_PERMS, resource_name_for_dag

        def needs_perm_views(dag_id: str) -> bool:
            dag_resource_name = resource_name_for_dag(dag_id)
            for permission_name in DAG_PERMS:
                if not (
                    session.query(sqla_models.PermissionView)
                    .join(sqla_models.Permission)
                    .join(sqla_models.ViewMenu)
                    .filter(sqla_models.Permission.name == permission_name)
                    .filter(sqla_models.ViewMenu.name == dag_resource_name)
                    .one_or_none()
                ):
                    return True
            return False

        if dag.access_control or needs_perm_views(dag.dag_id):
            self.log.debug("Syncing DAG permissions: %s to the DB", dag.dag_id)
            from airflow.www.security import ApplessAirflowSecurityManager

            security_manager = ApplessAirflowSecurityManager(session=session)
            security_manager.sync_perm_for_dag(dag.dag_id, dag.access_control)
