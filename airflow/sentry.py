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

"""Sentry Integration"""
import logging
from functools import wraps

from airflow.configuration import conf
from airflow.utils.session import find_session_idx, provide_session
from airflow.utils.state import State

log = logging.getLogger(__name__)


class DummySentry:
    """Blank class for Sentry."""

    @classmethod
    def add_tagging(cls, task_instance):
        """Blank function for tagging."""

    @classmethod
    def add_breadcrumbs(cls, task_instance, session=None):
        """Blank function for breadcrumbs."""

    @classmethod
    def enrich_errors(cls, run):
        """Blank function for formatting a TaskInstance._run_raw_task."""
        return run

    def flush(self):
        """Blank function for flushing errors."""


Sentry: DummySentry = DummySentry()
if conf.getboolean("sentry", 'sentry_on', fallback=False):
    import sentry_sdk

    # Verify blinker installation
    from blinker import signal  # noqa: F401 pylint: disable=unused-import
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.logging import ignore_logger

    class ConfiguredSentry(DummySentry):
        """Configure Sentry SDK."""

        SCOPE_TAGS = frozenset(("task_id", "dag_id", "execution_date", "operator", "try_number"))
        SCOPE_CRUMBS = frozenset(("task_id", "state", "operator", "duration"))

        UNSUPPORTED_SENTRY_OPTIONS = frozenset(
            (
                "integrations",
                "in_app_include",
                "in_app_exclude",
                "ignore_errors",
                "before_breadcrumb",
                "before_send",
                "transport",
            )
        )

        def __init__(self):
            """Initialize the Sentry SDK."""
            ignore_logger("airflow.task")
            ignore_logger("airflow.jobs.backfill_job.BackfillJob")
            executor_name = conf.get("core", "EXECUTOR")

            sentry_flask = FlaskIntegration()

            # LoggingIntegration is set by default.
            integrations = [sentry_flask]

            if executor_name == "CeleryExecutor":
                from sentry_sdk.integrations.celery import CeleryIntegration

                sentry_celery = CeleryIntegration()
                integrations.append(sentry_celery)

            dsn = None
            sentry_config_opts = conf.getsection("sentry") or {}
            if sentry_config_opts:
                sentry_config_opts.pop("sentry_on")
                old_way_dsn = sentry_config_opts.pop("sentry_dsn", None)
                new_way_dsn = sentry_config_opts.pop("dsn", None)
                # supported backward compatibility with old way dsn option
                dsn = old_way_dsn or new_way_dsn

                unsupported_options = self.UNSUPPORTED_SENTRY_OPTIONS.intersection(sentry_config_opts.keys())
                if unsupported_options:
                    log.warning(
                        "There are unsupported options in [sentry] section: %s",
                        ", ".join(unsupported_options),
                    )

            if dsn:
                sentry_sdk.init(dsn=dsn, integrations=integrations, **sentry_config_opts)
            else:
                # Setting up Sentry using environment variables.
                log.debug("Defaulting to SENTRY_DSN in environment.")
                sentry_sdk.init(integrations=integrations, **sentry_config_opts)

        def add_tagging(self, task_instance):
            """Function to add tagging for a task_instance."""
            task = task_instance.task

            with sentry_sdk.configure_scope() as scope:
                for tag_name in self.SCOPE_TAGS:
                    attribute = getattr(task_instance, tag_name)
                    if tag_name == "operator":
                        attribute = task.__class__.__name__
                    scope.set_tag(tag_name, attribute)

        @provide_session
        def add_breadcrumbs(self, task_instance, session=None):
            """Function to add breadcrumbs inside of a task_instance."""
            if session is None:
                return
            execution_date = task_instance.execution_date
            task = task_instance.task
            dag = task.dag
            task_instances = dag.get_task_instances(
                state={State.SUCCESS, State.FAILED},
                end_date=execution_date,
                start_date=execution_date,
                session=session,
            )

            for ti in task_instances:
                data = {}
                for crumb_tag in self.SCOPE_CRUMBS:
                    data[crumb_tag] = getattr(ti, crumb_tag)

                sentry_sdk.add_breadcrumb(category="completed_tasks", data=data, level="info")

        def enrich_errors(self, func):
            """Wrap TaskInstance._run_raw_task to support task specific tags and breadcrumbs."""
            session_args_idx = find_session_idx(func)

            @wraps(func)
            def wrapper(task_instance, *args, **kwargs):
                # Wrapping the _run_raw_task function with push_scope to contain
                # tags and breadcrumbs to a specific Task Instance

                try:
                    session = kwargs.get('session', args[session_args_idx])
                except IndexError:
                    session = None

                with sentry_sdk.push_scope():
                    try:
                        return func(task_instance, *args, **kwargs)
                    except Exception as e:
                        self.add_tagging(task_instance)
                        self.add_breadcrumbs(task_instance, session=session)
                        sentry_sdk.capture_exception(e)
                        raise

            return wrapper

        def flush(self):
            sentry_sdk.flush()

    Sentry = ConfiguredSentry()
