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
"""Celery command"""

from multiprocessing import Process
from typing import Optional

import daemon
import psutil
import sqlalchemy.exc
from celery import maybe_patch_concurrency
from celery.bin import worker as worker_bin
from daemon.pidfile import TimeoutPIDLockFile
from flower.command import FlowerCommand
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile

from airflow import settings
from airflow.configuration import conf
from airflow.executors.celery_executor import app as celery_app
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging
from airflow.utils.serve_logs import serve_logs

WORKER_PROCESS_NAME = "worker"


@cli_utils.action_logging
def flower(args):
    """Starts Flower, Celery monitoring tool"""
    options = [
        conf.get('celery', 'BROKER_URL'),
        f"--address={args.hostname}",
        f"--port={args.port}",
    ]

    if args.broker_api:
        options.append(f"--broker-api={args.broker_api}")

    if args.url_prefix:
        options.append(f"--url-prefix={args.url_prefix}")

    if args.basic_auth:
        options.append(f"--basic-auth={args.basic_auth}")

    if args.flower_conf:
        options.append(f"--conf={args.flower_conf}")

    flower_cmd = FlowerCommand()

    if args.daemon:
        pidfile, stdout, stderr, _ = setup_locations(
            process="flower",
            pid=args.pid,
            stdout=args.stdout,
            stderr=args.stderr,
            log=args.log_file,
        )
        with open(stdout, "w+") as stdout, open(stderr, "w+") as stderr:
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pidfile, -1),
                stdout=stdout,
                stderr=stderr,
            )
            with ctx:
                flower_cmd.execute_from_commandline(argv=options)
    else:
        flower_cmd.execute_from_commandline(argv=options)


def _serve_logs(skip_serve_logs: bool = False) -> Optional[Process]:
    """Starts serve_logs sub-process"""
    if skip_serve_logs is False:
        sub_proc = Process(target=serve_logs)
        sub_proc.start()
        return sub_proc
    return None


@cli_utils.action_logging
def worker(args):
    """Starts Airflow Celery worker"""
    if not settings.validate_session():
        raise SystemExit("Worker exiting, database connection precheck failed.")

    autoscale = args.autoscale
    skip_serve_logs = args.skip_serve_logs

    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")

    # Setup locations
    pid_file_path, stdout, stderr, log_file = setup_locations(
        process=WORKER_PROCESS_NAME,
        pid=args.pid,
        stdout=args.stdout,
        stderr=args.stderr,
        log=args.log_file,
    )

    if hasattr(celery_app.backend, 'ResultSession'):
        # Pre-create the database tables now, otherwise SQLA via Celery has a
        # race condition where one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        try:
            session = celery_app.backend.ResultSession()
            session.close()
        except sqlalchemy.exc.IntegrityError:
            # At least on postgres, trying to create a table that already exist
            # gives a unique constraint violation or the
            # "pg_type_typname_nsp_index" table. If this happens we can ignore
            # it, we raced to create the tables and lost.
            pass

    # Setup Celery worker
    worker_instance = worker_bin.worker(app=celery_app)
    options = {
        'optimization': 'fair',
        'O': 'fair',
        'queues': args.queues,
        'concurrency': args.concurrency,
        'autoscale': autoscale,
        'hostname': args.celery_hostname,
        'loglevel': conf.get('logging', 'LOGGING_LEVEL'),
        'pidfile': pid_file_path,
        'without_mingle': args.without_mingle,
        'without_gossip': args.without_gossip,
    }

    if conf.has_option("celery", "pool"):
        pool = conf.get("celery", "pool")
        options["pool"] = pool
        # Celery pools of type eventlet and gevent use greenlets, which
        # requires monkey patching the app:
        # https://eventlet.net/doc/patching.html#monkey-patch
        # Otherwise task instances hang on the workers and are never
        # executed.
        maybe_patch_concurrency(['-P', pool])

    if args.daemon:
        # Run Celery worker as daemon
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        if args.umask:
            umask = args.umask

        ctx = daemon.DaemonContext(
            files_preserve=[handle],
            umask=int(umask, 8),
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            sub_proc = _serve_logs(skip_serve_logs)
            worker_instance.run(**options)

        stdout.close()
        stderr.close()
    else:
        # Run Celery worker in the same process
        sub_proc = _serve_logs(skip_serve_logs)
        worker_instance.run(**options)

    if sub_proc:
        sub_proc.terminate()


@cli_utils.action_logging
def stop_worker(args):  # pylint: disable=unused-argument
    """Sends SIGTERM to Celery worker"""
    # Read PID from file
    pid_file_path, _, _, _ = setup_locations(process=WORKER_PROCESS_NAME)
    pid = read_pid_from_pidfile(pid_file_path)

    # Send SIGTERM
    if pid:
        worker_process = psutil.Process(pid)
        worker_process.terminate()

    # Remove pid file
    remove_existing_pidfile(pid_file_path)
