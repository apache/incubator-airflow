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

"""Webserver command"""
import hashlib
import logging
import os
import signal
import subprocess
import sys
import textwrap
import time
from contextlib import suppress
from time import sleep
from typing import Dict, List, NoReturn

import daemon
import psutil
from daemon.pidfile import TimeoutPIDLockFile
from lockfile.pidlockfile import read_pid_from_pidfile

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowWebServerTimeout
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.process_utils import check_if_pidfile_process_is_running
from airflow.www.app import cached_app, create_app

log = logging.getLogger(__name__)


class GunicornMonitor(LoggingMixin):
    """
    Runs forever, monitoring the child processes of @gunicorn_master_proc and
    restarting workers occasionally or when files in the plug-in directory
    has been modified.

    Each iteration of the loop traverses one edge of this state transition
    diagram, where each state (node) represents
    [ num_ready_workers_running / num_workers_running ]. We expect most time to
    be spent in [n / n]. `bs` is the setting webserver.worker_refresh_batch_size.
    The horizontal transition at ? happens after the new worker parses all the
    dags (so it could take a while!)
       V ────────────────────────────────────────────────────────────────────────┐
    [n / n] ──TTIN──> [ [n, n+bs) / n + bs ]  ────?───> [n + bs / n + bs] ──TTOU─┘
       ^                          ^───────────────┘
       │
       │      ┌────────────────v
       └──────┴────── [ [0, n) / n ] <─── start
    We change the number of workers by sending TTIN and TTOU to the gunicorn
    master process, which increases and decreases the number of child workers
    respectively. Gunicorn guarantees that on TTOU workers are terminated
    gracefully and that the oldest worker is terminated.

    :param gunicorn_master_pid: PID for the main Gunicorn process
    :param num_workers_expected: Number of workers to run the Gunicorn web server
    :param master_timeout: Number of seconds the webserver waits before killing gunicorn master that
        doesn't respond
    :param worker_refresh_interval: Number of seconds to wait before refreshing a batch of workers.
    :param worker_refresh_batch_size: Number of workers to refresh at a time. When set to 0, worker
        refresh is disabled. When nonzero, airflow periodically refreshes webserver workers by
        bringing up new ones and killing old ones.
    :param reload_on_plugin_change: If set to True, Airflow will track files in plugins_folder directory.
        When it detects changes, then reload the gunicorn.
    """

    def __init__(
        self,
        gunicorn_master_pid: int,
        num_workers_expected: int,
        master_timeout: int,
        worker_refresh_interval: int,
        worker_refresh_batch_size: int,
        reload_on_plugin_change: bool,
    ):
        super().__init__()
        self.gunicorn_master_proc = psutil.Process(gunicorn_master_pid)
        self.num_workers_expected = num_workers_expected
        self.master_timeout = master_timeout
        self.worker_refresh_interval = worker_refresh_interval
        self.worker_refresh_batch_size = worker_refresh_batch_size
        self.reload_on_plugin_change = reload_on_plugin_change

        self._num_workers_running = 0
        self._num_ready_workers_running = 0
        self._last_refresh_time = time.monotonic() if worker_refresh_interval > 0 else None
        self._last_plugin_state = self._generate_plugin_state() if reload_on_plugin_change else None
        self._restart_on_next_plugin_check = False

    def _generate_plugin_state(self) -> Dict[str, float]:
        """
        Generate dict of filenames and last modification time of all files in settings.PLUGINS_FOLDER
        directory.
        """
        if not settings.PLUGINS_FOLDER:
            return {}

        all_filenames: List[str] = []
        for (root, _, filenames) in os.walk(settings.PLUGINS_FOLDER):
            all_filenames.extend(os.path.join(root, f) for f in filenames)
        plugin_state = {f: self._get_file_hash(f) for f in sorted(all_filenames)}
        return plugin_state

    @staticmethod
    def _get_file_hash(fname: str):
        """Calculate MD5 hash for file"""
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_num_ready_workers_running(self) -> int:
        """Returns number of ready Gunicorn workers by looking for READY_PREFIX in process name"""
        workers = psutil.Process(self.gunicorn_master_proc.pid).children()

        def ready_prefix_on_cmdline(proc):
            try:
                cmdline = proc.cmdline()
                if len(cmdline) > 0:  # pylint: disable=len-as-condition
                    return settings.GUNICORN_WORKER_READY_PREFIX in cmdline[0]
            except psutil.NoSuchProcess:
                pass
            return False

        ready_workers = [proc for proc in workers if ready_prefix_on_cmdline(proc)]
        return len(ready_workers)

    def _get_num_workers_running(self) -> int:
        """Returns number of running Gunicorn workers processes"""
        workers = psutil.Process(self.gunicorn_master_proc.pid).children()
        return len(workers)

    def _wait_until_true(self, fn, timeout: int = 0) -> None:
        """Sleeps until fn is true"""
        start_time = time.monotonic()
        while not fn():
            if 0 < timeout <= time.monotonic() - start_time:
                raise AirflowWebServerTimeout(f"No response from gunicorn master within {timeout} seconds")
            sleep(0.1)

    def _spawn_new_workers(self, count: int) -> None:
        """
        Send signal to kill the worker.

        :param count: The number of workers to spawn
        """
        excess = 0
        for _ in range(count):
            # TTIN: Increment the number of processes by one
            self.gunicorn_master_proc.send_signal(signal.SIGTTIN)
            excess += 1
            self._wait_until_true(
                lambda: self.num_workers_expected + excess == self._get_num_workers_running(),
                timeout=self.master_timeout,
            )

    def _kill_old_workers(self, count: int) -> None:
        """
        Send signal to kill the worker.

        :param count: The number of workers to kill
        """
        for _ in range(count):
            count -= 1
            # TTOU: Decrement the number of processes by one
            self.gunicorn_master_proc.send_signal(signal.SIGTTOU)
            self._wait_until_true(
                lambda: self.num_workers_expected + count == self._get_num_workers_running(),
                timeout=self.master_timeout,
            )

    def _reload_gunicorn(self) -> None:
        """
        Send signal to reload the gunicorn configuration. When gunicorn receive signals, it reload the
        configuration, start the new worker processes with a new configuration and gracefully
        shutdown older workers.
        """
        # HUP: Reload the configuration.
        self.gunicorn_master_proc.send_signal(signal.SIGHUP)
        sleep(1)
        self._wait_until_true(
            lambda: self.num_workers_expected == self._get_num_workers_running(), timeout=self.master_timeout
        )

    def start(self) -> NoReturn:
        """Starts monitoring the webserver."""
        try:  # pylint: disable=too-many-nested-blocks
            self._wait_until_true(
                lambda: self.num_workers_expected == self._get_num_workers_running(),
                timeout=self.master_timeout,
            )
            while True:
                if not self.gunicorn_master_proc.is_running():
                    sys.exit(1)
                self._check_workers()
                # Throttle loop
                sleep(1)

        except (AirflowWebServerTimeout, OSError) as err:
            self.log.error(err)
            self.log.error("Shutting down webserver")
            try:
                self.gunicorn_master_proc.terminate()
                self.gunicorn_master_proc.wait()
            finally:
                sys.exit(1)

    def _check_workers(self) -> None:
        num_workers_running = self._get_num_workers_running()
        num_ready_workers_running = self._get_num_ready_workers_running()

        # Whenever some workers are not ready, wait until all workers are ready
        if num_ready_workers_running < num_workers_running:
            self.log.debug(
                '[%d / %d] Some workers are starting up, waiting...',
                num_ready_workers_running,
                num_workers_running,
            )
            sleep(1)
            return

        # If there are too many workers, then kill a worker gracefully by asking gunicorn to reduce
        # number of workers
        if num_workers_running > self.num_workers_expected:
            excess = min(num_workers_running - self.num_workers_expected, self.worker_refresh_batch_size)
            self.log.debug(
                '[%d / %d] Killing %s workers', num_ready_workers_running, num_workers_running, excess
            )
            self._kill_old_workers(excess)
            return

        # If there are too few workers, start a new worker by asking gunicorn
        # to increase number of workers
        if num_workers_running < self.num_workers_expected:
            self.log.error(
                "[%d / %d] Some workers seem to have died and gunicorn did not restart them as expected",
                num_ready_workers_running,
                num_workers_running,
            )
            sleep(10)
            num_workers_running = self._get_num_workers_running()
            if num_workers_running < self.num_workers_expected:
                new_worker_count = min(
                    self.num_workers_expected - num_workers_running, self.worker_refresh_batch_size
                )
                # log at info since we are trying fix an error logged just above
                self.log.info(
                    '[%d / %d] Spawning %d workers',
                    num_ready_workers_running,
                    num_workers_running,
                    new_worker_count,
                )
                self._spawn_new_workers(new_worker_count)
            return

        # Now the number of running and expected worker should be equal

        # If workers should be restarted periodically.
        if self.worker_refresh_interval > 0 and self._last_refresh_time:
            # and we refreshed the workers a long time ago, refresh the workers
            last_refresh_diff = time.monotonic() - self._last_refresh_time
            if self.worker_refresh_interval < last_refresh_diff:
                num_new_workers = self.worker_refresh_batch_size
                self.log.debug(
                    '[%d / %d] Starting doing a refresh. Starting %d workers.',
                    num_ready_workers_running,
                    num_workers_running,
                    num_new_workers,
                )
                self._spawn_new_workers(num_new_workers)
                self._last_refresh_time = time.monotonic()
                return

        # if we should check the directory with the plugin,
        if self.reload_on_plugin_change:
            # compare the previous and current contents of the directory
            new_state = self._generate_plugin_state()
            # If changed, wait until its content is fully saved.
            if new_state != self._last_plugin_state:
                self.log.debug(
                    '[%d / %d] Plugins folder changed. The gunicorn will be restarted the next time the '
                    'plugin directory is checked, if there is no change in it.',
                    num_ready_workers_running,
                    num_workers_running,
                )
                self._restart_on_next_plugin_check = True
                self._last_plugin_state = new_state
            elif self._restart_on_next_plugin_check:
                self.log.debug(
                    '[%d / %d] Starts reloading the gunicorn configuration.',
                    num_ready_workers_running,
                    num_workers_running,
                )
                self._restart_on_next_plugin_check = False
                self._last_refresh_time = time.monotonic()
                self._reload_gunicorn()


@cli_utils.action_logging
def webserver(args):
    """Starts Airflow Webserver"""
    print(settings.HEADER)

    # Check for old/insecure config, and fail safe (i.e. don't launch) if the config is wildly insecure.
    if conf.get('webserver', 'secret_key') == 'temporary_key':
        from rich import print as rich_print

        rich_print(
            "[red][bold]ERROR:[/bold] The `secret_key` setting under the webserver config has an insecure "
            "value - Airflow has failed safe and refuses to start. Please change this value to a new, "
            "per-environment, randomly generated string, for example using this command `[cyan]openssl rand "
            "-hex 30[/cyan]`",
            file=sys.stderr,
        )
        sys.exit(1)

    access_logfile = args.access_logfile or conf.get('webserver', 'access_logfile')
    error_logfile = args.error_logfile or conf.get('webserver', 'error_logfile')
    access_logformat = args.access_logformat or conf.get('webserver', 'access_logformat')
    num_workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = args.worker_timeout or conf.get('webserver', 'web_server_worker_timeout')
    ssl_cert = args.ssl_cert or conf.get('webserver', 'web_server_ssl_cert')
    ssl_key = args.ssl_key or conf.get('webserver', 'web_server_ssl_key')
    if not ssl_cert and ssl_key:
        raise AirflowException('An SSL certificate must also be provided for use with ' + ssl_key)
    if ssl_cert and not ssl_key:
        raise AirflowException('An SSL key must also be provided for use with ' + ssl_cert)

    if args.debug:
        print(f"Starting the web server on port {args.port} and host {args.hostname}.")
        app = create_app(testing=conf.getboolean('core', 'unit_test_mode'))
        app.run(
            debug=True,
            use_reloader=not app.config['TESTING'],
            port=args.port,
            host=args.hostname,
            ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None,
        )
    else:
        # This pre-warms the cache, and makes possible errors
        # get reported earlier (i.e. before demonization)
        os.environ['SKIP_DAGS_PARSING'] = 'True'
        app = cached_app(None)
        os.environ.pop('SKIP_DAGS_PARSING')

        pid_file, stdout, stderr, log_file = setup_locations(
            "webserver", args.pid, args.stdout, args.stderr, args.log_file
        )

        # Check if webserver is already running if not, remove old pidfile
        check_if_pidfile_process_is_running(pid_file=pid_file, process_name="webserver")

        print(
            textwrap.dedent(
                '''\
                Running the Gunicorn Server with:
                Workers: {num_workers} {workerclass}
                Host: {hostname}:{port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                Access Logformat: {access_logformat}
                =================================================================\
            '''.format(
                    num_workers=num_workers,
                    workerclass=args.workerclass,
                    hostname=args.hostname,
                    port=args.port,
                    worker_timeout=worker_timeout,
                    access_logfile=access_logfile,
                    error_logfile=error_logfile,
                    access_logformat=access_logformat,
                )
            )
        )

        run_args = [
            'gunicorn',
            '--workers',
            str(num_workers),
            '--worker-class',
            str(args.workerclass),
            '--timeout',
            str(worker_timeout),
            '--bind',
            args.hostname + ':' + str(args.port),
            '--name',
            'airflow-webserver',
            '--pid',
            pid_file,
            '--config',
            'python:airflow.www.gunicorn_config',
        ]

        if args.access_logfile:
            run_args += ['--access-logfile', str(args.access_logfile)]

        if args.error_logfile:
            run_args += ['--error-logfile', str(args.error_logfile)]

        if args.access_logformat and args.access_logformat.strip():
            run_args += ['--access-logformat', str(args.access_logformat)]

        if args.daemon:
            run_args += ['--daemon']

        if ssl_cert:
            run_args += ['--certfile', ssl_cert, '--keyfile', ssl_key]

        run_args += ["airflow.www.app:cached_app()"]

        gunicorn_master_proc = None

        def kill_proc(signum, _):  # pylint: disable=unused-argument
            log.info("Received signal: %s. Closing gunicorn.", signum)
            gunicorn_master_proc.terminate()
            with suppress(TimeoutError):
                gunicorn_master_proc.wait(timeout=30)
            if gunicorn_master_proc.poll() is not None:
                gunicorn_master_proc.kill()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_pid: int):
            # Register signal handlers
            signal.signal(signal.SIGINT, kill_proc)
            signal.signal(signal.SIGTERM, kill_proc)

            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            GunicornMonitor(
                gunicorn_master_pid=gunicorn_master_pid,
                num_workers_expected=num_workers,
                master_timeout=conf.getint('webserver', 'web_server_master_timeout'),
                worker_refresh_interval=conf.getint('webserver', 'worker_refresh_interval', fallback=30),
                worker_refresh_batch_size=conf.getint('webserver', 'worker_refresh_batch_size', fallback=1),
                reload_on_plugin_change=conf.getboolean(
                    'webserver', 'reload_on_plugin_change', fallback=False
                ),
            ).start()

        if args.daemon:
            handle = setup_logging(log_file)

            base, ext = os.path.splitext(pid_file)
            with open(stdout, 'w+') as stdout, open(stderr, 'w+') as stderr:
                ctx = daemon.DaemonContext(
                    pidfile=TimeoutPIDLockFile(f"{base}-monitor{ext}", -1),
                    files_preserve=[handle],
                    stdout=stdout,
                    stderr=stderr,
                )
                with ctx:
                    subprocess.Popen(run_args, close_fds=True)

                    # Reading pid of gunicorn master as it will be different that
                    # the one of process spawned above.
                    while True:
                        sleep(0.1)
                        gunicorn_master_proc_pid = read_pid_from_pidfile(pid_file)
                        if gunicorn_master_proc_pid:
                            break

                    # Run Gunicorn monitor
                    gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                    monitor_gunicorn(gunicorn_master_proc.pid)

        else:
            gunicorn_master_proc = subprocess.Popen(run_args, close_fds=True)
            monitor_gunicorn(gunicorn_master_proc.pid)
