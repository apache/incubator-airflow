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
#

import logging
import multiprocessing
import os
import signal
import subprocess
import time
import unittest
from contextlib import suppress
from subprocess import CalledProcessError
from tempfile import NamedTemporaryFile
from time import sleep
from unittest import mock

import psutil
import pytest

from airflow.exceptions import AirflowException
from airflow.utils import process_utils
from airflow.utils.process_utils import check_if_pidfile_process_is_running, execute_in_subprocess, log


class TestReapProcessGroup(unittest.TestCase):
    @staticmethod
    def _ignores_sigterm(child_pid, child_setup_done):
        def signal_handler(unused_signum, unused_frame):
            pass

        signal.signal(signal.SIGTERM, signal_handler)
        child_pid.value = os.getpid()
        child_setup_done.release()
        while True:
            time.sleep(1)

    @staticmethod
    def _parent_of_ignores_sigterm(parent_pid, child_pid, setup_done):
        def signal_handler(unused_signum, unused_frame):
            pass

        os.setsid()
        signal.signal(signal.SIGTERM, signal_handler)
        child_setup_done = multiprocessing.Semaphore(0)
        child = multiprocessing.Process(
            target=TestReapProcessGroup._ignores_sigterm, args=[child_pid, child_setup_done]
        )
        child.start()
        child_setup_done.acquire(timeout=5.0)
        parent_pid.value = os.getpid()
        setup_done.release()
        while True:
            time.sleep(1)

    def test_reap_process_group(self):
        """
        Spin up a process that can't be killed by SIGTERM and make sure
        it gets killed anyway.
        """
        parent_setup_done = multiprocessing.Semaphore(0)
        parent_pid = multiprocessing.Value('i', 0)
        child_pid = multiprocessing.Value('i', 0)
        args = [parent_pid, child_pid, parent_setup_done]
        parent = multiprocessing.Process(target=TestReapProcessGroup._parent_of_ignores_sigterm, args=args)
        try:
            parent.start()
            assert parent_setup_done.acquire(timeout=5.0)
            assert psutil.pid_exists(parent_pid.value)
            assert psutil.pid_exists(child_pid.value)

            process_utils.reap_process_group(parent_pid.value, logging.getLogger(), timeout=1)

            assert not psutil.pid_exists(parent_pid.value)
            assert not psutil.pid_exists(child_pid.value)
        finally:
            try:
                os.kill(parent_pid.value, signal.SIGKILL)  # terminate doesn't work here
                os.kill(child_pid.value, signal.SIGKILL)  # terminate doesn't work here
            except OSError:
                pass


class TestExecuteInSubProcess(unittest.TestCase):
    def test_should_print_all_messages1(self):
        with self.assertLogs(log) as logs:
            execute_in_subprocess(["bash", "-c", "echo CAT; echo KITTY;"])

        msgs = [record.getMessage() for record in logs.records]

        assert ["Executing cmd: bash -c 'echo CAT; echo KITTY;'", 'Output:', 'CAT', 'KITTY'] == msgs

    def test_should_raise_exception(self):
        with pytest.raises(CalledProcessError):
            process_utils.execute_in_subprocess(["bash", "-c", "exit 1"])


def my_sleep_subprocess():
    sleep(100)


def my_sleep_subprocess_with_signals():
    signal.signal(signal.SIGINT, lambda signum, frame: None)
    signal.signal(signal.SIGTERM, lambda signum, frame: None)
    sleep(100)


class TestKillChildProcessesByPids(unittest.TestCase):
    def test_should_kill_process(self):
        before_num_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")

        process = multiprocessing.Process(target=my_sleep_subprocess, args=())
        process.start()
        sleep(0)

        num_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
        assert before_num_process + 1 == num_process

        process_utils.kill_child_processes_by_pids([process.pid])

        num_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
        assert before_num_process == num_process

    def test_should_force_kill_process(self):

        process = multiprocessing.Process(target=my_sleep_subprocess_with_signals, args=())
        process.start()
        sleep(0)

        all_processes = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().splitlines()
        assert str(process.pid) in map(lambda x: x.strip(), all_processes)

        with self.assertLogs(process_utils.log) as cm:
            process_utils.kill_child_processes_by_pids([process.pid], timeout=0)
        assert any("Killing child PID" in line for line in cm.output)
        sleep(0)
        all_processes = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().splitlines()
        assert str(process.pid) not in map(lambda x: x.strip(), all_processes)


class TestPatchEnviron(unittest.TestCase):
    def test_should_update_variable_and_restore_state_when_exit(self):
        with mock.patch.dict("os.environ", {"TEST_NOT_EXISTS": "BEFORE", "TEST_EXISTS": "BEFORE"}):
            del os.environ["TEST_NOT_EXISTS"]

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ

            with process_utils.patch_environ({"TEST_NOT_EXISTS": "AFTER", "TEST_EXISTS": "AFTER"}):
                assert "AFTER" == os.environ["TEST_NOT_EXISTS"]
                assert "AFTER" == os.environ["TEST_EXISTS"]

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ

    def test_should_restore_state_when_exception(self):
        with mock.patch.dict("os.environ", {"TEST_NOT_EXISTS": "BEFORE", "TEST_EXISTS": "BEFORE"}):
            del os.environ["TEST_NOT_EXISTS"]

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ

            with suppress(AirflowException):
                with process_utils.patch_environ({"TEST_NOT_EXISTS": "AFTER", "TEST_EXISTS": "AFTER"}):
                    assert "AFTER" == os.environ["TEST_NOT_EXISTS"]
                    assert "AFTER" == os.environ["TEST_EXISTS"]
                    raise AirflowException("Unknown exception")

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ


class TestCheckIfPidfileProcessIsRunning(unittest.TestCase):
    def test_ok_if_no_file(self):
        check_if_pidfile_process_is_running('some/pid/file', process_name="test")

    def test_remove_if_no_process(self):
        # Assert file is deleted
        with pytest.raises(FileNotFoundError):
            with NamedTemporaryFile('+w') as f:
                f.write('19191919191919191991')
                f.flush()
                check_if_pidfile_process_is_running(f.name, process_name="test")

    def test_raise_error_if_process_is_running(self):
        pid = os.getpid()
        with NamedTemporaryFile('+w') as f:
            f.write(str(pid))
            f.flush()
            with pytest.raises(AirflowException, match="is already running under PID"):
                check_if_pidfile_process_is_running(f.name, process_name="test")
