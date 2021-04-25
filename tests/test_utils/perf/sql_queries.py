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

import os
import statistics
from time import monotonic, sleep
from typing import List, NamedTuple, Optional, Tuple

import pandas as pd

# Setup environment before any Airflow import
DAG_FOLDER = os.path.join(os.path.dirname(__file__), "dags")
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = DAG_FOLDER
os.environ["AIRFLOW__DEBUG__SQLALCHEMY_STATS"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "True"

# Here we setup simpler logger to avoid any code changes in
# Airflow core code base
LOG_LEVEL = "INFO"
LOG_FILE = "/files/sql_stats.log"  # Default to run in Breeze

os.environ["AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"] = "scripts.perf.sql_queries.DEBUG_LOGGING_CONFIG"

DEBUG_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"airflow": {"format": "%(message)s"}},
    "handlers": {
        "console": {"class": "logging.StreamHandler"},
        "task": {
            "class": "logging.FileHandler",
            "formatter": "airflow",
            "filename": LOG_FILE,
        },
        "processor": {
            "class": "logging.FileHandler",
            "formatter": "airflow",
            "filename": LOG_FILE,
        },
    },
    "loggers": {
        "airflow.processor": {
            "handlers": ["processor"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "airflow.task": {"handlers": ["task"], "level": LOG_LEVEL, "propagate": False},
        "flask_appbuilder": {
            "handler": ["console"],
            "level": LOG_LEVEL,
            "propagate": True,
        },
    },
    "root": {"handlers": ["console", "task"], "level": LOG_LEVEL},
}


class Query(NamedTuple):
    """
    Define attributes of the queries that will be picked up by the performance tests.
    """

    function: str
    file: str
    location: int
    sql: str
    stack: str
    time: float

    def __str__(self):
        sql = self.sql if len(self.sql) < 110 else f"{self.sql[:111]}..."
        return f"{self.function} in {self.file}:{self.location}: {sql}"

    def __eq__(self, other):
        """
        Override the __eq__ method to compare specific Query attributes
        """
        return (
            self.function == other.function
            and self.sql == other.sql
            and self.location == other.location
            and self.file == other.file
        )

    def to_dict(self):
        """
        Convert selected attributes of the instance into a dictionary.
        """
        return dict(zip(("function", "file", "location", "sql", "stack", "time"), self))


def reset_db():
    """
    Wrapper function that calls the airflow resetdb function.
    """
    from airflow.utils.db import resetdb

    resetdb()


def run_scheduler_job(with_db_reset=False) -> None:
    """
    Run the scheduler job, selectively resetting the db before creating a ScheduleJob instance
    """
    from airflow.jobs.scheduler_job import SchedulerJob

    if with_db_reset:
        reset_db()
    SchedulerJob(subdir=DAG_FOLDER, do_pickle=False, num_runs=3).run()


def is_query(line: str) -> bool:
    """
    Return True, if provided line embeds a query, else False
    """
    return "@SQLALCHEMY" in line and "|$" in line


def make_report() -> List[Query]:
    """
    Returns a list of Query objects that are expected to be run during the performance run.
    """
    queries = []
    with open(LOG_FILE, "r+") as f:
        raw_queries = [line for line in f.readlines() if is_query(line)]

    for query in raw_queries:
        time, info, stack, sql = query.replace("@SQLALCHEMY ", "").split("|$")
        func, file, loc = info.split(":")
        file_name = file.rpartition("/")[-1] if "/" in file else file
        queries.append(
            Query(
                function=func.strip(),
                file=file_name.strip(),
                location=int(loc.strip()),
                sql=sql.strip(),
                stack=stack.strip(),
                time=float(time.strip()),
            )
        )

    return queries


def run_test() -> Tuple[List[Query], float]:
    """
    Run the tests inside a scheduler and then return the elapsed time along with
    the queries that will be run.
    """
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    tic = monotonic()
    run_scheduler_job(with_db_reset=False)
    toc = monotonic()
    queries = make_report()
    return queries, toc - tic


def rows_to_csv(rows: List[dict], name: Optional[str] = None) -> pd.DataFrame:
    """
    Write results stats to a file.
    """
    df = pd.DataFrame(rows)
    name = name or f"/files/sql_stats_{int(monotonic())}.csv"
    df.to_csv(name, index=False)
    print(f"Saved result to {name}")
    return df


def main() -> None:
    """
    Run the tests and write stats to a csv file.
    """
    reset_db()
    rows = []
    times = []

    for i in range(4):
        sleep(5)
        queries, exec_time = run_test()
        if i == 0:
            continue
        times.append(exec_time)
        for qry in queries:
            info = qry.to_dict()
            info["test_no"] = i
            rows.append(info)

    rows_to_csv(rows, name="/files/sql_after_remote.csv")
    print(times)
    msg = "Time for %d dag runs: %.4fs"

    if len(times) > 1:
        print((msg + " (±%.3fs)") % (len(times), statistics.mean(times), statistics.stdev(times)))
    else:
        print(msg % (len(times), times[0]))


if __name__ == "__main__":
    main()
