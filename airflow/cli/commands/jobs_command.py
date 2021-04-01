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

from typing import List

from airflow.jobs.base_job import BaseJob
from airflow.utils.session import provide_session
from airflow.utils.state import State


@provide_session
def check(args, session=None):
    """Checks if job(s) are still alive"""
    if args.allow_multiple and not args.limit > 1:
        raise SystemExit("To use option --allow-multiple, you must set the limit to a value greater than 1.")
    query = (
        session.query(BaseJob)
        .filter(BaseJob.state == State.RUNNING)
        .order_by(BaseJob.latest_heartbeat.desc())
    )
    if args.job_type:
        query = query.filter(BaseJob.job_type == args.job_type)
    if args.hostname:
        query = query.filter(BaseJob.hostname == args.hostname)
    if args.limit > 0:
        query = query.limit(args.limit)

    jobs: List[BaseJob] = query.all()
    alive_jobs = [job for job in jobs if job.is_alive()]

    count_alive_jobs = len(alive_jobs)
    if count_alive_jobs == 0:
        raise SystemExit("No alive jobs found.")
    if count_alive_jobs > 1 and not args.allow_multiple:
        raise SystemExit(f"Found {count_alive_jobs} alive jobs. Expected only one.")
    if count_alive_jobs == 1:
        print("Found one alive job.")
    else:
        print(f"Found {count_alive_jobs} alive jobs.")
