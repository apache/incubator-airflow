/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export interface DagTag {
  name: string,
}

export interface TimeDelta {
  days: number,
  microseconds: number,
  seconds: number,
  type: string,
}

export interface CronExpression {
  type: string,
  value: string,
}

export interface Dag {
  dagId: string,
  rootDagId: string,
  isPaused: boolean,
  isSubdag: boolean,
  fileloc: string,
  fileToken: string,
  owners: Array<string>,
  description: string,
  scheduleInterval?: TimeDelta | CronExpression,
  tags: DagTag[],
}

export interface DagRun {
  dagRunId: string,
  dagId: Dag['dagId'],
  executionDate: Date,
  startDate: Date,
  endDate: Date,
  state: 'success' | 'running' | 'failed',
  externalTrigger: boolean,
  conf: JSON,
}

export interface Task {
  taskId: string,
  owner: string,
  startDate: Date,
  endDate: Date,
}

export interface TaskInstance {
  taskId: Task['taskId'],
  dagId: Dag['dagId'],
  executionDate: Date,
  startDate: Date,
  endDate: Date,
  duration: number,
  state: string, // TODO: create enum
  tryNumber: number,
  maxTries: number,
  hostname: string,
  unixname: string,
  pool: string,
  poolSlots: number,
  queue: string,
  priorityWeight: number,
  operator: string,
  queuedWhen: string,
  pid: number
  executorConfig: string,
  slaMiss: {
    taskId: Task['taskId'],
    dagId: Dag['dagId'],
    executionDate: Date,
    emailSent: boolean,
    timestamp: Date,
    description: string,
    notification_sent: boolean,
  },
}

export interface Version {
  version: string,
  gitVersion: string,
}

export interface ConfigSection {
  name: string;
  options: Record<string, string>[];
}

export interface Config {
  sections: ConfigSection[];
}
