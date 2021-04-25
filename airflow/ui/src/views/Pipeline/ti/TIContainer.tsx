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

import React from 'react';
import useReactRouter from 'use-react-router';

import PipelineBreadcrumb from 'components/PipelineBreadcrumb';
import SectionNav from 'components/SectionNav';

import type {
  Dag as DagType,
  DagRun as DagRunType,
  TaskInstance as TaskInstanceType,
} from 'interfaces';

import PipelineContainer from '../PipelineContainer';

interface RouterProps {
  match: { params: { dagId: DagType['dagId'], dagRunId: DagRunType['dagRunId'], taskId: TaskInstanceType['taskId'] } }
}

interface Props {
  currentView: string;
}

const RunContainer: React.FC<Props> = ({ children, currentView }) => {
  const { match: { params: { dagId, dagRunId, taskId } } }: RouterProps = useReactRouter();

  const basePath = `/pipelines/${dagId}/${dagRunId}/${taskId}`;
  const navItems = [
    {
      label: 'Details',
      path: `${basePath}/details`,
    },
    {
      label: 'Rendered Template',
      path: `${basePath}/rendered-template`,
    },
    {
      label: 'Rendered K8s',
      path: `${basePath}/rendered-k8s`,
    },
    {
      label: 'Log',
      path: `${basePath}/log`,
    },
    {
      label: 'XCom',
      path: `${basePath}/xcom`,
    },
  ];

  return (
    <PipelineContainer>
      <PipelineBreadcrumb dagId={dagId} dagRunId={dagRunId} taskId={taskId} />
      <SectionNav navItems={navItems} currentView={currentView} />
      {children}
    </PipelineContainer>
  );
};

export default RunContainer;
